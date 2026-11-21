"""Relay routes and optional shared body pacing."""

import json
from pathlib import Path
import threading
import time


MIN_BULK_BYTES = 64 * 1024
RELAY_BULK_CONFIG = Path(__file__).resolve().parent.parent / '.local/relay-bulk.json'
# Persist an effectively unlimited positive rate for SDKs that predate null.
UNLIMITED_RATE = (1 << 63) - 1


class RelayBulkBudget:
    def __init__(self, ports, bytes_per_second, *, download_bytes_per_second=None):
        if (not isinstance(ports, list) or not ports
                or any(type(port) is not int or not 1 <= port <= 65535 for port in ports)):
            raise ValueError('Relay bulk ports must be a nonempty list of TCP port numbers')
        if bytes_per_second is not None and (type(bytes_per_second) is not int or bytes_per_second <= 0):
            raise ValueError('Relay bulk bytes_per_second must be a positive integer or null')
        self._authorities = frozenset(f'127.0.0.1:{port}' for port in ports)
        self.bytes_per_second = None if bytes_per_second == UNLIMITED_RATE else bytes_per_second
        self._condition = threading.Condition()
        self._waiting = []
        self._grants = 0
        self._next = 0.0
        self._paced_bytes = 0
        self._paced_chunks = 0
        # Old configurations keep their combined budget. New ones can use
        # the capacity of both directions of a full-duplex relay.
        self.download = (self if download_bytes_per_second is None else
                         RelayBulkBudget(ports, download_bytes_per_second))

    def matches(self, endpoint):
        return (endpoint.scheme == 'http' and endpoint.netloc in self._authorities and not endpoint.path.rstrip('/')
                and not endpoint.query and not endpoint.fragment)

    def pace(self, size, check, *, remaining=None):
        if not size:
            return
        check()
        if self.bytes_per_second is None:
            return
        ticket = (remaining if remaining is not None else float('inf'), object())
        with self._condition:
            self._waiting.append(ticket)
        try:
            while True:
                check()
                with self._condition:
                    # Reserve every fourth block for the oldest waiting stream.
                    # Let short transfers finish first with the other blocks.
                    winner = (self._waiting[0] if self._grants % 4 == 3 else
                              min(self._waiting, key=lambda entry: entry[0]))
                    now = time.monotonic()
                    delay = self._next - now
                    if winner is ticket and delay <= 0:
                        # Send this block now. Time spent on the wire counts
                        # toward the interval before the next grant.
                        self._next = now + size / self.bytes_per_second
                        self._grants += 1
                        self._paced_bytes += size
                        self._paced_chunks += 1
                        return
                    self._condition.wait(min(.1, delay) if delay > 0 else .1)
        finally:
            with self._condition:
                self._waiting.remove(ticket)
                self._condition.notify_all()

    def snapshot(self):
        with self._condition:
            state = {'bytes_per_second': self.bytes_per_second,
                     'paced_bytes': self._paced_bytes, 'paced_chunks': self._paced_chunks}
        if self.download is not self:
            incoming = self.download.snapshot()
            state.update(download_bytes_per_second=incoming['bytes_per_second'],
                         upload_paced_bytes=state['paced_bytes'], download_paced_bytes=incoming['paced_bytes'])
            state['paced_bytes'] += incoming['paced_bytes']
            state['paced_chunks'] += incoming['paced_chunks']
        return state


def load_relay_bulk_budget(path=None):
    path = Path(path) if path is not None else RELAY_BULK_CONFIG
    try:
        raw = path.read_text(encoding='utf-8-sig')
    except FileNotFoundError:
        return None
    return RelayBulkBudget(**json.loads(raw))
