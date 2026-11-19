"""Shared body pacing for explicitly configured local FRP visitors."""

import json
from pathlib import Path
import threading
import time


MIN_BULK_BYTES = 64 * 1024


class RelayBulkBudget:
    def __init__(self, ports, bytes_per_second):
        if (not isinstance(ports, list) or not ports
                or any(type(port) is not int or not 1 <= port <= 65535 for port in ports)):
            raise ValueError('Relay bulk ports must be a nonempty list of TCP port numbers')
        if type(bytes_per_second) is not int or bytes_per_second <= 0:
            raise ValueError('Relay bulk bytes_per_second must be a positive integer')
        self._authorities = frozenset(f'127.0.0.1:{port}' for port in ports)
        self.bytes_per_second = bytes_per_second
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._waiting = []
        self._grants = 0
        self._next = 0.0
        self._paced_bytes = 0
        self._paced_chunks = 0

    def matches(self, endpoint):
        return (endpoint.scheme == 'http' and endpoint.netloc in self._authorities and not endpoint.path.rstrip('/')
                and not endpoint.query and not endpoint.fragment)

    def pace(self, size, check, *, remaining=None):
        if not size:
            return
        check()
        ticket = (remaining if remaining is not None else float('inf'), object())
        with self._condition:
            self._waiting.append(ticket)
            try:
                while True:
                    check()
                    # Reserve every fourth block for the oldest waiting stream.
                    # Let short transfers finish first with the other blocks.
                    winner = (self._waiting[0] if self._grants % 4 == 3 else
                              min(self._waiting, key=lambda entry: entry[0]))
                    delay = self._next - time.monotonic()
                    if winner is ticket and delay <= 0:
                        ready = time.monotonic() + size / self.bytes_per_second
                        self._next = ready
                        self._grants += 1
                        break
                    self._condition.wait(min(.1, delay) if delay > 0 else .1)
            finally:
                self._waiting.remove(ticket)
                self._condition.notify_all()
        while (remaining := ready - time.monotonic()) > 0:
            check()
            time.sleep(min(.1, remaining))
        check()
        with self._lock:
            self._paced_bytes += size
            self._paced_chunks += 1

    def snapshot(self):
        with self._lock:
            return {'bytes_per_second': self.bytes_per_second,
                    'paced_bytes': self._paced_bytes, 'paced_chunks': self._paced_chunks}


def load_relay_bulk_budget(path=None):
    path = Path(path) if path is not None else Path(__file__).resolve().parent.parent / '.local/relay-bulk.json'
    try:
        raw = path.read_text(encoding='utf-8-sig')
    except FileNotFoundError:
        return None
    return RelayBulkBudget(**json.loads(raw))
