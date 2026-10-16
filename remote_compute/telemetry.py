"""Bounded operator notifications and recent authenticated peer addresses."""
from collections import deque
import threading
import time


class Activity:
    def __init__(self):
        self.lock = threading.Lock()
        self.events = deque(maxlen=6)
        self.peers = {}

    def access(self, address):
        with self.lock:
            self.peers[address] = time.time()
            self.peers = dict(sorted(self.peers.items(), key=lambda x: x[1])[-8:])

    def notify(self, message, address='terminal'):
        with self.lock:
            self.events.append(dict(time=time.time(), ip=address, message=message))

    def snapshot(self):
        with self.lock:
            return dict(events=list(self.events), peers=[dict(ip=k, seen=v) for k, v in self.peers.items()
                                                       if time.time() - v < 120])
