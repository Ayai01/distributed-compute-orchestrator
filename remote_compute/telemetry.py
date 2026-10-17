"""Bounded operator notifications and recent authenticated peer addresses."""
from collections import deque
import threading
import time


class Activity:
    def __init__(self):
        self.lock = threading.Lock()
        self.events = deque(maxlen=6)
        self.peers = {}
        self.host_ip = None

    def access(self, address, host_ip=None):
        with self.lock:
            if host_ip and host_ip not in ('0.0.0.0', '::', '::1') and not host_ip.startswith('127.'):
                self.host_ip = host_ip
            self.peers[address] = time.time()
            self.peers = dict(sorted(self.peers.items(), key=lambda x: x[1])[-8:])

    def notify(self, message, address='terminal'):
        with self.lock:
            self.events.append(dict(time=time.time(), ip=address, message=message))

    def submitted(self, result, task, address):
        label = result.get('label', 'job')
        if not task.get('project') or label.startswith('machine/'):
            self.notify(label + ' [' + result['id'][:8] + ']', address)

    def snapshot(self):
        with self.lock:
            return dict(host_ip=self.host_ip, events=list(self.events), peers=[dict(ip=k, seen=v) for k, v in self.peers.items()
                                                       if time.time() - v < 120])
