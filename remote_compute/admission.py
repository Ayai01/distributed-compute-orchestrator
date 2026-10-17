"""Refresh a bounded submission window as competing project demand changes."""
import threading
import time


class AdmissionWindow:
    def __init__(self, client, project, **options):
        self.client,self.project,self.options=client,project,options
        self.lock=threading.Lock()
        self.value=client.admission(project,**options)
        self.maximum=self.value.get('max_in_flight',self.value['in_flight'])
        self.updated=time.monotonic()

    def current(self):
        with self.lock:
            if time.monotonic()-self.updated>=1:
                self.value=self.client.admission(self.project,**self.options)
                self.maximum=self.value.get('max_in_flight',self.value['in_flight'])
                self.updated=time.monotonic()
            return max(1,min(self.maximum,self.value['in_flight']))
