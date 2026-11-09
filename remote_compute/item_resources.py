"""Private item IPC; only the parent executor owns the distributed CPU lease."""
from multiprocessing.connection import Client,Listener
import os
import mmap
from pathlib import Path
import struct
import tempfile
import threading
import uuid


class ItemResources:
    def __init__(self,grants,maximum,remaining):
        self.grants,self.maximum,self.remaining=grants,maximum,remaining
        self.items={};self.handlers=set();self.stopped=False
        self.scratch=tempfile.TemporaryDirectory(prefix='dco-item-ipc-')
        self.signal_path=Path(self.scratch.name)/'demand.bin'
        self.signal_stream=self.signal_path.open('w+b')
        self.signal_stream.truncate(max(1,remaining)*16)
        self.signals=mmap.mmap(self.signal_stream.fileno(),0)
        self.address=(r'\\.\pipe\dco-'+uuid.uuid4().hex if os.name=='nt'
                      else str(Path(self.scratch.name)/'resource.sock'))
        self.key=os.urandom(32)
        self.listener=Listener(self.address,authkey=self.key)
        self.grants.observer=self._publish
        self.thread=threading.Thread(target=self._accept,name='dco-item-resources',daemon=True)
        self.thread.start()

    def _accept(self):
        while True:
            connection=self.listener.accept()
            if self.stopped:
                connection.close();return
            handler=threading.Thread(target=self._serve,args=(connection,),name='dco-item-stage',daemon=True)
            with self.grants.condition:self.handlers.add(handler)
            handler.start()

    def _serve(self,connection):
        try:
            while True:
                index,operation,maximum=connection.recv()
                if operation=='ready':
                    with self.grants.condition:
                        if index in self.items:
                            self.items[index]['desired']=min(self.maximum,maximum);self._report()
                    continue
                try:reply=dict(cpus=self.stage(index,maximum)if operation=='enter'else self.release(index))
                except Exception as error:reply=dict(error=str(error))
                connection.send(reply)
        except (EOFError,OSError):pass
        finally:
            connection.close()
            with self.grants.condition:self.handlers.discard(threading.current_thread())

    def register(self,index):
        with self.grants.condition:
            self.items[index]=dict(width=1,desired=1)
            struct.pack_into('=I',self.signals,index*16+8,1)
            self._publish()

    def reserve(self,position,requested):
        with self.grants.condition:
            count=self.grants.reserve(requested)
            for index in range(position,position+count):self.register(index)
            return count

    def _report(self):
        active=sum(item['width']for item in self.items.values())
        ready=self.remaining+sum(max(0,item['desired']-item['width'])for item in self.items.values())
        self.grants.report(active,ready)
        self._publish()

    def _publish(self):
        available=not self.remaining and self.grants.limit()>self.grants.active
        for index,item in self.items.items():
            # Parent writes offer/known-demand; only this item writes its
            # requested width. Separate aligned words never overwrite a peer.
            struct.pack_into('=II',self.signals,index*16,int(available),item['desired'])

    def report(self,remaining):
        with self.grants.condition:
            self.remaining=remaining
            for index,item in self.items.items():
                requested=struct.unpack_from('=I',self.signals,index*16+8)[0]
                item['desired']=max(item['width'],min(self.maximum,max(1,requested)))
            self._report()

    def stage(self,index,maximum):
        with self.grants.condition:
            item=self.items[index];item['desired']=min(self.maximum,maximum)
            struct.pack_into('=I',self.signals,index*16+8,item['desired'])
            self._report()
            # Growth is asynchronous. A numerical batch always has its funded
            # lane slot; it must not wait for an optional distributed resize.
            if self.grants.error:raise self.grants.error
            if self.stopped:raise RuntimeError('DCO item executor stopped')
            if not self.remaining:
                item['width']+=self.grants.reserve(max(0,item['desired']-item['width']))
            self._report()
            return item['width']

    def release(self,index):
        with self.grants.condition:
            item=self.items[index];item.update(width=1,desired=1)
            struct.pack_into('=I',self.signals,index*16+8,1);self._report()
            return 1

    def finish(self,index):
        with self.grants.condition:
            self.items.pop(index,None);self._report()

    def channel(self,index):return self.address,self.key,index,self.maximum,str(self.signal_path)

    def close(self):
        with self.grants.condition:
            self.stopped=True;self.grants.observer=None;self.grants.condition.notify_all()
        with Client(self.address,authkey=self.key):pass
        self.thread.join();self.listener.close()
        with self.grants.condition:handlers=tuple(self.handlers)
        for handler in handlers:handler.join()
        self.signals.close();self.signal_stream.close()
        self.scratch.cleanup()


class ItemChannel:
    def __init__(self,address,key,index,maximum,signal_path):
        self.connection=Client(address,authkey=key);self.index=index;self.maximum=maximum
        self.stream=open(signal_path,'r+b');self.signals=mmap.mmap(self.stream.fileno(),0)
        self.fast=False

    def request(self,operation,maximum=1):
        if operation=='enter':
            struct.pack_into('=I',self.signals,self.index*16+8,maximum)
            offer,known=struct.unpack_from('=II',self.signals,self.index*16)
            self.fast=maximum==1 or not offer
            if self.fast:
                # One-way notice only when the parent's known demand differs;
                # steady one-core batches need no request/reply round trips.
                if maximum>1 and known!=maximum:self.connection.send((self.index,'ready',maximum))
                return 1
        elif operation=='leave':
            struct.pack_into('=I',self.signals,self.index*16+8,1)
            if self.fast:self.fast=False;return 1
        self.connection.send((self.index,operation,maximum))
        reply=self.connection.recv()
        if 'error'in reply:raise RuntimeError(reply['error'])
        return reply['cpus']

    def close(self):
        self.connection.close();self.signals.close();self.stream.close()
