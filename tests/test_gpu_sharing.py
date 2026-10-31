import threading
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from remote_compute.policy import normalize_accelerator_requirement, select_accelerator_units
from remote_compute.client import Node
from remote_compute.worker import Worker
from remote_compute.coordinator import Coordinator
from remote_compute import Client, Pool
from test_compute import provider, scratch, until, python
from test_center import center


class GPUSharingTests(unittest.TestCase):
    def setUp(self):
        self.unit = (0, 0, 'cuda', 8192, ('gpu:training', 'gpu:shared'), 'one-physical-card')
        self.request = dict(kind='cuda', memory_mb=3072, requires=['gpu:training'], sharing='memory', max_jobs=2)

    def test_two_jobs_share_one_real_card_and_third_waits(self):
        first = select_accelerator_units([self.unit], self.request)
        second = select_accelerator_units([self.unit], self.request, first)
        self.assertEqual(first[0][:6], second[0][:6])
        self.assertIsNone(select_accelerator_units([self.unit], self.request, first + second))

    def test_memory_headroom_and_participant_limit(self):
        first = select_accelerator_units([self.unit], dict(self.request, memory_mb=4096))
        self.assertIsNone(select_accelerator_units([self.unit], dict(self.request, memory_mb=4096), first))
        first = select_accelerator_units([self.unit], dict(self.request, max_jobs=1))
        self.assertIsNone(select_accelerator_units([self.unit], self.request, first))

    def test_existing_exclusive_reservation_keeps_whole_card(self):
        self.assertIsNone(select_accelerator_units([self.unit], self.request, [self.unit]))
        shared = select_accelerator_units([self.unit], self.request)
        self.assertIsNone(select_accelerator_units([self.unit], dict(kind='cuda'), shared))

    def test_shared_request_requires_provider_support(self):
        old = (*self.unit[:4], ('gpu:training',), self.unit[5])
        self.assertIsNone(select_accelerator_units([old], self.request))
        self.assertIn('gpu:shared', normalize_accelerator_requirement(self.request)['requires'])

    def test_shared_request_validates_memory_and_limit(self):
        for changed in ({'memory_mb': 0}, {'max_jobs': 0}, {'max_jobs': True}, {'sharing': 'fake'}):
            with self.assertRaises(ValueError):normalize_accelerator_requirement(dict(self.request, **changed))

    def test_worker_and_scheduler_use_same_reservation_accounting(self):
        node = Node('test', Mock()); node.health={'accelerators': [dict(kind='cuda',count=1,memory_mb=8192,tags=list(self.unit[4]),ids=[self.unit[5]])]}
        worker = object.__new__(Worker); worker.accelerators=node.health['accelerators'];worker.accelerator_reservations={}
        for key in ('a','b'):
            selected=node.reserve_accelerator(self.request,key)
            self.assertEqual(selected,worker._match_accelerator(self.request))
            worker.accelerator_reservations[key]=selected
        self.assertIsNone(worker._match_accelerator(self.request))
        node.release_accelerator('a');worker.accelerator_reservations.pop('a')
        self.assertEqual(node._select_accelerator(self.request),worker._match_accelerator(self.request))
        env={};worker._accelerator_environment('b',env)
        self.assertEqual(env['DCO_ACCELERATOR_MEMORY_MB'],'3072')
        self.assertEqual(env['DCO_ACCELERATOR_SHARING'],'memory')

    def test_zero_wait_prefers_available_gpu_then_falls_back_under_same_id(self):
        scheduler=object.__new__(Coordinator);node=SimpleNamespace(error=None,enabled=True,health={},
            can_match_accelerator=Mock(return_value=True),memory_free=8192,capacity=8,reserved=0,external=0)
        scheduler.pool=SimpleNamespace(condition=threading.Condition(),_matching=lambda unused:[node]);scheduler._save=Mock()
        job={'task':{'accelerator':dict(self.request,fallback_after_seconds=0),'memory_mb':4096},
            'result':{'id':'original','status':'queued','submitted':0},'node':None}
        scheduler._fallback_accelerator(job)
        self.assertIsNotNone(job['task']['accelerator']);scheduler._save.assert_not_called()
        node.reserved=8;scheduler._fallback_accelerator(job)
        self.assertIsNotNone(job['task']['accelerator'])
        node.can_match_accelerator.return_value=False;scheduler._fallback_accelerator(job)
        self.assertIsNone(job['task']['accelerator']);self.assertEqual(job['result']['id'],'original')
        self.assertEqual(job['result']['accelerator_fallback']['reason'],'queue_timeout')

    def test_center_runs_two_shared_gpu_jobs_and_overflow_cpu(self):
        with scratch() as folder, provider(Path(folder)/'worker',cpus=8,
                accelerators=[dict(kind='cuda',memory_mb=8192,ids=['real-one'],tags=['gpu:training'])]) as (_, remote):
            with Pool([remote]) as pool, center(pool,Path(folder)/'center') as (_, operator):
                client=Client(operator.url)
                code='import json,os,time;print(json.dumps({k:os.environ[k] for k in ("DCO_ACCELERATOR_COUNT","DCO_ACCELERATOR_IDS")}));time.sleep(.8)'
                first=[]
                for index in range(2):
                    identity=client.submit(python(code,project='shared',accelerator=self.request))['id']
                    first.append(identity);until(lambda:client.status(identity,brief=True)['status']=='running')
                overflow=client.submit(python(code,project='shared',accelerator=dict(self.request,fallback_after_seconds=0)))['id']
                results=[client.wait(identity)for identity in first+[overflow]]
                self.assertTrue(all(result['status']=='succeeded'for result in results))
                env=[json.loads(result['stdout'])for result in results]
                self.assertEqual([row['DCO_ACCELERATOR_COUNT']for row in env],['1','1','0'])
                self.assertEqual(env[0]['DCO_ACCELERATOR_IDS'],env[1]['DCO_ACCELERATOR_IDS'])


if __name__=='__main__':unittest.main()
