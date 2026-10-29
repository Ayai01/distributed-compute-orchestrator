"""Resource changes stay on the accepted job and release both ledgers."""
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.worker import validate_task
from test_compute import provider, python, scratch, until
from test_center import center


HOLD = "from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"


class GrantTests(unittest.TestCase):
    def finish(self, client, identity):
        client.cancel(identity)
        client.wait(identity)
        return client.fetch(identity)

    def test_auto_and_resize_honor_the_task_demand_ceiling(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                task=python(HOLD,cpus=0,max_cpus=2)
                accepted=client.submit(task); identity=accepted['id']
                until(lambda:client.status(identity)['status']=='running')
                self.assertEqual(client.status(identity)['cpus'],1)
                self.assertEqual(client.resize(identity,99)['cpus'],2)
                self.assertEqual(worker.resize(identity,99)['cpus'],2)
                self.assertEqual(client.submit(task,job_id=identity)['request_sha256'],accepted['request_sha256'])
                result=self.finish(client,identity)
                self.assertEqual(result['peak_cpus'],2)
                until(lambda:pool.nodes[0].reserved==0)

    def test_joint_resize_unblocks_memory_and_failure_preserves_both_grants(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4,memory_mb=256) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                first=client.submit(python(HOLD,cpus=2,memory_mb=192))['id']
                until(lambda:client.status(first)['status']=='running')
                with patch.object(client,'_capabilities',['resize']):
                    declined=client.resize(first,4,memory_mb=96)
                self.assertFalse(declined['supported'])
                self.assertEqual((declined['cpus'],declined['memory_mb']),(2,192))
                second=client.submit(python(HOLD,memory_mb=128))['id']
                self.assertEqual(client.status(second)['status'],'queued')
                with self.assertRaises(RemoteError) as rejected:
                    client.resize(first,4,memory_mb=300)
                self.assertEqual(rejected.exception.status_code,409)
                self.assertEqual((client.status(first)['cpus'],worker.status(first)['memory_mb']),(2,192))
                rejected_by_worker=RemoteError('memory changed after the last host sample')
                rejected_by_worker.status_code=409
                before=client.health()['projects']['default']['demand']
                with patch.object(pool.nodes[0].client,'resize',side_effect=rejected_by_worker):
                    with self.assertRaises(RemoteError):client.resize(first,4,memory_mb=192)
                self.assertEqual(client.health()['projects']['default']['demand'],before)
                del client._capabilities  # A client created before the in-process update.
                grant=client.resize(first,1,memory_mb=96)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(1,96))
                pool.refresh()
                until(lambda:client.status(second)['status']=='running')
                self.assertEqual(pool.nodes[0].reserved_memory_mb,224)
                self.assertEqual(worker.status(first)['memory_mb'],96)
                for identity in (first,second):self.finish(client,identity)
                until(lambda:pool.nodes[0].reserved_memory_mb==0)
                self.assertEqual(worker.health()['memory_reserved_mb'],0)

    def test_joint_resize_recovers_a_lost_reply_without_another_request(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4,memory_mb=256) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python(HOLD,memory_mb=128))['id']
                until(lambda:client.status(identity)['status']=='running')
                original=pool.nodes[0].client.resize
                def lost(*args,**kwargs):
                    original(*args,**kwargs)
                    raise RemoteError('reply lost after accepting the change')
                with patch.object(pool.nodes[0].client,'resize',side_effect=lost) as request:
                    grant=client.resize(identity,3,memory_mb=96)
                    self.assertEqual(request.call_count,1)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(3,96))
                self.assertEqual((pool.nodes[0].reserved,pool.nodes[0].reserved_memory_mb),(3,96))
                self.finish(client,identity)
                until(lambda:pool.nodes[0].reserved==pool.nodes[0].reserved_memory_mb==0)

    def test_auto_waiting_for_memory_cannot_reserve_another_projects_cpu(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=2,memory_mb=128) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                busy=client.submit(python(HOLD,project='A',memory_mb=80))['id']
                until(lambda:client.status(busy)['status']=='running')
                client.control(paused=True)
                automatic=client.submit(python(HOLD,project='B',cpus=0,memory_mb=80))['id']
                small=client.submit(python(HOLD,project='C',memory_mb=32))['id']
                client.control(paused=False)
                until(lambda:client.status(small)['status']=='running')
                self.assertEqual(client.status(automatic)['status'],'queued')
                self.assertEqual(client.health()['used_cpus'],2)
                for identity in (automatic,small,busy):self.finish(client,identity)

    def test_auto_eventually_starts_in_a_partly_occupied_node(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                busy=client.submit(python(HOLD,project='A',cpus=3))['id']
                until(lambda:client.status(busy)['status']=='running')
                automatic=client.submit(python(HOLD,project='B',cpus=0))['id']
                until(lambda:client.status(automatic)['status']=='running')
                self.assertEqual(client.status(automatic)['cpus'],1)
                self.assertEqual(client.status(busy)['status'],'running')
                for identity in (automatic,busy):self.finish(client,identity)

    def test_bounded_auto_admission_counts_useful_work_units(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                window=client.admission('bounded',cpus=0,max_cpus=2,queue_ratio=0)
                self.assertEqual((window['running_tasks'],window['in_flight']),(2,2))
        for limit in (True,0,-1):
            with self.assertRaises(ValueError):validate_task(python('pass',max_cpus=limit),4)
        with self.assertRaises(ValueError):validate_task(python('pass',cpus=3,max_cpus=2),4)
