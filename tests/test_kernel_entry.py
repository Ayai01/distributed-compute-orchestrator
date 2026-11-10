"""A fixed numerical kernel starts only after its bounded grant handshake."""
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import Pool,python_task
from test_center import center
from test_compute import provider,scratch


SOURCE='''import os
from pathlib import Path
from remote_compute import run_call
def kernel():
    p=Path('calls.txt');count=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(count))
    return dict(cpus=int(os.environ['REMOTE_COMPUTE_CPUS']),calls=count)
def entry():return run_call(kernel,limits=dict(max_cpus=4,memory_mb=768))
'''


class KernelEntryTests(unittest.TestCase):
    def run_entry(self,cpus=4,cold=False):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=cpus,memory_mb=4096,memory_headroom_mb=0)as(worker,remote):
            with Pool([remote])as pool,center(pool,Path(folder)/'center')as(_,client):
                original=worker.resize;limited=False;requests=[]
                def resize(identity,cpus,**options):
                    nonlocal limited
                    probe=cold and not limited and options.get('memory_mb')==768
                    if probe:limited=True;options['memory_budget_mb']=256
                    result=original(identity,cpus,**options)
                    requests.append(dict(cpus=cpus,active=options.get('active_cpus'),grant=result['cpus'],cold=probe))
                    return result
                with patch.object(worker,'resize',side_effect=resize):
                    result=client.run(python_task('business:entry',max_cpus=4,memory_mb=512,timeout=15,retries=0),files={'business.py':SOURCE})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(value['calls'],1)
                self.assertEqual(worker.used,0)
                return value,requests

    def test_cold_one_slot_probe_is_reconciled_before_kernel_starts(self):
        value,requests=self.run_entry(cold=True)
        self.assertEqual(value['cpus'],4)
        self.assertEqual(requests[0]['grant'],1)
        self.assertTrue(requests[0]['cold'])
        self.assertEqual(requests[1]['cpus'],4)
        self.assertEqual(requests[1]['grant'],4)

    def test_a_small_host_keeps_one_core_without_waiting_for_four(self):
        value,requests=self.run_entry(cpus=1)
        self.assertEqual(value['cpus'],1)
        self.assertEqual([r['cpus']for r in requests],[1,1,1,1])
        self.assertEqual(len(requests),4)

    def test_fully_funded_kernel_keeps_the_existing_handshake(self):
        value,requests=self.run_entry()
        self.assertEqual(value['cpus'],4)
        self.assertEqual([(r['cpus'],r['active'])for r in requests],[(4,0),(4,4),(1,0)])
