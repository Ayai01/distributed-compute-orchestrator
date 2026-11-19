"""Short execution roles, one Python entry and actual backend routing."""
from pathlib import Path
import sys
import unittest

from remote_compute import Pool, command
from remote_compute.view import _capabilities
from remote_compute.runtimes import execution_capabilities
from test_center import center
from test_compute import provider, scratch


class RuntimeContractTests(unittest.TestCase):
    def test_cached_syntax_keeps_tool_availability_backends_and_budgets_live(self):
        from remote_compute.runtimes import _execution_bindings
        with scratch()as root,provider(root,cpus=2,
                python_runtimes={'cpu':[sys.executable],'ml':[sys.executable]})as(_,client):
            with Pool([client])as pool:
                node=pool.nodes[0];task=command(['${tool:python}','-c','print(1)'],requires=['ml'],max_cpus=2)
                task['cpus']=2  # Allocation matches a materialized grant, not its public upper bound.
                _execution_bindings.cache_clear()
                for _ in range(3):self.assertEqual(pool._matching(task),[node])
                self.assertGreaterEqual(_execution_bindings.cache_info().hits,2)
                node.health['python_backends']=['cpu'];self.assertEqual(pool._matching(task),[])
                node.health['python_backends'].append('ml');self.assertEqual(pool._matching(task),[node])
                node.limit=1;self.assertEqual(pool._matching(task),[])
                node.limit=0;task['env']['COMPILER']='${tool:optional-compiler}'
                self.assertEqual(pool._matching(task),[])
                node.health['tools']['optional-compiler']=dict(available=True,argv=[sys.executable])
                self.assertEqual(pool._matching(task),[node])
                node.health['tools']['optional-compiler']['available']=False
                self.assertEqual(pool._matching(task),[])

    def test_base_and_build_details_do_not_expand_the_capability_line(self):
        tags=['python','cxx','g++','java','scala-cli','firtool','circt','make','verilator',
              'ng47','hspice','chisel','ml','cuda','layout','magic','netgen','rtl','onnx',
              'python_torch_scientific','shared:rocket','gpu:training','very_long_internal_tool']
        self.assertEqual(execution_capabilities(tags),
                         ['ng47','hsp','chisel','ml','cuda','layout','rtl','onnx'])
        self.assertEqual(_capabilities(['hsp', *tags]), 'Python ng47 hsp RTL/Chisel ONNX layout cuda')
        self.assertEqual(_capabilities(['cpu','python','cxx','g++']), 'Python')

    def test_chisel_and_layout_require_the_complete_toolchain(self):
        self.assertNotIn('chisel',execution_capabilities(['java','scala-cli','firtool']))
        self.assertNotIn('layout',execution_capabilities(['magic','netgen']))
        self.assertEqual(execution_capabilities(['verilator']),[])

    def test_toolchain_capabilities_are_not_composed_across_nodes(self):
        with scratch()as root,provider(Path(root)/'frontend',
                tools={n:sys.executable for n in ('java','scala-cli','firtool')})as(_,frontend), \
                provider(Path(root)/'backend',
                tools={n:sys.executable for n in ('circt','verilator','cxx','make','layout')})as(_,backend):
            with Pool([frontend,backend])as pool,center(pool,Path(root)/'center')as(_,client):
                self.assertNotIn('chisel',client.capabilities()['tags'])
                self.assertNotIn('layout',client.capabilities()['tags'])
                self.assertEqual(client.admission('chain',requires=['chisel'])['capable_cpus'],0)

    def test_configured_labels_do_not_manufacture_complete_toolchains(self):
        with scratch()as root,provider(root,tools={'verilator':sys.executable},
                tags=['chisel','java','scala-cli','firtool','circt','make',
                      'cxx','verilator','layout','magic','netgen'])as(worker,client):
            self.assertNotIn('chisel',worker.health()['tags'])
            self.assertNotIn('layout',worker.health()['tags'])
            with Pool([client])as pool,center(pool,Path(root)/'center')as(_,hub):
                self.assertNotIn('chisel',hub.capabilities()['tags'])
                self.assertNotIn('layout',hub.capabilities()['tags'])

    def test_one_python_entry_selects_cpu_ml_and_cuda_and_records_the_binding(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2,
                python_runtimes={'cpu':[sys.executable], 'ml':[sys.executable,'-X','dev'],
                                 'cuda':[sys.executable,'-I']},
                accelerators=[dict(kind='cuda',count=1,memory_mb=1024)])as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                code='import sys;print(str(int(sys.flags.dev_mode))+":"+str(sys.flags.isolated))'
                cases=[({},'0:0'),({'requires':['ml']},'1:0'),
                       ({'requires':['ml','cuda'],'accelerator':dict(kind='cuda',count=1,memory_mb=128)},'0:1'),
                       ({'requires':['ml'],'accelerator':dict(count=1,memory_mb=128)},'0:1'),
                       ({'requires':['ml'],'accelerator':{}},'0:1')]
                for options,expected in cases:
                    with self.subTest(options=options):
                        r=client.run(command(['${tool:python}','-c',code],**options))
                        self.assertEqual(r['status'],'succeeded',r)
                        self.assertEqual(r['stdout'].strip(),expected)
                        self.assertIn('python',r['tools'])
                capabilities=client.capabilities()
                self.assertIn('ml',capabilities['tags'])
                self.assertNotIn('python',capabilities['tags'])
                self.assertNotIn('cxx',capabilities['tags'])

    def test_python_gpu_tasks_do_not_route_to_cpu_only_interpreters(self):
        with scratch()as root,provider(Path(root)/'cpu',cpus=2,
                python_runtimes={'cpu':[sys.executable]},
                accelerators=[dict(kind='cuda',count=1,memory_mb=1024)])as(_,cpu), \
                provider(Path(root)/'cuda',cpus=3,
                python_runtimes={'cpu':[sys.executable],'cuda':[sys.executable]},
                accelerators=[dict(kind='cuda',count=1,memory_mb=1024)])as(_,cuda):
            with Pool([cpu,cuda])as pool,center(pool,Path(root)/'center')as(_,client):
                task=command(['${tool:python}','-c','print("cuda runtime")'],
                             accelerator=dict(kind='cuda',count=1,memory_mb=128))
                self.assertEqual([node.name for node in pool._matching(task)],['node2'])
                self.assertEqual(client.admission('p',requires=['python'],
                    accelerator=dict(kind='cuda',count=1,memory_mb=128))['capable_cpus'],3)
                self.assertEqual(client.run(task)['stdout'].strip(),'cuda runtime')

    def test_any_accelerator_uses_the_backend_of_an_available_matching_device(self):
        from test_compute import python,until
        with scratch()as root,provider(root,cpus=3,
                python_runtimes={'cpu':[sys.executable],'cuda':[sys.executable,'-I'],
                                 'xpu':[sys.executable,'-X','dev']},
                accelerators=[dict(kind='cuda',count=1,memory_mb=1024),
                              dict(kind='xpu',count=1,memory_mb=1024)])as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                first=client.submit(python('import time;time.sleep(30)',
                    accelerator=dict(kind='cuda',count=1,memory_mb=128)))['id']
                until(lambda:client.status(first)['status']=='running')
                r=client.run(command(['${tool:python}','-c','import sys;print(int(sys.flags.dev_mode))'],
                    accelerator=dict(count=1,memory_mb=128)))
                self.assertEqual(r['stdout'].strip(),'1',r)
                self.assertEqual(r['accelerator']['kind'],'xpu')
                self.assertEqual(r['requested_accelerator']['kind'],'any')
                client.cancel(first);client.wait(first);client.fetch(first)

    def test_any_accelerator_rejects_a_python_backend_for_another_device_kind(self):
        from remote_compute import RemoteError
        with scratch()as root,provider(root,cpus=2,
                python_runtimes={'cpu':[sys.executable],'cuda':[sys.executable]},
                accelerators=[dict(kind='xpu',count=1,memory_mb=1024)])as(_,worker):
            task=command(['${tool:python}','-c','print(1)'],accelerator=dict(count=1,memory_mb=128))
            with self.assertRaisesRegex(RemoteError,'Python backend'):worker.submit(task)
            with Pool([worker])as pool:self.assertEqual(pool._matching(task),[])

    def test_busy_python_backend_does_not_reserve_a_different_free_accelerator(self):
        from test_compute import python
        with scratch()as root,provider(root,cpus=2,
                python_runtimes={'cpu':[sys.executable],'cuda':[sys.executable]},
                accelerators=[dict(kind='cuda',count=1,memory_mb=1024),
                              dict(kind='xpu',count=1,memory_mb=1024)])as(_,worker):
            with Pool([worker])as pool:
                busy=python('print(1)',accelerator=dict(kind='cuda',count=1,memory_mb=128))
                any_task=python('print(2)',accelerator=dict(kind='any',count=1,memory_mb=128))
                node=pool._acquire(busy,block=False,job_id='a'*32)
                self.assertIsNotNone(node)
                self.assertIsNone(pool._acquire(any_task,block=False,job_id='b'*32))
                self.assertNotIn('b'*32,node.accelerator_reservations)
                pool._release(node,busy,None,job_id='a'*32)
                self.assertIs(pool._acquire(any_task,block=False,job_id='b'*32),node)
                self.assertEqual(any_task['accelerator']['kind'],'cuda')
                pool._release(node,any_task,None,job_id='b'*32)

    def test_cpu_acquisition_skips_accelerator_units_and_reservations(self):
        from unittest.mock import patch
        with scratch()as root,provider(root)as(_,worker):
            with Pool([worker])as pool:
                node=pool.nodes[0];task=command(['${tool:python}','-c','print(1)'])
                with patch.object(node,'accelerator_units',side_effect=AssertionError('CPU asks for a GPU')):
                    self.assertIs(pool._acquire(task,block=False,job_id='c'*32),node)
                self.assertEqual(node.accelerator_reservations,{})
                pool._release(node,task,None,job_id='c'*32)


if __name__=='__main__':unittest.main()
