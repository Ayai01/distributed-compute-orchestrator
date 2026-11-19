import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import Client, Pool, command
from test_center import center
from test_compute import provider, scratch


class RuntimeProfileTests(unittest.TestCase):
    def test_profiles_are_discovered_and_bind_only_qualified_work(self):
        base = {'onnx': [sys.executable]}
        configured = {**base, 'pe64': [sys.executable],
                      'onnx-int8': [sys.executable, '-X', 'integer_profile']}
        probe = {'runtime': 'fixture', 'provider': 'CPUExecutionProvider', 'result': [-4, 4]}
        with scratch() as folder:
            root = Path(folder)
            with patch('remote_compute.worker.pe64_available', side_effect=[False, True]), \
                 patch('remote_compute.worker.check_onnx_int8', side_effect=[None, probe]):
                with provider(root/'ordinary', tools=configured) as (first, ordinary), \
                     provider(root/'integer', tools=configured) as (second, integer), \
                     Pool([ordinary, integer]) as pool, center(pool, root/'center') as (scheduler, operator):
                    self.assertIn('onnx', first.tags)
                    self.assertFalse({'pe64', 'onnx-int8'} & set(first.tags))
                    self.assertTrue({'pe64', 'onnx-int8'} <= set(second.tags))
                    self.assertEqual(second._tool_argv('onnx'), base['onnx'])
                    project = Client(operator.url, scheduler.token)
                    self.assertEqual(project.admission('profiles', requires=['pe64', 'onnx-int8'])['capable_cpus'], 1)
                    self.assertTrue({'pe64', 'onnx-int8'} <= set(project.capabilities()['tags']))
                    task = command(['${tool:onnx}', '-c', "import sys; print('integer_profile' in sys._xoptions)"],
                                   max_cpus=1, requires=['pe64', 'onnx-int8'], project='profiles')
                    receipt = project.run(task)
                    self.assertEqual(receipt['stdout'].strip(), 'True')
                    self.assertEqual(second.get(receipt['id'])['tools']['onnx']['argv'], configured['onnx-int8'])
