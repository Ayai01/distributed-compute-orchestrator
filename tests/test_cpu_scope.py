from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from remote_compute.allocation import cpu_capacity
from remote_compute.resources import Host, _cgroup_cpu_counter


class CpuScopeTests(unittest.TestCase):
    def test_linux_cpu_uses_its_quota_and_keeps_the_host_percentage(self):
        with tempfile.TemporaryDirectory(prefix='dco-cpu-scope-') as temp:
            root = Path(temp)
            for version in (1, 2):
                with self.subTest(version=version):
                    if version == 1:
                        locations = {name: (root, root, 1) for name in ('cpu', 'cpuacct')}
                        (root / 'cpu.cfs_quota_us').write_text('2400000')
                        (root / 'cpu.cfs_period_us').write_text('100000')
                        counter, before, after = root / 'cpuacct.usage', '3000000000', '26040000000'
                    else:
                        locations = {'': (root, root, 2)}
                        (root / 'cpu.max').write_text('2400000 100000')
                        counter, before, after = root / 'cpu.stat', 'usage_usec 3000000', 'usage_usec 26040000'
                    counter.write_text(before)
                    with patch('remote_compute.resources.sys.platform', 'linux'), \
                            patch('remote_compute.resources.os.cpu_count', return_value=96), \
                            patch('remote_compute.resources._effective_cpu_capacity', return_value=24), \
                            patch('remote_compute.resources._cgroup_locations', return_value=locations), \
                            patch('remote_compute.resources.memory', return_value=(100, 90)), \
                            patch('remote_compute.resources.commit_memory', return_value=None), \
                            patch('remote_compute.resources.cpu_ticks', side_effect=[(0, 0), (100, 95), (200, 190)]), \
                            patch('remote_compute.resources.time.monotonic', side_effect=[1., 2.]):
                        host = Host()
                        first = host.sample()
                        self.assertIsNone(first['cpu_percent'])
                        counter.write_text(after)
                        live = host.sample()
                    self.assertEqual(live['cpu_scope'], 'cgroup')
                    self.assertEqual(live['logical_cpus'], 24)
                    self.assertEqual(live['cpu_percent'], 96.)
                    self.assertEqual(live['host_cpu_percent'], 5.)
                    self.assertEqual(cpu_capacity(24, 24, live['cpu_percent'], 1.5), 24)

    def test_missing_group_counter_leaves_a_host_sample(self):
        with patch('remote_compute.resources.sys.platform', 'linux'), \
                patch('remote_compute.resources._cgroup_locations', return_value={}), \
                patch('remote_compute.resources._effective_cpu_capacity', return_value=24), \
                patch('remote_compute.resources.os.cpu_count', return_value=96), \
                patch('remote_compute.resources.memory', return_value=(100, 90)), \
                patch('remote_compute.resources.commit_memory', return_value=None), \
                patch('remote_compute.resources.cpu_ticks', side_effect=[(0, 0), (100, 80)]):
            sample = Host().sample()
        self.assertEqual(sample['cpu_scope'], 'host')
        self.assertEqual(sample['cpu_percent'], 20.)

    def test_unavailable_or_malformed_counters_are_ignored(self):
        with tempfile.TemporaryDirectory(prefix='dco-cpu-counter-') as temp:
            root = Path(temp)
            (root / 'cpu.stat').write_text('usage_usec invalid\nuser_usec 10')
            self.assertIsNone(_cgroup_cpu_counter({'': (root, root, 2)}))
