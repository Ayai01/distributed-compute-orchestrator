"""Linux jobs retain ownership of native children that create a new session."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from remote_compute import process as process_module
from remote_compute.process import Process
from remote_compute.resources import process_usage
from test_compute import scratch


class ProcessSamplingTests(unittest.TestCase):
    def test_monitoring_reuses_scan_but_termination_reads_fresh(self):
        with scratch() as temporary:
            root=Path(temporary);(root/'12').mkdir();stat=root/'12'/'stat'
            stat.write_text('12 (example process) S 1 12')
            with patch.object(process_module,'_proc_sample',(0.0,{})),patch.object(process_module,'Path',return_value=root),patch.object(process_module.time,'monotonic',return_value=100):
                first=process_module._proc_rows()
                stat.write_text('12 (example process) Z 1 12')
                self.assertIs(process_module._proc_rows(),first)
                self.assertEqual(first[12][0],'S')
                self.assertEqual(process_module._proc_rows(fresh=True)[12][0],'Z')
                stat.write_text('12 (example process) S 1 12')
                with patch.object(process_module.time,'monotonic',return_value=100.3):
                    self.assertEqual(process_module._proc_rows()[12][0],'S')


@unittest.skipUnless(sys.platform=='linux','Linux process ancestry')
class DetachedProcessTests(unittest.TestCase):
    def test_cancel_reaps_child_from_another_session(self):
        with tempfile.TemporaryDirectory(prefix='dco-process-') as temporary:
            root=Path(temporary)
            child="import time;time.sleep(60)"
            parent=("import subprocess,sys,time;from pathlib import Path;"
                    "p=subprocess.Popen([sys.executable,'-c',"+repr(child)+"],start_new_session=True);"
                    "Path('child.pid').write_text(str(p.pid));time.sleep(60)")
            with (root/'stdout').open('wb') as stdout,(root/'stderr').open('wb') as stderr:
                process=Process([sys.executable,'-c',parent],str(root),dict(os.environ),stdout,stderr)
                try:
                    deadline=time.monotonic()+5
                    while not (root/'child.pid').exists() and time.monotonic()<deadline:time.sleep(.01)
                    identity=int((root/'child.pid').read_text())
                    self.assertIn(identity,process._posix_members())
                    self.assertGreater(process_usage(process)['rss_mb'],0)
                    process.stop()
                    path=Path('/proc')/str(identity)/'stat'
                    if path.exists():self.assertEqual(path.read_text().rsplit(')',1)[1].split()[0],'Z')
                finally:
                    process.close()
                    process.child.wait()
