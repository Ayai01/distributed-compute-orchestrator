"""Execution layers import on demand and build jobs obey the actual grant."""
import json,os,subprocess,sys,unittest
from pathlib import Path
from unittest.mock import patch
from remote_compute.paths import build_environment
from remote_compute import rtl


class RuntimeEfficiencyTests(unittest.TestCase):
    def test_item_bootstrap_does_not_import_service_or_collection_pipeline(self):
        code='import json,sys;from remote_compute import execution;print(json.dumps([n for n in sys.modules if n in("remote_compute.worker","remote_compute.pipeline","remote_compute.client","concurrent.futures","subprocess","tempfile")]))'
        value=subprocess.check_output([sys.executable,'-B','-c',code],text=True,cwd=Path(__file__).resolve().parents[1])
        self.assertEqual(json.loads(value),[])

    def test_all_public_exports_keep_their_original_identity(self):
        import remote_compute,importlib
        for name,module in remote_compute._exports.items():
            self.assertIs(getattr(remote_compute,name),getattr(importlib.import_module('remote_compute.'+module),name))

    def test_registered_cache_launchers_preserve_explicit_task_choices(self):
        cache=str(Path.cwd()/'cache-tools/ccache')
        env=build_environment(dict(PATH='original',OBJCACHE='',CMAKE_CXX_COMPILER_LAUNCHER='custom'),{'ccache':[cache]})
        self.assertEqual(env['OBJCACHE'],'');self.assertEqual(env['CMAKE_CXX_COMPILER_LAUNCHER'],'custom')
        self.assertEqual(env['CMAKE_C_COMPILER_LAUNCHER'],Path(cache).as_posix())
        self.assertEqual(env['PATH'].split(os.pathsep)[0],str(Path(cache).parent))

    def test_verilator_build_jobs_default_to_and_never_exceed_the_grant(self):
        for flags,expected in (([],8),(['-j','0'],8),(['-j','64'],8),(['-j','2'],2),(['--build-jobs','64'],8)):
            with self.subTest(flags=flags),patch('remote_compute.rtl.os.name','posix'),patch.dict(os.environ,REMOTE_COMPUTE_WORKER='1',REMOTE_COMPUTE_CPUS='8'),patch('remote_compute.rtl.subprocess.call',return_value=0)as call:
                self.assertEqual(rtl.main(['--binary','top.sv',*flags]),0)
                argv=call.call_args.args[0];key='--build-jobs'if '--build-jobs'in argv else'-j'
                self.assertEqual(int(argv[argv.index(key)+1]),expected)
