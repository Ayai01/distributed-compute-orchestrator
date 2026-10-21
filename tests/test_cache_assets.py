"""Project asset groups preserve their builders' file boundaries and bytes."""

import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute.bundle import pack
from remote_compute.cache_assets import groups
from test_compute import scratch


class CacheAssetTests(unittest.TestCase):
    def test_aop_preserves_source_paths_and_deduplicates_actual_model_closures(self):
        with scratch() as directory:
            root = Path(directory)
            names = ('config.py', 'analog/core.py', 'compute/runner.py', 'utils/paths.py',
                     '.tools/unused.py', 'results/old.py')
            for name in names:
                path = root/name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(name.encode())
            (root/'json').mkdir()
            for engine in ('ngspice', 'hspice'):
                (root/'json'/('AO_cfg_test_'+engine+'.json')).write_text(
                    json.dumps(dict(simulator=engine)), encoding='utf-8')
            (root/'json/retired.json').write_text('invalid historical configuration', encoding='utf-8')
            model = 'a'*64
            included = 'b'*64
            raw = ('.include "__AOP_MODEL_'+included+'__"\n').encode()
            bundle = Mock(return_value=('deck', {model:'first', included:'second'},
                                        {'first':raw, 'second':b'.model original\n'}))
            jobs = Mock(return_value=[dict(netlist='corner one'), dict(netlist='corner two')])
            modules = {
                'analog.simulators':SimpleNamespace(simulation_jobs=jobs),
                'compute.models':SimpleNamespace(model_bundle=bundle),
            }
            with patch('remote_compute.cache_assets.importlib.import_module', side_effect=modules.__getitem__):
                result = groups(root, 'aop')
            self.assertEqual(set(result['aop_code']['files']), set(names[:4]))
            self.assertTrue(all(isinstance(value,Path) for value in result['aop_code']['files'].values()))
            models = [value for name,value in result.items() if name!='aop_code']
            self.assertEqual(len(models), 2)
            self.assertEqual({tuple(value['requires']) for value in models},
                             {('ng47','python_numpy'),('hspice','python_numpy')})
            expected = {model+'.lib':('.include "'+included+'.lib"\n').encode(),
                        included+'.lib':b'.model original\n'}
            for value in models:
                self.assertEqual(pack(value['files']), pack(expected))
            self.assertEqual(bundle.call_count, 4)
            for call in jobs.call_args_list:
                self.assertEqual(call.args[0]['project_root'], str(root.resolve()))

    def test_rgcn_reuses_native_builder_and_only_current_research_references(self):
        with scratch() as directory:
            root = Path(directory)
            for name,data in {
                'configs/live.json':json.dumps({'circuit':{'pre':{
                    'netlist':'reference/current.spice','reference_netlist':'reference/retained.spice'}}}).encode(),
                'reference/current.spice':b'current deck',
                'reference/retained.spice':b'comparison deck',
                'tools/research_worker.py':b'research entry',
                'models/recovered/old.pt':b'historical model',
                '.runtime/tool.exe':b'installed runtime',
            }.items():
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            native_files={'src/current.py':b'native entry'}
            pdk_files={'sky130A/libs.tech/ngspice/sky130.lib.spice':b'.lib current'}
            native=SimpleNamespace(NATIVE_TOOL='ngspice47',native_code=Mock(return_value=native_files),
                                   native_models=Mock(return_value=pdk_files))
            eda=SimpleNamespace(resolve_tools=Mock(return_value={'pdk_root':'selected-pdk'}))
            modules={'src.remote_native':native,'src.eda':eda}
            with patch('remote_compute.cache_assets.importlib.import_module',side_effect=modules.__getitem__):
                result=groups(root,'rgcn')
            native.native_code.assert_called_once_with(root.resolve())
            native.native_models.assert_called_once_with('selected-pdk')
            self.assertEqual(result['rgcn_native']['files'], native_files)
            self.assertEqual(result['rgcn_pdk']['files'], pdk_files)
            self.assertIs(result['rgcn_research_pdk']['files'], result['rgcn_pdk']['files'])
            self.assertEqual(result['rgcn_research_pdk']['requires'], ['ng47','python_torch_pyg'])
            self.assertEqual(set(result['rgcn_research']['files']), {
                'src/current.py','configs/live.json','reference/current.spice',
                'reference/retained.spice','tools/research_worker.py'})
            self.assertEqual(result['rgcn_research']['requires'], ['python_torch_pyg'])
            self.assertNotIn('configs/live.json', native_files)

    def test_bench_captures_original_named_source_and_static_groups(self):
        source={'bench/entry.py':b'source','pyproject.toml':b'project'}
        static={'bench/problems/data/model.npz':b'model bytes'}
        def builder(client):
            data=client.sync_files(static)
            code=client.sync_files(source)
            return dict(bench=code,bench_data=data)
        module=SimpleNamespace(_source_assets=Mock(side_effect=builder))
        with patch('remote_compute.cache_assets.importlib.import_module',return_value=module):
            result=groups(Path.cwd(),'bench')
        self.assertEqual(set(result), {'bench','bench_data'})
        self.assertEqual(pack(result['bench']['files']),pack(source))
        self.assertEqual(pack(result['bench_data']['files']),pack(static))
        self.assertEqual(result['bench']['requires'], ['python_scientific'])
        module._source_assets.assert_called_once()

    def test_arh_uses_existing_source_snapshot_without_copying_shared_tools(self):
        files={'main.py':b'entry','framework/runtime.py':b'runtime'}
        module=SimpleNamespace(files=Mock(return_value=files))
        with patch('remote_compute.cache_assets.importlib.import_module',return_value=module):
            result=groups(Path.cwd(),'arh')
        module.files.assert_called_once_with(Path.cwd().resolve())
        self.assertEqual(result,{'arh_source':{
            'files':files,'requires':['python_numpy','shared:arh']}})

    def test_unknown_project_does_not_import_or_scan_a_project(self):
        with patch('remote_compute.cache_assets.importlib.import_module') as imported:
            with self.assertRaisesRegex(ValueError,'Unknown cache project'):
                groups(Path.cwd(),'unknown')
        imported.assert_not_called()


if __name__=='__main__':
    unittest.main()
