"""The selected worker receives only the applicable, registered variant."""

from hashlib import sha256
from pathlib import Path
import sys
import unittest

from remote_compute import Client, Pool
from remote_compute.asset_variants import bind_asset_variants, freeze_asset_variants
from remote_compute.bundle import atomic_json, pack, task_bundle
from remote_compute.view import _capabilities
from test_capability_packages import packaged_worker
from test_center import center
from test_compute import python, scratch


class AssetVariantTests(unittest.TestCase):
    def test_dispatch_uses_one_variant_without_changing_original_id_or_asset_pins(self):
        with scratch() as directory:
            root = Path(directory)
            home = root / 'center'
            home.mkdir()
            selected_files, other_files = {'marker': b'local'}, {'marker': b'other'}
            selected, other = (sha256(pack(f)).hexdigest() for f in (selected_files, other_files))
            with packaged_worker(root/'worker', 'python', cpus=2) as (worker, remote):
                target = sys.platform + '/' + worker.architecture
                atomic_json(home/'asset-variants.json', [{target: selected, 'other/x86_64': other}])
                with Pool([remote]) as pool, center(pool, home) as (_, operator):
                    client = Client(operator.url, operator.token)
                    self.assertEqual(client.sync_files(selected_files), selected)
                    self.assertEqual(client.sync_files(other_files), other)
                    assets = {'local_build': selected, 'other_build': other}
                    env = {'LOCAL_BUILD': '${asset:local_build}', 'OTHER_BUILD': '${asset:other_build}'}
                    code = ('import os; from pathlib import Path; '
                            'assert "OTHER_BUILD" not in os.environ; '
                            'print((Path(os.environ["LOCAL_BUILD"])/"marker").read_text())')
                    task = python(code, assets=assets, env=env)
                    frozen = task_bundle(task)
                    original = 'a' * 32
                    accepted = client.submit_bundle(original, frozen)
                    client.wait(original, wait_timeout=20)
                    result = client.fetch(original, delete=False)
                    self.assertEqual(result['id'], accepted['id'])
                    self.assertEqual(result['request_sha256'], sha256(frozen).hexdigest())
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(result['stdout'].strip(), 'local')
                    self.assertEqual(result['assets'], assets)
                    self.assertNotIn('_asset_variants', result)
                    self.assertTrue((worker.root/'assets'/selected).is_dir())
                    self.assertFalse((worker.root/'assets'/other).exists())

    def test_an_asset_referenced_from_steps_or_unknown_targets_keeps_the_legacy_bundle(self):
        arm, x86 = 'a'*64, 'b'*64
        groups = ({'linux/aarch64': arm, 'linux/x86_64': x86},)
        task = {'assets': {'arm': arm, 'x86': x86},
                'env': {'ARM': '${asset:arm}', 'X86': '${asset:x86}'}, 'steps': [['echo']]}
        variants = freeze_asset_variants(task, groups)
        self.assertIs(bind_asset_variants(task, variants, {'platform': 'win32', 'architecture': 'amd64'}), task)
        self.assertEqual(bind_asset_variants(task, variants, {'platform': 'linux', 'architecture': 'arm64'})['assets'],
                         {'arm': arm})
        self.assertEqual(freeze_asset_variants(dict(task, steps=[['copy', '${asset:x86}/marker']]), groups), [])
        self.assertEqual(_capabilities(['rtl']), 'Chisel')
