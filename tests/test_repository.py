"""ZIP deployment updates preserve device-owned files without requiring Git."""
import io
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile

from remote_compute import repository
from test_compute import scratch


class RepositoryTests(unittest.TestCase):
    def archive(self, extra=None):
        content=io.BytesIO()
        with zipfile.ZipFile(content,'w') as archive:
            archive.comment=b'0123456789abcdef0123456789abcdef01234567'
            files={'dco-main/':b'','dco-main/main.py':b'new source',
                   'dco-main/remote_compute/new_module.py':b'new module'}
            files.update(extra or {})
            for name,data in files.items():archive.writestr(name,data)
        return content.getvalue()

    def test_zip_update_keeps_private_configuration_runtime_and_unrelated_files(self):
        private=('worker.local.json','nodes.local.json','.local/state/original-id/result.json',
                 '.tools/python/python.exe','personal.txt')
        with scratch() as temporary:
            root=Path(temporary)
            for name in private:
                p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'keep '+name.encode())
            (root/'main.py').write_bytes(b'old source')
            snapshot={name:(root/name).read_bytes() for name in private}
            archive=self.archive({'dco-main/'+name:b'not device configuration' for name in private[:-1]})
            with patch.object(repository,'ROOT',root), patch.object(repository,'urlopen',return_value=io.BytesIO(archive)), \
                    patch.object(repository,'git',side_effect=AssertionError('ZIP deployment does not use Git')):
                result=repository.update()
            self.assertTrue(result['updated']);self.assertTrue(result['restart_required'])
            self.assertEqual((root/'main.py').read_bytes(),b'new source')
            self.assertEqual((root/'remote_compute/new_module.py').read_bytes(),b'new module')
            self.assertEqual({name:(root/name).read_bytes() for name in private},snapshot)
            self.assertFalse(list(root.rglob('*.tmp')))

    def test_check_is_read_only_and_unchanged_archive_does_not_rewrite_source(self):
        with scratch() as temporary:
            root=Path(temporary);(root/'main.py').write_bytes(b'old source')
            with patch.object(repository,'ROOT',root), \
                    patch.object(repository,'urlopen',side_effect=lambda *a,**k:io.BytesIO(self.archive())):
                check=repository.update(check=True)
                self.assertFalse(check['updated']);self.assertFalse(check['restart_required'])
                self.assertEqual((root/'main.py').read_bytes(),b'old source')
                repository.update()
                before=(root/'main.py').stat().st_mtime_ns
                result=repository.update()
                self.assertFalse(result['updated']);self.assertEqual(result['changed_files'],0)
                self.assertEqual((root/'main.py').stat().st_mtime_ns,before)

    def test_invalid_archive_does_not_publish_a_partial_source_update(self):
        with scratch() as temporary:
            root=Path(temporary);(root/'main.py').write_bytes(b'old source')
            with patch.object(repository,'ROOT',root), \
                    patch.object(repository,'urlopen',return_value=io.BytesIO(self.archive({'dco-main/../../outside.py':b'bad'}))), \
                    self.assertRaises(ValueError):repository.update()
            self.assertEqual((root/'main.py').read_bytes(),b'old source')

    def test_git_install_keeps_the_existing_fast_forward_path(self):
        with scratch() as temporary:
            root=Path(temporary);(root/'.git').mkdir()
            calls=[]
            def git(*args):
                calls.append(args)
                if args==('remote','get-url','origin'):return repository.URL
                if args==('status','--porcelain','--untracked-files=no'):return ''
                if args[0]=='merge-base':return 'old'
                if args==('rev-parse','origin/main'):return 'new'
                if args==('rev-parse','HEAD'):return 'new' if ('merge','--ff-only','origin/main') in calls else 'old'
                return ''
            with patch.object(repository,'ROOT',root),patch.object(repository,'git',side_effect=git), \
                    patch.object(repository,'urlopen',side_effect=AssertionError('Git install does not download ZIP')):
                result=repository.update()
            self.assertTrue(result['updated']);self.assertIn(('merge','--ff-only','origin/main'),calls)
