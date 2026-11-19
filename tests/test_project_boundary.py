import unittest
from pathlib import Path

from remote_compute import Client, Pool, RemoteError
from test_center import center
from test_compute import provider, python, scratch


class ProjectBoundaryTests(unittest.TestCase):
    def test_operator_can_list_jobs_and_project_can_recover_original(self):
        with scratch() as folder, provider(Path(folder) / 'worker') as (_, worker), Pool([worker]) as pool:
            with center(pool, Path(folder) / 'center') as (scheduler, operator):
                project = Client(operator.url, scheduler.token)
                original = project.submit(python("print('original')"))['id']
                project.wait(original)
                for path in ('/nodes', '/control/resources', '/jobs', '/jobs?limit=1',
                             '/jobs?prefix=' + original[:8]):
                    with self.subTest(path=path), self.assertRaises(RemoteError) as denied:
                        project.request(path)
                    self.assertEqual(denied.exception.status_code, 403)
                self.assertEqual(operator.status_page(prefix=original[:8])['items'][0]['id'], original)
                self.assertEqual(project.statuses([original], compact=True)[original]['status'], 'succeeded')
                self.assertEqual(project.fetch(original)['stdout'].strip(), 'original')
