"""Project admission is work-conserving; placement requires operator mode."""
import json,sys,threading,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,RemoteError,command,python_map
from remote_compute.bundle import task_bundle
from test_compute import provider,scratch,until
from test_center import center,AdmissionPlanningTests


class ProjectAdmissionPolicyTests(unittest.TestCase):
    def test_sdk_rejects_placement_and_external_paths_without_any_request(self):
        client=Client()
        with patch.object(client,'request')as request:
            for field,value in (('node','node1'),('platform',sys.platform),('cwd','C:/external')):
                with self.subTest(field=field),self.assertRaisesRegex(RemoteError,'operator/debug-only'):
                    client.submit(command(['echo'],**{field:value}))
            with self.assertRaisesRegex(RemoteError,'operator/debug-only'):client.capabilities(platform=sys.platform)
            with self.assertRaisesRegex(RemoteError,'operator/debug-only'):client.admission('A',platform=sys.platform)
            request.assert_not_called()

    def test_raw_http_and_sdk_have_the_same_ordinary_boundary(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=2)as(_,remote),Pool([remote])as pool:
            with center(pool,Path(folder)/'center')as(state,operator):
                client=Client(operator.url)
                for field,value in (('node','node1'),('platform',sys.platform),('cwd','C:/external')):
                    identity=uuid.uuid4().hex
                    with self.subTest(field=field),self.assertRaisesRegex(RemoteError,'operator/debug-only'):
                        client.submit_bundle(identity,task_bundle(command(['echo'],**{field:value})))
                    self.assertEqual(client.statuses([identity])[identity]['status'],'unknown')
                for path,data in (('/capabilities?platform='+sys.platform,None),('/admission',dict(project='A',platform=sys.platform))):
                    with self.assertRaisesRegex(RemoteError,'operator/debug-only'):client.request(path,data)
                for value in (client.health(),client.capabilities()):
                    for hidden in ('platforms','nodes','node','root','home','pid','projects','worker_url'):
                        self.assertNotIn(hidden,value)
                with self.assertRaises(RemoteError):client.request('/nodes')
                with self.assertRaises(RemoteError):client.request('/control',dict(paused=True))
                with self.assertRaises(RemoteError):client.request('/restart',{})
                self.assertNotIn('target_cpus',client.admission('A'))
                result=operator.run(command(['${tool:python}','-c','import sys;print(sys.platform)'],
                    platform=sys.platform,max_cpus=2,memory_mb=128,project='debug'))
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(result['stdout'].strip(),sys.platform)
                self.assertIn('platforms',operator.capabilities())

    def test_direct_worker_also_rejects_non_debug_platform(self):
        with scratch()as folder,provider(folder)as(_,client):
            with self.assertRaisesRegex(RemoteError,'operator/debug-only'):
                client.submit_bundle(uuid.uuid4().hex,task_bundle(command(['echo'],platform=sys.platform)))

    def test_project_view_and_advice_have_no_target_quota(self):
        state=AdmissionPlanningTests().center([4,4]);state.queue=[]
        state.jobs={'A':dict(task=dict(steps=[['echo']],cpus=0,max_cpus=64,memory_mb=64,project='A'),
            auto_cpus=True,node=None,result=dict(status='queued')),
            'B':dict(task=dict(steps=[['echo']],cpus=0,max_cpus=64,memory_mb=64,project='B'),
            auto_cpus=True,node=None,result=dict(status='queued'))}
        for project,view in state.project_resources().items():
            self.assertNotIn('target',view);self.assertNotIn('node_targets',view)
            self.assertEqual(state.admission(dict(project=project))['capable_cpus'],8)

    def test_candidates_are_unique_and_small_startup_widths_precede_wide(self):
        state=AdmissionPlanningTests().center([8]);state.queue=['wide','small','auto']
        for key,width in (('wide',4),('small',1),('auto',0)):
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=width,max_cpus=1024,memory_mb=64,project='A',priority=0),
                auto_cpus=width==0,node=None,result=dict(status='queued'))
        self.assertEqual(list(state._queued_candidates(state.project_resources())),['small','auto','wide'])

    def test_least_occupied_project_precedes_another_projects_new_work(self):
        state=AdmissionPlanningTests().center([8]);state.queue=['busy','empty']
        for key in state.queue:
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=0,max_cpus=1024,memory_mb=64,project=key,priority=0),
                auto_cpus=True,node=None,result=dict(status='queued'))
        budgets=state.project_resources();budgets['busy']['used']=6
        self.assertEqual(next(state._queued_candidates(budgets)),'empty')

    def test_compatible_waiting_peer_precedes_wide_growth_on_its_node(self):
        state=AdmissionPlanningTests().center([8]);state.queue=['waiting'];node=state.pool.nodes[0]
        node.reserved=4
        state.jobs={'running':dict(task=dict(steps=[['echo']],cpus=4,max_cpus=8,memory_mb=64,project='A'),
            auto_cpus=False,elastic_cpus=True,desired_cpus=8,node=node,cancel=threading.Event(),result=dict(status='running',submitted=0)),
            'waiting':dict(task=dict(steps=[['echo']],cpus=0,max_cpus=8,memory_mb=64,project='B'),
            auto_cpus=True,node=None,cancel=threading.Event(),result=dict(status='queued',submitted=1))}
        self.assertEqual(state._cooperative_cpu_target('running',8,state.project_resources()),4)
        state.queue.clear();state.jobs.pop('waiting')
        self.assertEqual(state._cooperative_cpu_target('running',8,state.project_resources()),8)

    def test_other_projects_warm_threads_precede_wide_growth_without_a_quota(self):
        state=AdmissionPlanningTests().center([12]);state.queue=[];node=state.pool.nodes[0];node.reserved=2
        for key,submitted in (('A',0),('B',1)):
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=64,memory_mb=64,project=key),
                auto_cpus=True,desired_cpus=64,node=node,cancel=threading.Event(),result=dict(status='running',submitted=submitted))
        self.assertEqual(state._cooperative_cpu_target('B',64,state.project_resources()),8)
        node.reserved=9;state.jobs['B']['task']['cpus']=8
        self.assertEqual(state._cooperative_cpu_target('A',64,state.project_resources()),4)

    def test_centre_operator_role_requires_its_token_and_plain_sdk_drops_role_injection(self):
        with scratch()as folder,provider(Path(folder)/'worker')as(_,remote),Pool([remote])as pool:
            with center(pool,Path(folder)/'center')as(_,operator):
                with self.assertRaisesRegex(RemoteError,'Operator token required'):
                    Client(operator.url,operator=True).health()
                client=Client(operator.url)
                with self.assertRaisesRegex(RemoteError,'operator/debug-only'):
                    client.request('/capabilities?platform='+sys.platform,extra_headers={'X-DCO-Role':'operator'})


if __name__=='__main__':unittest.main()
