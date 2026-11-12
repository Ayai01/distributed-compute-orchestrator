"""Cross-project CPU opportunity follows usable weighted shares at boundaries."""
import json,threading,time,unittest
from pathlib import Path
from remote_compute import Pool,python_map
import test_center as fixtures
from test_center import center
from test_compute import provider,scratch,until


class ProjectShareTests(unittest.TestCase):
    def state(self,capacity=16):return fixtures.AdmissionPlanningTests().center([capacity])

    def job(self,state,key,project,*,cpus=1,maximum=64,desired=None,share=1,priority=0,queued=False,requires=(),node_index=0):
        node=state.pool.nodes[node_index]
        row=dict(task=dict(steps=[['echo']],cpus=cpus,max_cpus=maximum,memory_mb=64,project=project,resource_share=share,priority=priority,requires=list(requires)),
                 node=None if queued else node,elastic_cpus=maximum>1,cancel=threading.Event(),result=dict(id=key,status='queued'if queued else'running',submitted=len(state.jobs),cpus=cpus,memory_mb=64))
        if desired is not None:row['desired_cpus']=desired
        state.jobs[key]=row
        if queued:state.queue.append(key)
        else:node.reserved+=cpus;node.reserved_memory_mb+=64;node.live_jobs.add(key)
        return row

    def test_two_equal_shares_have_equal_growth_opportunity_despite_priority(self):
        s=self.state();self.job(s,'a','A',desired=64,priority=1000);self.job(s,'b','B',desired=64,priority=-1000)
        resources=s.project_resources()
        self.assertEqual(s._project_cpu_plan(resources),{'A':{'0':8},'B':{'0':8}})
        self.assertEqual([s._cooperative_cpu_target(k,64,resources)for k in('a','b')],[8,8])

    def test_unequal_shares_are_weighted_and_unused_demand_is_borrowed(self):
        s=self.state();self.job(s,'a','A',desired=64,share=3);b=self.job(s,'b','B',desired=64)
        self.assertEqual(s._project_cpu_plan(s.project_resources()),{'A':{'0':12},'B':{'0':4}})
        b['desired_cpus']=1
        self.assertEqual(s._project_cpu_plan(s.project_resources()),{'A':{'0':15},'B':{'0':1}})

    def test_lower_current_ratio_gets_the_indivisible_weighted_remainder(self):
        s=self.state(14);self.job(s,'a','A',cpus=10,desired=64,share=3);self.job(s,'b','B',cpus=3,desired=64)
        resources=s.project_resources()
        self.assertEqual(s._project_cpu_plan(resources),{'A':{'0':10},'B':{'0':4}})
        self.assertEqual(s._cooperative_cpu_target('a',64,resources),10)
        self.assertEqual(s._cooperative_cpu_target('b',64,resources),4)

    def test_queue_uses_current_used_per_share_before_cross_project_priority(self):
        s=self.state(256);self.job(s,'a','ARH',cpus=202,maximum=1024,desired=1024,priority=1000)
        self.job(s,'b','DCBFS',cpus=2,maximum=64,desired=64,priority=-1000)
        self.job(s,'aq','ARH',queued=True,priority=1000);self.job(s,'bq','DCBFS',queued=True,priority=-1000)
        resources=s.project_resources();it=s._queued_candidates(resources)
        self.assertEqual(next(it),'bq')
        self.assertEqual(s._growth_headroom(resources),{'0':2})
        resources['DCBFS']['used']=204
        self.assertEqual(next(it),'aq')

    def test_dominant_project_yields_only_at_its_own_safe_boundary(self):
        s=self.state();node=s.pool.nodes[0];node.health['capabilities']=['resize','resize_active_cpus']
        node.client.resize=lambda identity,cpus,**options:dict(cpus=cpus,memory_mb=64)
        s.stopping=False;s._publish=lambda job:None;s._save=lambda job:None
        self.job(s,'a','A',cpus=14,desired=64);self.job(s,'b','B',cpus=2,desired=64)
        self.assertEqual(s.resize('a',64,active_cpus=14)['cpus'],14)
        self.assertEqual(s.jobs['b']['task']['cpus'],2)
        self.assertEqual(s.resize('a',64,active_cpus=1)['cpus'],8)
        self.assertEqual(s.resize('b',64,active_cpus=1)['cpus'],8)
        self.assertEqual(node.reserved,16)

    def test_hundreds_of_old_one_core_jobs_cannot_reserve_low_share_projects_free_slots(self):
        s=self.state(208)
        for i in range(182):self.job(s,'a'+str(i),'ARH',desired=64,priority=1000)
        self.job(s,'wide','ARH',cpus=20,desired=64,priority=1000)
        self.job(s,'b','DCBFS',cpus=2,desired=64,priority=-1000)
        self.job(s,'aq','ARH',queued=True,priority=1000);self.job(s,'bq','DCBFS',queued=True,priority=-1000)
        resources=s.project_resources()
        self.assertEqual(resources['ARH']['used'],202)
        self.assertEqual(s._cooperative_cpu_target('a0',64,resources),1)
        self.assertEqual(s._growth_headroom(resources),{'0':2})
        self.assertEqual(next(s._queued_candidates(resources)),'bq')

    def test_lower_share_wide_growth_precedes_another_projects_new_job(self):
        s=self.state(24);self.job(s,'a','A',cpus=12,desired=64)
        self.job(s,'b','B',cpus=4,desired=64)
        q=self.job(s,'q','A',queued=True)
        resources=s.project_resources()
        self.assertEqual(s._growth_headroom(resources,candidate=q['task']),{'0':8})
        self.assertEqual(s._growth_headroom(resources),{})

    def test_paused_running_cpu_is_a_fixed_share_offset(self):
        s=fixtures.AdmissionPlanningTests().center([12,12])
        self.job(s,'held','A',cpus=12,desired=64)
        s.pool.nodes[0].health['paused']=True
        self.job(s,'aq','A',queued=True)
        self.job(s,'b','B',cpus=1,desired=64,node_index=1)
        resources=s.project_resources()
        self.assertEqual(s._project_cpu_plan(resources),{'A':{'1':0},'B':{'1':12}})
        self.assertEqual(s._cooperative_cpu_target('b',64,resources),12)

    def test_memory_or_capability_blocked_queue_has_no_phantom_share(self):
        for reason in('memory','capability'):
            s=self.state();self.job(s,'a','A',desired=64)
            self.job(s,'q','B',queued=True,requires=['missing']if reason=='capability'else[])
            if reason=='memory':s.pool.nodes[0].health['memory_available_mb']=63
            self.assertEqual(s._cooperative_cpu_target('a',64,s.project_resources()),16)

    def test_priority_orders_work_inside_the_same_project(self):
        s=self.state();self.job(s,'slow','A',queued=True,priority=-1);self.job(s,'urgent','A',queued=True,priority=1)
        self.assertEqual(list(s._queued_candidates(s.project_resources())),['urgent','slow'])

    def test_terminal_or_recovering_projects_do_not_enter_growth_comparison(self):
        for status in('succeeded','recovering'):
            s=self.state();old=self.job(s,'old','old')
            if status=='recovering':old['recover']=True
            else:old['result']['status']=status
            q=self.job(s,'q','new',queued=True)
            self.assertEqual(s._growth_headroom(s.project_resources(),candidate=q['task']),{})

    def test_actual_memory_blocked_growth_lends_cores_but_new_phase_keeps_cpu_opportunity(self):
        s=self.state();node=s.pool.nodes[0];self.job(s,'a','A',desired=16)
        b=self.job(s,'b','B',desired=16)
        model=dict(base_memory_mb=0,memory_per_cpu_mb=200)
        b['task'].update(memory_mb=200,memory_model=model);b['phase_memory']=model
        node.reserved_memory_mb+=136
        node.health.update(memory_capacity_mb=512,memory_available_mb=100,active_jobs=[dict(id='b',cpus=1,active_cpus=1,memory_sample_cpus=1,memory_phase=model,memory_calibrated=True,memory_reservation_mb=200,memory_commit_reservation_mb=200)])
        self.assertEqual(s._project_cpu_plan(s.project_resources()),{'A':{'0':15},'B':{'0':1}})
        self.assertEqual(s._cooperative_cpu_target('b',16,s.project_resources(),phase_model=dict(memory_mb=64)),8)

    def test_positive_share_changes_queue_order_by_ratio_not_raw_cpu_count(self):
        s=self.state();self.job(s,'a','A',cpus=4,share=4);self.job(s,'b','B',cpus=2)
        self.job(s,'aq','A',queued=True,share=4);self.job(s,'bq','B',queued=True)
        self.assertEqual(next(s._queued_candidates(s.project_resources())),'aq')

    def test_held_cores_on_a_contested_node_do_not_idle_an_unusable_other_node(self):
        s=fixtures.AdmissionPlanningTests().center([16,16]);s.pool.nodes[0].health['tags'].append('only-here')
        self.job(s,'a0','A',cpus=15,desired=64)
        self.job(s,'a1','A',cpus=1,desired=64,node_index=1)
        self.job(s,'b','B',cpus=1,desired=64,requires=['only-here'])
        resources=s.project_resources()
        self.assertEqual(s._cooperative_cpu_target('a0',64,resources),1)
        self.assertEqual(s._cooperative_cpu_target('a1',64,resources),16)
        self.assertEqual(s._growth_headroom(resources),{'1':3})

    def test_priority_does_not_reserve_fixed_work_ahead_of_underrepresented_project(self):
        s=self.state();self.job(s,'a','A',cpus=4)
        self.job(s,'b','B',cpus=1)
        self.job(s,'fixed','A',cpus=4,maximum=4,queued=True,priority=1000)
        self.job(s,'small','B',queued=True,priority=-1000)
        resources=s.project_resources();claims=s._fixed_queue_claims(resources)
        self.assertEqual(s._fixed_queue_headroom(s.jobs['small']['task'],resources,claims),{})

    def test_real_http_lower_priority_project_enters_and_grows_while_peer_runs(self):
        source='import time,os\ndef work(x):\n time.sleep(.2)\n return dict(x=x,pid=os.getpid())\n'
        with scratch()as root,provider(Path(root)/'worker',cpus=8)as(_,worker),Pool([worker])as pool,center(pool,Path(root)/'center')as(owner,c):
            a=c.submit(python_map('work:work',range(96),backend='thread',max_cpus=8,memory_mb=128,project='A',priority=1000),files={'work.py':source})['id']
            until(lambda:c.status(a).get('cpus')==8,seconds=8)
            b=c.submit(python_map('work:work',range(32),backend='thread',max_cpus=8,memory_mb=128,project='B',priority=-1000),files={'work.py':source})['id']
            samples=[]
            def balanced():
                projects=c.health()['projects'];samples.append({k:(v['used'],v['demand'])for k,v in projects.items()})
                return projects.get('B',{}).get('used',0)>=4 and projects.get('A',{}).get('used',0)>=3
            try:until(balanced,seconds=8)
            except AssertionError:self.fail(str(samples))
            for i,count in((a,96),(b,32)):
                c.wait(i,wait_timeout=15)
                r=c.fetch(i);self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                values=[v['value']['x']for v in json.loads(r['files']['.dco-results.json'])['items']]
                self.assertEqual(values,list(range(count)))
