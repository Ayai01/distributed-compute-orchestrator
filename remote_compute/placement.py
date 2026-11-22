"""Read-only calculations shared by placement and phase admission."""
from collections import deque
from copy import copy
import json
import math
import time

from .allocation import plan_resize, startup_cpus, weighted_node_threads
from .bundle import TERMINAL
from .memory import plan_memory, same_phase
from .policy import priority_weight
from .records import active_items, active_values
from .runtimes import capability_set, execution_bindings


def _matching_pass(pool):
    """Reuse provider matches within one unchanged allocation pass."""
    cache={}
    def matching(task):
        accelerator=task.get('accelerator')
        key=(task.get('node'),task.get('platform'),task.get('_capability_package'),
             task.get('_artifact_abi'),bool(task.get('produces_artifact_ref')),
             json.dumps(task.get('native_cache'), sort_keys=True) if task.get('native_cache') else None,
             tuple(key for key in ('step_phases', 'short_job', 'timeout_grace_s', 'checkpoint_output') if key in task),
             startup_cpus(task),task.get('min_start_cpus',1)>1,task.get('memory_mb',256),task.get('executable_ram_mb',0),
             execution_bindings(task),capability_set(task.get('requires',())),
             json.dumps(accelerator,sort_keys=True)if accelerator is not None else None)
        if key not in cache:cache[key]=tuple(pool._matching(task))
        return cache[key]
    return matching


def _elastic_job(job):
    return job.get('elastic_cpus',job.get('auto_cpus',False))


def _copy_node(node):
    view = copy(node)
    # The sampler replaces nested reports. Copy the outer mapping and the
    # resource ledger entries that can be changed in place during planning.
    view.health = node.health.copy()
    view.live_jobs = node.live_jobs.copy()
    view.memory_holds = {key: hold.copy() for key, hold in node.memory_holds.items()}
    view.ram_holds = node.ram_holds.copy()
    view.accelerator_reservations = node.accelerator_reservations.copy()
    view.external_accelerator_reservations = list(node.external_accelerator_reservations)
    return view


class PlacementPlanning:
    def _project_resources(self):
        shares, demands, used, queued, rooms = {}, {}, {}, {}, {}
        online={n.name:n for n in self.pool.nodes if not n.error and n.enabled
                and (not n.health.get('paused') or n.reserved)}
        capacities={name:node.reserved if node.health.get('paused') else node.capacity for name,node in online.items()}
        largest = max((n.capacity for n in online.values() if not n.health.get('paused')), default=1)
        matching=_matching_pass(self.pool)
        for job in active_values(self.jobs):
            if job['result']['status'] in TERMINAL:
                continue
            held=job.get('node')
            # Online quotas exclude disconnected grants. Their original
            # records retain ownership until actual worker reattachment.
            if job.get('recover') or (held is not None and held.name not in online):
                continue
            task = job.get('task', {})
            name = task.get('project') or 'default'
            shares[name] = task.get('resource_share', 1.0)
            startup=startup_cpus(task)
            if held is not None and _elastic_job(job):startup=max(4,startup)
            demand = job.get('desired_cpus',largest if job.get('auto_cpus') else startup)
            demand=min(demand,task.get('max_cpus',demand))
            paused=held is not None and held.health.get('paused')
            if paused:demand=task.get('cpus',0)
            demands[name] = demands.get(name, 0) + demand
            available=rooms.setdefault(name,{})
            wanted={**task,'cpus':0} if _elastic_job(job) else task
            if paused and held.name in online:
                # Pausing stops new work; existing grants still occupy
                # their project's share until their tasks release them.
                available[held.name]=available.get(held.name,0)+demand
            else:
                for node in matching(wanted):
                    if node.name in online and not node.health.get('paused') and (not held or node is held):
                        available[node.name]=available.get(node.name,0)+min(demand,node.capacity)
            used[name] = used.get(name, 0) + (task.get('cpus', 0) if job.get('node') else 0)
            queued[name] = queued.get(name, 0) + int(job['result']['status'] == 'queued')
        ceilings={name:sum(min(capacities[node],demand) for node,demand in rooms[name].items()) for name in shares}
        return {name:dict(resource_share=shares[name],demand=demands[name],used=used[name],queued=queued[name],
                          capacity=ceilings[name],eligible_nodes=sorted(rooms[name]),node_demands=rooms[name]) for name in shares}

    def _queued_candidates(self,budgets):
        """Least occupied weighted project first; priority orders its own work."""
        matching=_matching_pass(self.pool)
        ready=[key for key in self.queue if self.jobs[key]['result'].get('resource_retry_after',0)<=time.time()]
        counts={key:sum(not node.error and node.enabled and not node.health.get('paused')
                        for node in matching(self.jobs[key]['task'])) for key in ready}
        modes={key:startup_cpus(self.jobs[key]['task']) for key in ready}
        groups={}
        weights={key:priority_weight(self.jobs[key]['task'].get('priority')) for key in ready}
        for key in sorted(ready,key=lambda key:(-weights[key],modes[key],counts[key])):
            task=self.jobs[key]['task']
            groups.setdefault(task.get('project')or'default',deque()).append(key)
        while groups:
            group=min(groups,key=lambda group:(budgets[group]['used']/budgets[group]['resource_share'] if group in budgets else math.inf,
                modes[groups[group][0]],counts[groups[group][0]],-weights[groups[group][0]],group))
            key=groups[group].popleft()
            if not groups[group]:del groups[group]
            yield key

    def _growth_headroom(self, resources, *, candidate=None, fixed_claims=None):
        """Give existing elastic work its first four useful threads.

        This holds no worker lease and never changes an active pool. Its owner
        claims the slots with the next drained resize; lowering demand or
        finishing makes them available to queued work immediately.
        """
        protected={}
        warm_cache={} if fixed_claims is None else {'claims':fixed_claims}
        remaining={}
        occupied={}
        for _key,peer in active_items(self.jobs):
            node=peer.get('node')
            if node is not None and peer['result']['status']not in TERMINAL:
                key=(peer['task'].get('project')or'default',node.name)
                occupied[key]=occupied.get(key,0)+peer['task']['cpus']
        warm_cache['occupied']=occupied
        for identity,job in sorted(active_items(self.jobs),key=lambda row:(row[1]['result'].get('submitted',0),row[0])):
            held=job.get('node');current=job.get('task',{})
            group=current.get('project')or'default'
            if group not in resources:continue
            other=(candidate.get('project')or'default')if candidate is not None else group
            lower=(candidate is not None and other in resources and group!=other and
                   resources[group]['used']/resources[group]['resource_share']<resources[other]['used']/resources[other]['resource_share'])
            startup=min(4,current.get('max_cpus',held.capacity)) if held is not None and _elastic_job(job) else 0
            desired=job.get('desired_cpus',startup)
            request=job['result'].get('resource_request')or {}
            pending=request and request.get('request_id')!=job.get('lease_answered')
            wide_request=lower and pending and desired>4
            # Beyond warmup, protect growth only while its owner is actually
            # requesting it. A standing ceiling during a long native step
            # must not strand idle slots or prevent overcommit from opening.
            if not wide_request:desired=min(4,desired)
            old=current.get('cpus',0)
            if (held is None or desired<=old
                    or job['result']['status'] not in ('running','dispatching') or job['cancel'].is_set()
                    or held.error or not held.enabled or held.health.get('paused')):
                continue
            free=max(0,held.capacity-held.reserved-held.external-protected.get(held.name,0))
            if not free:continue
            if 'projects' not in warm_cache:warm_cache['projects']=self._project_cpu_plan(resources)
            if wide_request:
                if 'claims' not in warm_cache:warm_cache['claims']=self._fixed_queue_claims(resources)
                target=self._cooperative_cpu_target(identity,desired,resources,fixed_claims=warm_cache['claims'])
            else:
                target=self._cooperative_cpu_target(identity,desired,resources,warm_cache=warm_cache)
            key=(group,held.name)
            if key not in remaining:
                remaining[key]=(max(0,warm_cache['projects'].get(group,{}).get(held.name,0)-occupied.get(key,0))
                                if len(warm_cache['projects'])>1 else math.inf)
            available_memory=held.memory_credit(job.get('worker_id',identity),current['memory_mb'])+max(0,min(held.memory_free,self.pool._host_memory_free(held)))
            model=dict(memory_mb=None,base_memory_mb=None,memory_per_cpu_mb=None,memory_limit_mb=None)
            model.update(job.get('phase_memory',dict(memory_mb=current['memory_mb'])))
            calibrated=held.resize_memory_model(job.get('worker_id',identity),model)
            model={key:calibrated.get(key)for key in model}
            target,memory=plan_memory(target,available_memory,**model)if 'adaptive_memory'in held.health.get('capabilities',())else plan_resize(target,available_mb=available_memory,**model)
            if memory is not None and memory>available_memory:continue
            count=max(0,min(target-old,free,remaining[key]))
            if count:
                remaining[key]-=count
                protected[held.name]=protected.get(held.name,0)+count
        return protected

    def _fixed_queue_claims(self, resources):
        """Let a ready fixed-width job accumulate its share on one worker."""
        claims, groups = {}, set()
        candidates=[];queued=set(self.queue)
        for identity,job in active_items(self.jobs):
            if (job['task'].get('project')or'default')not in resources:continue
            minimum=job['result'].get('min_cpus')or 0
            waiting=(job.get('node')is not None and minimum>1
                and not job['result'].get('active_cpus',0)and job['task']['cpus']<minimum)
            if waiting:
                candidates.append((identity,dict(job['task'],cpus=minimum)))
            elif identity in queued and startup_cpus(job['task'])>1:
                candidates.append((identity,job['task']))
        def order(identity):
            task = self.jobs[identity]['task']
            group = task.get('project') or 'default'
            return (resources[group]['used']/resources[group]['resource_share'], group, -priority_weight(task.get('priority')))
        for identity,task in sorted(candidates,key=lambda row:order(row[0])):
            job = self.jobs[identity]
            if job['result'].get('resource_retry_after',0)>time.time():continue
            group = task.get('project') or 'default'; resource = resources[group]
            width = startup_cpus(task)
            if width <= 1 or group in groups or job['cancel'].is_set():
                continue
            held=job.get('node')
            ready = [node for node in self.pool._matching(task)
                     if node.name not in claims and not node.error and node.enabled
                     and (held is None or node is held)
                     and not node.health.get('paused') and not node.health.get('queued', 0)
                     and node.capacity-node.external >= width
                     and min(node.memory_free, self.pool._host_memory_free(node))
                         +(node.memory_credit(job.get('worker_id',identity),job['task']['memory_mb'])if held is node else 0) >= node.startup_memory(task)
                     and node.can_match_accelerator(node.execution_accelerator(task,available=True))]
            if ready:
                node = max(ready, key=lambda node: (node.capacity-node.reserved-node.external, node.name))
                claims[node.name] = (node, task)
                groups.add(group)
        return claims

    @staticmethod
    def _fixed_queue_headroom(task, resources, claims):
        group = task.get('project') or 'default'; current = resources[group]
        protected = {}
        for name, (node, waiting) in claims.items():
            owner = waiting.get('project') or 'default'; resource = resources[owner]
            startup_claim = (waiting.get('cpus') == 0 and waiting is not task
                             and priority_weight(waiting.get('priority')) >= priority_weight(task.get('priority')))
            if (owner != group or startup_claim) and resource['used']/resource['resource_share'] <= current['used']/current['resource_share']:
                protected[name] = max(0, min(startup_cpus(waiting), node.capacity-node.reserved-node.external))
        return protected

    def _project_cpu_plan(self, resources, *, demand_override=None, memory_exempt=()):
        """Lend compatible capacity by current, usable weighted demand.

        These are opportunities at safe boundaries, not revocable live grants.
        A project unable to use RAM, an accelerator or a node contributes no
        hypothetical claim there; the other projects can borrow that capacity.
        """
        nodes={n.name:n for n in self.pool.nodes if n.enabled and not n.error and not n.health.get('paused')}
        capacities={name:max(0,n.capacity-n.external)for name,n in nodes.items()}
        matching=_matching_pass(self.pool);demands={};rooms={};memory={};fixed={}
        now=time.time()
        for key,job in active_items(self.jobs):
            if (job['result']['status']in TERMINAL or job['cancel'].is_set()or job.get('recover')
                    or job['result'].get('resource_retry_after',0)>now):continue
            task=job['task'];group=task.get('project')or'default';held=job.get('node')
            if group not in resources:continue
            if held is not None and held.name not in nodes:
                if held.enabled and not held.error and held.health.get('paused'):
                    fixed[group]=fixed.get(group,0)+task['cpus']
                continue
            elastic=_elastic_job(job)
            demand=job.get('desired_cpus',max(4,task.get('cpus',1))if held is not None and elastic else startup_cpus(task))
            if demand_override is not None:demand=demand_override.get(key,demand)
            demand=min(demand,task.get('max_cpus',demand))
            eligible=[]
            for node in matching({**task,'cpus':0}if elastic else task):
                if node.name not in nodes or held is not None and held is not node:continue
                if node.name not in memory:memory[node.name]=max(0,min(node.memory_free,self.pool._host_memory_free(node)))
                if held is None:
                    if node.startup_memory(task)>memory[node.name]:continue
                    if task.get('accelerator')and not node.can_match_accelerator(node.execution_accelerator(task,available=True)):continue
                eligible.append(node.name)
            if not eligible:continue
            if held is not None and elastic:
                model=dict(memory_mb=None,base_memory_mb=None,memory_per_cpu_mb=None,memory_limit_mb=None)
                model.update(job.get('phase_memory')or task.get('memory_model')or dict(memory_mb=task['memory_mb']))
                sample=next((j for j in held.health.get('active_jobs',())if j['id']==job.get('worker_id',key)),{})
                # Unknown estimates must not erase CPU opportunity before the
                # caller can wait for cold calibration. Its new phase is fitted
                # separately by resize, after the fair CPU target is known.
                if (key not in memory_exempt and sample.get('memory_calibrated')and
                        same_phase(model,sample.get('memory_phase')or sample.get('memory_phase_model'))):
                    calibrated=held.resize_memory_model(job.get('worker_id',key),model)
                    model={field:calibrated.get(field)for field in model}
                    available=min(held.memory_capacity,held.memory_credit(job.get('worker_id',key),task['memory_mb'])+memory[held.name])
                    width,_=plan_memory(demand,available,**model)if 'adaptive_memory'in held.health.get('capabilities',())else plan_resize(demand,available_mb=available,**model)
                    demand=max(1,min(demand,width))
            demands[group]=demands.get(group,0)+demand
            edges=rooms.setdefault(group,{})
            for name in eligible:edges[name]=edges.get(name,0)+min(demand,capacities[name])
        shares={group:resources[group]['resource_share']for group in demands}
        if len(shares)<=1:
            return {group:{name:min(capacities[name],room)for name,room in rooms[group].items()}for group in shares}
        return weighted_node_threads(capacities,shares,demands,rooms,current_used={group:resources[group]['used']for group in shares},fixed_used=fixed)

    def _cooperative_cpu_target(self, identity, desired, resources, *, warm_cache=None, phase_model=None, fixed_claims=None):
        """Apply the project share, then proportional task weights at a drained boundary.

        Running peers keep their grants until their own boundary. Within the
        project's usable compatible capacity, existing work gets up to four useful threads, ready work
        enters at its startup width, then idle slots widen running work. Explicit
        fixed-width requests retain their indivisible admission contract.
        """
        current=self.jobs[identity];group=current['task'].get('project') or 'default'
        if desired==0:return 0
        if fixed_claims is not None:
            claims=fixed_claims
        elif warm_cache is None:
            claims=self._fixed_queue_claims(resources)
        else:
            if 'claims' not in warm_cache:warm_cache['claims']=self._fixed_queue_claims(resources)
            claims=warm_cache['claims']
        claim=claims.get(current['node'].name)
        minimum=current['result'].get('min_cpus')or 0
        if minimum>1 and not current['result'].get('active_cpus',0):
            # Weighted shares choose the next indivisible opportunity. Rounding
            # every waiting four-core stage down to two would strand both.
            return desired if claim is None or (claim[1].get('project')or'default')==group else 0
        claimed_room=None
        if claim is not None:
            owner=claim[1].get('project')or'default'
            if owner==group or resources[owner]['used']/resources[owner]['resource_share']<=resources[group]['used']/resources[group]['resource_share']:
                claimed_room=max(0,current['node'].capacity-current['node'].external-current['node'].reserved
                                 +current['task']['cpus']-startup_cpus(claim[1]))
                if claimed_room==0:return 0
        if desired==1:
            # The plan grants at most the requested width and at least one.
            # A one-thread phase therefore has the exact target without a plan.
            return 1
        if warm_cache is not None:
            if 'projects'not in warm_cache:warm_cache['projects']=self._project_cpu_plan(resources)
            projects=warm_cache['projects']
        else:
            previous=current.get('phase_memory')or current['task'].get('memory_model')or dict(memory_mb=current['task']['memory_mb'])
            exempt=(identity,)if phase_model is not None and not same_phase(phase_model,previous)else()
            projects=self._project_cpu_plan(resources,demand_override={identity:max(desired,current.get('desired_cpus',desired))},memory_exempt=exempt)
        # Other jobs retain their real grants until their own boundaries.
        # An ideal lower share must never be treated as if those peers had
        # already returned CPUs, or hundreds of one-core jobs can warm again.
        if warm_cache is not None and 'occupied' in warm_cache:
            held_here=warm_cache['occupied'].get((group,current['node'].name),0)
        else:
            held_here=sum(peer['task']['cpus']for _key,peer in active_items(self.jobs)
                          if peer.get('node')is current['node']and peer['result']['status']not in TERMINAL
                          and(peer['task'].get('project')or'default')==group)
        share_room=(max(1,projects.get(group,{}).get(current['node'].name,0)-max(0,held_here-current['task']['cpus']))
                    if len(projects)>1 else desired)
        if claimed_room is not None:share_room=min(share_room,claimed_room)
        if warm_cache is not None and group in warm_cache.get('warm',{}):
            return min(share_room,max(1,warm_cache['warm'][group].get(identity,{}).get(current['node'].name,0)))
        members={key:job for key,job in active_items(self.jobs)
                 if job['result']['status'] not in TERMINAL and not job['cancel'].is_set()
                 and job['result'].get('resource_retry_after',0)<=time.time()
                 and (job['task'].get('project') or 'default')==group}
        nodes={node.name:node for node in self.pool.nodes
               if node.enabled and not node.error and not node.health.get('paused')}
        capacities={}
        for name,node in nodes.items():
            held=sum(job['task']['cpus'] for job in members.values() if job.get('node') is node)
            capacities[name]=max(0,min(node.capacity-node.external-node.reserved+held,projects.get(group,{}).get(name,0)))
        matching=_matching_pass(self.pool)
        fixed=[];demands={};rooms={};waiting={};memory_available={}
        for key,job in members.items():
            task=job['task'];held=job.get('node')
            automatic=key==identity or _elastic_job(job)
            if held is not None and not automatic:
                if held.name in capacities:capacities[held.name]=max(0,capacities[held.name]-task['cpus'])
                continue
            wanted={**task,'cpus':0} if automatic else task
            eligible=[]
            for node in matching(wanted):
                if node.name not in capacities or (held is not None and held is not node):continue
                if held is None:
                    if node.name not in memory_available:
                        memory_available[node.name]=max(0,min(node.memory_free,self.pool._host_memory_free(node)))
                    if node.startup_memory(task)>memory_available[node.name]:continue
                    if task.get('accelerator') and not node.can_match_accelerator(node.execution_accelerator(task,available=True)):continue
                eligible.append(node.name)
            if not eligible:continue
            if not automatic:
                fixed.append((key,task,eligible,job['result'].get('submitted',0)))
                continue
            # Before its first phase request, an elastic peer gets four-thread
            # warmup demand, bounded by its useful ceiling.
            demand=desired if key==identity else job.get('desired_cpus',
                max(nodes[n].capacity for n in eligible)if job.get('auto_cpus')else max(4,task.get('cpus',1)))
            demands[key]=min(demand,task.get('max_cpus',demand))
            rooms[key]={name:min(demands[key],nodes[name].capacity) for name in eligible}
            if held is None:waiting[key]=startup_cpus(task)
        for key,task,eligible,submitted in sorted(fixed,key=lambda row:(-priority_weight(row[1].get('priority')),len(row[2]),row[3])):
            choices=[name for name in eligible if capacities[name]>=task['cpus']]
            if choices:
                selected=max(choices,key=lambda name:capacities[name])
                capacities[selected]-=task['cpus']
        running={key:value for key,value in demands.items() if key not in waiting}
        shares={key:priority_weight(members[key]['task'].get('priority')) for key in running}
        warm=weighted_node_threads(capacities,shares,{key:min(4,value) for key,value in running.items()},rooms)
        # A one-slot tail can admit a new one-thread job. Its later four-slot
        # warmup must not dilute older jobs back to two or three threads.
        # Running work is pinned to its current node, so preserve warmup in
        # admission order on nodes whose combined warm demand does not fit.
        for name,capacity in capacities.items():
            pinned=[key for key in running if name in rooms[key]]
            if sum(min(4,running[key])for key in pinned)<=capacity or len({shares[key] for key in pinned})>1:continue
            remaining=capacity
            for key in sorted(pinned,key=lambda key:(members[key]['result'].get('submitted',0),key)):
                count=min(remaining,4,running[key],rooms[key][name])
                warm[key]={name:count};remaining-=count
        if warm_cache is not None:
            # One growth pass asks each elastic peer for its first four useful
            # threads. The same group has one unchanged warm plan;
            # wider idle growth is handled separately at the phase boundary.
            warm_cache.setdefault('warm',{})[group]=warm
            return min(share_room,max(1,warm.get(identity,{}).get(current['node'].name,0)))
        spare={name:max(0,value-sum(row.get(name,0) for row in warm.values())) for name,value in capacities.items()}
        entrants=weighted_node_threads(spare,{key:priority_weight(members[key]['task'].get('priority')) for key in waiting},waiting,rooms)
        # A ready entrant owns this opportunity until the scheduler admits it.
        # Wider running phases may still use other nodes with no ready entrant.
        for node_name in spare:
            if any(row.get(node_name,0) for row in entrants.values()):
                capacities[node_name]-=spare[node_name]
        planned=weighted_node_threads(capacities,shares,running,rooms)
        name=current['node'].name
        target=min(share_room,max(1,planned.get(identity,{}).get(name,0)))
        return target


class PlacementSnapshot(PlacementPlanning):
    """Private view of the ledger. Calculations never acquire service locks."""

    def __init__(self, state):
        self.pool = copy(state.pool)
        self.pool.nodes = [_copy_node(node) for node in state.pool.nodes]
        self.nodes = {node.name: node for node in self.pool.nodes}
        self.queue = dict.fromkeys(state.queue)
        self.jobs = {key: self._copy_job(job) for key, job in active_items(state.jobs)}
        self.committed = None

    def _copy_job(self, job):
        held = job.get('node')
        return dict(job, task=job.get('task', {}).copy(), result=job['result'].copy(),
                    node=self.nodes.get(held.name) if held else None)

    def record_dispatch(self, identity, job, node):
        current = _copy_node(node)
        if node.name in self.nodes:
            # Preserve object identity for jobs already pinned to this worker.
            self.nodes[node.name].__dict__.update(current.__dict__)
        else:
            self.nodes[node.name] = current
            self.pool.nodes.append(current)
        self.queue.pop(identity, None)
        self.jobs[identity] = self._copy_job(job)
        self.committed = self.jobs[identity]

    def candidates(self):
        budgets = self._project_resources()
        growth, claims = {}, None
        matching = _matching_pass(self.pool)
        for identity in self._queued_candidates(budgets):
            job = self.jobs[identity]
            task = job['task']
            group = task.get('project') or 'default'
            if job['cancel'].is_set() or group not in budgets:
                continue
            if claims is None:
                claims = self._fixed_queue_claims(budgets)
            if group not in growth:
                growth[group] = self._growth_headroom(budgets, candidate=task, fixed_claims=claims)
            headroom = dict(growth[group])
            for name, count in self._fixed_queue_headroom(task, budgets, claims).items():
                headroom[name] = max(headroom.get(name, 0), count)
            providers = matching(task)
            # No structural match is handled by the live ledger. An ordinary
            # unavailable candidate can wait for a fresh sample without a lock.
            if providers and not self.pool._ready_nodes(providers, task, needed_cpus=lambda _: startup_cpus(task),
                    node_headroom=headroom, memory_floor=job['result'].get('resource_recovery_memory_mb', 0),
                    resource_recovery=bool(job['result'].get('resource_recovery_count'))):
                continue
            yield identity, job, headroom
            if self.committed is not None:
                placed = self.committed
                self.committed = None
                budgets[group]['used'] += placed['task']['cpus']
                growth, claims, matching = {}, None, _matching_pass(self.pool)
                if _elastic_job(placed):
                    budgets.clear()
                    budgets.update(self._project_resources())

