"""Integer weighted shares, redistributing demand that a project cannot use."""
import math
import heapq

STARTUP_CPU_FLOOR = 'startup_cpu_floor'
ACTIVE_GRANT_CONFLICT = 'active_cpus must count threads within the existing grant and requested width'


def validate_cpu_overcommit(value):
    if isinstance(value,bool)or not isinstance(value,(int,float))or not math.isfinite(value)or not 1<=value<=1.5:
        raise ValueError('cpu_overcommit_factor must be a finite number in 1..1.5')
    return float(value)


def validate_cpu_utilization(value):
    if isinstance(value,bool)or not isinstance(value,(int,float))or not math.isfinite(value)or not 1<=value<=100:
        raise ValueError('cpu_overcommit_utilization must be a finite percentage in 1..100')
    return float(value)


def cpu_capacity(base,used,percent,factor,utilization=90):
    """Lend bounded slots below the configured host utilization threshold."""
    capacity=base
    if used>=base and percent is not None and math.isfinite(percent)and 0<=percent<utilization:
        capacity=int(base*factor)
    return capacity


def startup_cpus(task):
    """Width required before an unassigned task can start."""
    return task.get('cpus', 1) or task.get('min_start_cpus', 1)


def validate_min_start_cpus(value, maximum):
    if type(value) is not int or type(maximum) is not int or not 1 <= value <= maximum:
        raise ValueError('min_start_cpus must be an integer in 1..max_cpus')


def elastic_cpus(task):
    """The accepted request can change width beyond its startup grant."""
    initial=task.get('cpus',1)
    return initial==0 or task.get('max_cpus',initial)>initial


def plan_resize(cpus, memory_mb, base_memory_mb, memory_per_cpu_mb, memory_limit_mb, available_mb):
    """Fit a phase's linear RAM demand before granting its CPU count."""
    if base_memory_mb is None and memory_per_cpu_mb is None:
        if memory_limit_mb is not None:
            raise ValueError('memory_limit_mb requires a phase memory model')
        return cpus,memory_mb
    values=(base_memory_mb,memory_per_cpu_mb)
    if memory_mb is not None or any(type(v) is not int or v<0 for v in values) or sum(values)<1:
        raise ValueError('Use base_memory_mb + memory_per_cpu_mb with nonnegative integers, without memory_mb')
    if memory_limit_mb is not None and (type(memory_limit_mb) is not int or memory_limit_mb<1):
        raise ValueError('memory_limit_mb must be a positive integer')
    available=int(min(available_mb,memory_limit_mb)) if memory_limit_mb is not None else int(available_mb)
    if available<base_memory_mb:return 0,None
    if memory_per_cpu_mb:cpus=min(cpus,(available-base_memory_mb)//memory_per_cpu_mb)
    return max(0,cpus),base_memory_mb+max(0,cpus)*memory_per_cpu_mb


def weighted_threads(capacity, shares, demands):
    allocated = {name: 0 for name in shares}
    remaining = capacity
    while remaining:
        active = [name for name in shares if allocated[name] < demands[name]]
        if not active:
            break
        total = sum(shares[name] for name in active)
        exact = {name: remaining * shares[name] / total for name in active}
        grants = {name: min(demands[name] - allocated[name], math.floor(exact[name])) for name in active}
        for name, grant in grants.items():
            allocated[name] += grant
            remaining -= grant
        if not remaining:
            break
        for name in sorted(active, key=lambda key: (-(exact[key] - math.floor(exact[key])), key)):
            if remaining and allocated[name] < demands[name]:
                allocated[name] += 1
                remaining -= 1
    return allocated


def weighted_node_threads(capacities, shares, demands, rooms, *, current_used=None, fixed_used=None):
    """Integer weighted max-min shares, with only usable node edges in the flow."""
    if len(capacities)==1 and current_used is None and fixed_used is None:
        node,capacity=next(iter(capacities.items()))
        grants=weighted_threads(capacity,shares,{p:min(demands[p],rooms.get(p,{}).get(node,0)) for p in shares})
        return {p:({node:grants[p]} if node in rooms.get(p,{}) else {}) for p in shares}
    placed={name:{node:0 for node in rooms.get(name,{})} for name in shares}
    used={node:0 for node in capacities}
    total={name:0 for name in shares}
    rank=(lambda name:((total[name]+(fixed_used or {}).get(name,0))/shares[name],current_used.get(name,0)/shares[name],name))if current_used is not None else lambda name:((total[name]+1)/shares[name],name)
    # Running jobs with a fixed provider cannot exchange cores between hosts.
    # Interleaving their independent heaps gives exactly the same integer grants.
    pinned = {}
    for name in shares:
        edges = [node for node,limit in rooms.get(name,{}).items() if node in capacities and limit > 0]
        if len(edges) > 1:
            break
        if edges and demands[name] > 0:
            pinned.setdefault(edges[0], []).append((rank(name),name))
    else:
        for node, queue in pinned.items():
            heapq.heapify(queue)
            remaining = capacities[node]
            while queue and remaining > 0:
                _,name = heapq.heappop(queue)
                total[name] += 1
                placed[name][node] += 1
                remaining -= 1
                if total[name] < min(demands[name],rooms[name][node]):
                    heapq.heappush(queue,(rank(name),name))
        return placed
    degree={node:sum(node in edges for edges in rooms.values()) for node in capacities}
    def place(name,seen):
        nodes=[node for node in placed[name] if node in capacities and node not in seen
               and placed[name][node]<rooms[name][node]]
        nodes.sort(key=lambda node:(used[node]>=capacities[node],
                                   degree[node],node))
        for node in nodes:
            seen.add(node)
            if used[node]<capacities[node]:
                placed[name][node]+=1;used[node]+=1
                return True
            for other in shares:
                if other!=name and len(placed[other])>1 and placed[other].get(node,0) and place(other,seen):
                    placed[other][node]-=1
                    placed[name][node]+=1
                    return True
        return False
    # Project opportunity uses current occupancy to break integer share ties.
    queue=[(rank(name),name) for name in shares if demands[name]>0]
    heapq.heapify(queue)
    remaining=sum(capacities.values())
    while remaining>0 and queue:
        _,name=heapq.heappop(queue)
        if place(name,set()):
            total[name]+=1
            remaining-=1
            if total[name]<demands[name]:heapq.heappush(queue,(rank(name),name))
    return placed
