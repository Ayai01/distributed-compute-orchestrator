"""Integer weighted shares, redistributing demand that a project cannot use."""
import math


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
