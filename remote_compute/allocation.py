"""Integer weighted shares, redistributing demand that a project cannot use."""
import math


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
