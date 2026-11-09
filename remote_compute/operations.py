"""Endpoint-owned operator cleanup at the queue's dispatch boundary."""
import threading
import time

from .records import archive_job,asset_references


def clean_pending(service, *, discard=None):
    # Queue removal and cancellation share the scheduler's admission lock.
    # Already dispatched/running work never enters this deletion set.
    with service.condition:
        targets=[]
        for identity in tuple(service.queue):
            job=service.jobs[identity]
            if job['result']['status']!='queued':continue
            job['cancel'].set()
            service.queue.remove(identity)
            job['result'].update(status='cancelled',finished=time.time())
            service._save(job)
            archive_job(service.jobs,job)
            job.setdefault('artifact_lock',threading.RLock())
            targets.append((identity,job))
        service.condition.notify_all()
    deleted,errors=[],{}
    assets={asset for _,job in targets for asset in job['result'].get('assets',{}).values()}
    for identity,job in targets:
        # Filesystem work and remote attempt cleanup hold only this original ID.
        try:
            with job['artifact_lock']:
                with service.condition:present=service.jobs.get(identity)is job
                if present:
                    if discard is not None:discard(job)
                    service.delete(identity)
            deleted.append(identity)
        except (OSError,ValueError,RuntimeError) as error:
            errors[identity]=str(error)
    retired,shared=[],[]
    for asset in sorted(assets):
        try:
            with service.asset_cache.lock,service.condition:
                protected=asset_references(service.jobs)|set(getattr(service,'accepting_assets',()))
                for held,expires in service.asset_handoffs.values():
                    if expires>time.monotonic():protected.update(held)
                transfer=service.asset_writes.get(asset)
                if asset in protected or transfer is not None and transfer.locked():
                    shared.append(asset)
                    continue
                service.asset_cache.retire(asset)
                if hasattr(service,'legacy_assets'):service.legacy_assets.discard(asset)
                retired.append(asset)
        except (OSError,ValueError)as error:errors[asset]=str(error)
    return dict(status='incomplete'if errors else'completed',matched=len(targets),
                deleted=deleted,errors=errors,retired_assets=retired,shared_assets=shared)
