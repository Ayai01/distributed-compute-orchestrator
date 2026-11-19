"""One durable SDK pipeline owns preparation, admission and publication."""
from collections import deque
from concurrent.futures import wait,FIRST_COMPLETED
from hashlib import sha256
from heapq import heappush,heappop
from pathlib import Path
import json,time,uuid,re,pickle,threading
from .bundle import atomic_json,task_bundle,TERMINAL,native_path
from .client import PoolBusy,RemoteError
from .worker import lock_root
from .pipeline_control import project_call


class Deferred:
    """Business inputs are not yet ready; no worker thread waits on them."""
    def __init__(self,reason='Waiting for business inputs'):self.reason=reason


class Skipped:
    """The business gate has completed and this planned item needs no execution."""
    def __init__(self,reason='Business gate skipped this item'):self.reason=reason


class _ReadyQueue:
    """Merge readiness by catalog position, with a turn for fresh inputs."""
    def __init__(self,catalog):
        self.order={key:index for index,key in enumerate(catalog)}
        self.fresh=[];self.retries=[];self.queued=set();self.retry_streak=0

    def add(self,key,*,retry=False):
        if key in self.queued:return
        heappush(self.retries if retry else self.fresh,(self.order[key],key))
        self.queued.add(key)

    def pop(self):
        # Preparation has four slots. Earlier retries retain their priority,
        # while every fourth choice can advance a fresh ready input.
        retry=bool(self.retries)and(not self.fresh or
            self.retries[0][0]<self.fresh[0][0]and self.retry_streak<3)
        _,key=heappop(self.retries if retry else self.fresh)
        self.queued.remove(key)
        self.retry_streak=self.retry_streak+1 if retry else 0
        return key

    def __bool__(self):return bool(self.fresh or self.retries)


class ExecutionResults(list):
    def __init__(self,values=(),skipped=()):
        super().__init__(values);self.skipped=list(skipped)


class ExecutionFailure(RuntimeError):
    def __init__(self,failures,results,remaining=(),blocked=()):
        super().__init__('DCO batch has '+str(len(failures))+' unresolved execution/publication errors')
        self.failures,self.results=failures,results
        self.remaining,self.blocked=list(remaining),list(blocked)


class ExecutionPaused(RuntimeError):
    def __init__(self,results,remaining):
        super().__init__('DCO admissions paused; accepted original IDs have been drained')
        self.results,self.remaining=results,list(remaining)


def _input_identity(value):
    """Hash the declared business inputs, without re-running preparation."""
    def canonical(item):
        if item is None or isinstance(item,(str,bool,int)):return item
        if isinstance(item,float):return ['float',repr(item)]
        if isinstance(item,Path):return ['path',str(item.resolve())]
        if isinstance(item,bytes):return ['bytes',sha256(item).hexdigest()]
        if isinstance(item,dict):
            pairs=[[canonical(key),canonical(value)]for key,value in item.items()]
            return ['dict',sorted(pairs,key=lambda pair:json.dumps(pair[0],sort_keys=True))]
        if isinstance(item,(list,tuple,set,frozenset)):
            values=[canonical(value)for value in item]
            if isinstance(item,(set,frozenset)):values.sort(key=lambda value:json.dumps(value,sort_keys=True))
            return [type(item).__name__,values]
        return ['value',sha256(pickle.dumps(item,protocol=4)).hexdigest()]
    return sha256(json.dumps(canonical(value),ensure_ascii=False,separators=(',',':')).encode('utf8')).hexdigest()


def execute(client,requests,*,project,output_dir,prepare=None,publish=None,ready_tasks=None,stop_when=None,cancel_when=None):
    """Execute a keyed immutable business plan.

    Each value is a request or {'spec': business_input, 'after': [business_keys]}.
    prepare(spec, job_id) returns task/files/inputs, Deferred, or Skipped.
    publish(receipt) is concurrent, idempotent scientific publication by original ID.
    Item failures leave independent work running; failed dependencies stay blocked.
    A maintenance gate drains accepted work and raises ExecutionPaused.
    cancel_when cancels this call's accepted originals and still publishes results.
    """
    root=native_path(output_dir,force=True);root.mkdir(parents=True,exist_ok=True)
    lock=lock_root(root,'pipeline')
    try:
        with project_call(client,project,root,ready_tasks)as call:
            return _execute(client,requests,project,root,prepare,publish,stop_when,cancel_when,call)
    finally:lock.close()


def _execute(client,requests,project,root,prepare,publish,stop_when,cancel_when,call):
    entries=list(requests.items()if hasattr(requests,'items')else requests)
    catalog={};dependencies={};followers={}
    for key,raw in entries:
        if not isinstance(key,str)or not key or key in catalog:raise ValueError('Business keys must be unique nonempty strings')
        catalog[key]=raw
        after=raw.get('after',[])if isinstance(raw,dict)else[]
        if not isinstance(after,list)or any(not isinstance(k,str)for k in after):raise ValueError('after requires business keys')
        dependencies[key]=set(after)
    for key,after in dependencies.items():
        if not after.issubset(catalog):raise ValueError('after refers to an unknown business key')
        for previous in after:followers.setdefault(previous,[]).append(key)
    degrees={key:len(after)for key,after in dependencies.items()}
    probe=deque(key for key,count in degrees.items()if not count);visited=0
    while probe:
        key=probe.popleft();visited+=1
        for following in followers.get(key,[]):
            degrees[following]-=1
            if not degrees[following]:probe.append(following)
    if visited!=len(catalog):raise ValueError('Business dependency graph contains a cycle')
    identities={key:_input_identity(raw)for key,raw in catalog.items()}
    manifest=dict(project=project,keys=list(catalog),after={key:sorted(after)for key,after in dependencies.items()},inputs=identities)
    manifest_path=root/'plan.json'
    if manifest_path.exists():
        if json.loads(manifest_path.read_bytes())!=manifest:raise ValueError('Output directory binds another immutable business plan')
    else:atomic_json(manifest_path,manifest)
    window=call.window
    pending={};preparing={};submitting={};collecting={};deferred=[];rows={}
    results=ExecutionResults();failures=[];completed=set();failed=set();blocked=set()
    ready=_ReadyQueue(catalog)
    paused=False;next_poll=next_gate=status_retry_at=0.
    poll_interval=.5
    previous_statuses={}
    cancellation=threading.Event();cancel_attempted=set();state_lock=threading.RLock()

    def save(row,**changes):
        with state_lock:
            row.update(changes);atomic_json(Path(row['directory'])/'state.json',row)

    def cancelling():
        if not cancellation.is_set()and cancel_when and cancel_when():
            with state_lock:
                if not cancellation.is_set():
                    for row in rows.values():
                        if row['state']in ('submitting','accepted'):save(row,cancel_requested=True)
                    cancellation.set()
        return cancellation.is_set()

    def cancel_original(row,state):
        if not(row.get('cancel_requested')or cancelling()):return
        if not row.get('cancel_requested'):save(row,cancel_requested=True)
        if state['status']not in TERMINAL and row['id']not in cancel_attempted:
            # Intent precedes the RPC. Recovery retries only this original ID.
            cancel_attempted.add(row['id'])
            client._read_reconnecting(lambda:client.cancel(row['id']))

    def execution_failed(row,receipt):
        return receipt['status']!='succeeded'and not(receipt['status']=='cancelled'and row.get('cancel_requested'))

    def row_for(key):
        if key in rows:return rows[key]
        directory=root/'jobs'/sha256(key.encode()).hexdigest();directory.mkdir(parents=True,exist_ok=True)
        path=directory/'state.json'
        if path.exists():
            row=json.loads(path.read_bytes())
            if row['key']!=key or row['project']!=project or row.get('input_identity')!=identities[key]:
                raise ValueError('State belongs to another immutable business input')
            row['directory']=str(directory)
        else:
            raw=catalog[key];original=raw.get('job_id')if isinstance(raw,dict)else None
            if original is not None and not re.fullmatch('[0-9a-f]{32}',original):raise ValueError('job_id requires an original DCO ID')
            row=dict(id=original or uuid.uuid4().hex,key=key,project=project,directory=str(directory),state='new',recovery=bool(original),input_identity=identities[key])
            save(row)
        with state_lock:rows[key]=row
        return row

    def stage(key,row):
        raw=catalog[key];directory=Path(row['directory']);payload=directory/'request.zip'
        if row.get('recovery')and row['state']=='new':
            try:
                original=client.status(row['id'])
            except RemoteError as error:
                if error.status_code!=404:raise
                receipt_source=raw.get('receipt')
                if receipt_source is None:raise RuntimeError('Original ID is absent; provide its durable receipt')
                receipt=receipt_source if isinstance(receipt_source,dict)else json.loads(Path(receipt_source).read_bytes())
                if receipt.get('id')!=row['id']:raise ValueError('Durable receipt belongs to another original ID')
                atomic_json(directory/'receipt.json',receipt);save(row,state='fetched')
                return 'fetched'
            save(row,state='accepted',request_sha256=original['request_sha256'])
            return 'accepted'
        if row['state']=='submitting'or row.get('cancel_requested'):
            try:original=client._read_reconnecting(lambda:client.status(row['id']))
            except RemoteError as error:
                if error.status_code!=404:raise
                if row.get('cancel_requested'):
                    save(row,state='prepared');return 'paused'
            else:
                if original['request_sha256']!=row['request_sha256']:
                    raise RuntimeError('Original request identity differs')
                save(row,state='accepted');return 'accepted'
        if not payload.is_file():
            spec=raw.get('spec',raw)if isinstance(raw,dict)else raw
            request=prepare(spec,row['id'])if prepare else spec
            if isinstance(request,(Deferred,Skipped)):return request
            request={'task':request}if 'steps'in request else dict(request)
            task=dict(request['task']);task['project']=project
            if 'cpus'in task:raise ValueError('Declare max_cpus; DCO assigns actual CPUs')
            data=task_bundle(task,request.get('inputs'),request.get('files'))
            temporary=directory/'request.part';temporary.write_bytes(data);temporary.replace(payload)
            atomic_json(directory/'task.json',task)
        data=payload.read_bytes()
        digest=sha256(data).hexdigest()
        if row.get('request_sha256',digest)!=digest:raise RuntimeError('Frozen request bytes differ')
        save(row,state='prepared',request_sha256=digest)
        return data

    def submit(row,data):
        if cancelling()or stop_when and stop_when():save(row,state='prepared');return 'paused'
        save(row,state='submitting')
        if cancelling()or row.get('cancel_requested'):
            save(row,state='prepared');return 'paused'
        try:accepted=client.submit_bundle(row['id'],data)
        except RemoteError as error:
            if error.status_code==409 and 'new task admissions'in str(error):
                save(row,state='prepared');return 'paused'
            # A lost reply never creates a new identity. Query the same request.
            try:accepted=client.status(row['id'])
            except RemoteError as lookup:
                if 400<=(getattr(error,'status_code',None)or 0)<500 and lookup.status_code==404:
                    save(row,state='prepared')
                raise error
        if accepted['id']!=row['id']or accepted['request_sha256']!=row['request_sha256']:
            raise RuntimeError('Original request identity differs')
        save(row,state='accepted');return 'accepted'

    def collect(row,queued=None):
        directory=Path(row['directory']);receipt_path=directory/'receipt.json'
        started=time.monotonic();timing=dict(row.get('collection_timing',{}))
        timing.setdefault('enqueued_at',queued[1]if queued else time.time())
        timing.setdefault('queue_seconds',max(0,started-queued[0])if queued else 0.0)
        if row['state']not in ('fetched','publication_failed','published','collected'):
            fetched=time.monotonic()
            try:receipt=client.fetch(row['id'],directory/'result',delete=False)
            except BaseException:
                timing['fetch_seconds']=time.monotonic()-fetched
                save(row,collection_timing=dict(timing));raise
            timing['fetch_seconds']=time.monotonic()-fetched
            transport=getattr(client,'last_fetch_timing',lambda:{})()
            if isinstance(transport,dict)and transport.get('id')==row['id']:timing['transport']=transport
            receipt['business_key']=row['key']
            persisted=time.monotonic();atomic_json(receipt_path,receipt)
            timing['receipt_write_seconds']=time.monotonic()-persisted
            save(row,state='fetched',collection_timing=dict(timing))
        else:
            receipt=json.loads(receipt_path.read_bytes())
            if (directory/'result').is_dir():receipt['output_dir']=str(directory/'result')
        if row['state']not in ('published','collected'):
            published=time.monotonic()
            try:
                if publish:publish(receipt)
            except BaseException:
                timing['publication_seconds']=timing.get('publication_seconds',0)+time.monotonic()-published
                save(row,state='publication_failed',collection_timing=dict(timing));raise
            timing['publication_seconds']=timing.get('publication_seconds',0)+time.monotonic()-published
            save(row,state='published',collection_timing=dict(timing))
        if row['state']!='collected':
            retired=time.monotonic()
            try:client.delete(row['id'])
            except RemoteError as error:
                if error.status_code!=404:
                    timing['retirement_seconds']=timing.get('retirement_seconds',0)+time.monotonic()-retired
                    save(row,collection_timing=dict(timing));raise
            timing['retirement_seconds']=timing.get('retirement_seconds',0)+time.monotonic()-retired
            save(row,state='collected',collection_timing=dict(timing))
        payload=directory/'request.zip'
        if payload.exists():payload.unlink()
        return receipt

    def queue_collection(row):
        return collect_pool.submit(collect,row,(time.monotonic(),time.time()))

    def finish(key):
        completed.add(key)
        if key in failed or key in blocked:return
        for following in followers.get(key,[]):
            if following not in blocked and dependencies[following].issubset(completed):ready.add(following)

    def failure(row,stage_name,error):
        failures.append(dict(id=row['id'],key=row['key'],stage=stage_name,error=str(error)))
        failed.add(row['key'])
        # A failed original cannot unlock its business descendants. Other
        # ready inputs retain their own admission and publication lifecycle.
        descendants=list(followers.get(row['key'],()))
        while descendants:
            key=descendants.pop()
            if key in blocked:continue
            blocked.add(key);descendants.extend(followers.get(key,()))

    with call.pools()as(prep_pool,submit_pool,collect_pool):
        recovering=set()
        # Recovery and collection continue while new admissions are closed.
        # Existing accepted IDs may be deeper in the dependency graph.
        for key,raw in catalog.items():
            path=root/'jobs'/sha256(key.encode()).hexdigest()/'state.json'
            if not path.exists()and not(isinstance(raw,dict)and raw.get('job_id')):continue
            row=row_for(key)
            if row['state']in ('collected','published','fetched','publication_failed'):
                if row['state']!='collected':call.hold(row,recovered=True)
                collecting[queue_collection(row)]=row;recovering.add(key)
            elif row['state']=='accepted':
                call.hold(row,recovered=True);pending[row['id']]=row;recovering.add(key)
            elif row['state']=='submitting'or row.get('recovery')and row['state']=='new'or row.get('cancel_requested'):
                call.hold(row,recovered=True)
                preparing[prep_pool.submit(stage,key,row)]=row;recovering.add(key)
        for key,after in dependencies.items():
            if not after and key not in recovering:ready.add(key)
        call.recovered()
        while len(completed|failed|blocked)<len(catalog)or preparing or submitting or pending or collecting:
            try:
                credit_blocked=False;progressed=False
                if cancelling():paused=True
                if time.monotonic()>=next_gate:
                    client._read_reconnecting(window.current)
                    paused=paused or bool(stop_when and stop_when())or not window.value.get('accepting',True)
                    next_gate=time.monotonic()+1.
                if not paused:
                    now=time.monotonic()
                    while deferred and deferred[0][0]<=now:
                        _,_,key=heappop(deferred);ready.add(key,retry=True)
                    while ready and len(preparing)<4:
                        key=ready.pop();row=row_for(key)
                        if key in recovering or key in completed or key in failed or key in blocked:continue
                        if row['state']=='skipped':
                            results.skipped.append(key);finish(key);continue
                        if not call.hold(row):
                            ready.add(key);credit_blocked=True;break
                        if row['state']in ('collected','published','fetched','publication_failed'):
                            collecting[queue_collection(row)]=row
                        elif row['state']=='accepted':pending[row['id']]=row
                        else:preparing[prep_pool.submit(stage,key,row)]=row
                for future in [f for f in preparing if f.done()]:
                    progressed=True
                    row=preparing.pop(future)
                    try:
                        data=future.result()
                        if isinstance(data,Deferred):
                            save(row,state='deferred',reason=data.reason)
                            heappush(deferred,(time.monotonic()+1.,ready.order[row['key']],row['key']))
                            call.release(row)
                        elif isinstance(data,Skipped):
                            save(row,state='skipped',reason=data.reason);results.skipped.append(row['key'])
                            client.release_assets(row['id']);finish(row['key'])
                            call.release(row)
                        elif data=='accepted':pending[row['id']]=row
                        elif data=='fetched':collecting[queue_collection(row)]=row
                        elif data=='paused':paused=True;call.release(row)
                        elif not paused:submitting[submit_pool.submit(submit,row,data)]=row
                        else:call.release(row)
                    except KeyboardInterrupt:paused=True
                    except BaseException as error:
                        failure(row,'prepare',error)
                        try:client.release_assets(row['id'])
                        except Exception as cleanup:failures.append(dict(id=row['id'],stage='input release',error=str(cleanup)))
                        call.release(row)
                for future in [f for f in submitting if f.done()]:
                    progressed=True
                    row=submitting.pop(future)
                    try:
                        if future.result()=='paused':
                            paused=True;call.release(row)
                        else:pending[row['id']]=row
                    except KeyboardInterrupt:
                        paused=True;preparing[prep_pool.submit(stage,row['key'],row)]=row
                    except BaseException as error:
                        failure(row,'submit',error);call.release(row)
                cancel_due = any(row.get('cancel_requested') and ident not in cancel_attempted
                                 for ident,row in pending.items())
                now=time.monotonic()
                if pending and (now>=next_poll or cancel_due and now>=status_retry_at):
                    try:
                        states=client.statuses(pending,compact=True)
                    except RemoteError as error:
                        if not (error.transport_retryable or error.status_code in (502,503,504)
                                or isinstance(error,PoolBusy)):
                            raise
                        # Keep accepted originals while preparation and collection continue.
                        poll_interval=min(5.,poll_interval*2)
                        next_poll=status_retry_at=time.monotonic()+poll_interval
                        states=None
                else:
                    states=None
                if states is not None:
                    status_retry_at=0.
                    snapshot={ident:state['status'] for ident,state in states.items()}
                    changed=any(status!=previous_statuses.get(ident) for ident,status in snapshot.items())
                    poll_interval=.5 if changed else min(5.,poll_interval*2)
                    previous_statuses=snapshot
                    for job_id,state in states.items():
                        row=pending[job_id]
                        if state['status']!='unknown':
                            try:cancel_original(row,state)
                            except Exception as error:failure(row,'cancel',error)
                        if state['status']in TERMINAL:
                            pending.pop(job_id);collecting[queue_collection(row)]=row
                        elif state['status']=='unknown':
                            failure(row,'recovery','Accepted original ID missing');pending.pop(job_id)
                    next_poll=time.monotonic()+poll_interval
                for future in [f for f in collecting if f.done()]:
                    progressed=True
                    row=collecting.pop(future)
                    try:
                        receipt=future.result();results.append(receipt)
                        if execution_failed(row,receipt):failure(row,'execution',receipt['status'])
                        finish(row['key'])
                    except KeyboardInterrupt:
                        paused=True
                        receipt_path=Path(row['directory'])/'receipt.json'
                        if row['state']in ('published','collected')and receipt_path.is_file():
                            results.append(json.loads(receipt_path.read_bytes()))
                            if row['state']=='collected':finish(row['key'])
                    except BaseException as error:failure(row,'collection/publication',error)
                    call.release(row)
                if not(preparing or submitting or pending or collecting):
                    # Unpublished originals keep their project credits. Once
                    # those credits prevent further work, return the durable
                    # failure for recovery rather than waiting on this call.
                    if paused or failures and credit_blocked and not progressed:break
                    if len(completed|failed|blocked)==len(catalog):break
                futures=[*preparing,*submitting,*collecting]
                if futures:wait(futures,timeout=.05,return_when=FIRST_COMPLETED)
                elif pending or deferred:time.sleep(.05)
                elif ready:call.wait()
                elif len(completed)<len(catalog):raise RuntimeError('Business dependency has no remaining runnable predecessor')
            except KeyboardInterrupt:
                paused=True
    # A signal can arrive between Executor.submit and registry assignment.
    # Joined submitters have durable rows; drain those original IDs as well.
    for row in rows.values():
        if (row['key']in completed or row['state']not in ('accepted','submitting')
                or any(f['id']==row['id']and f['stage']=='collection/publication'for f in failures)):continue
        try:
            try:state=client._read_reconnecting(lambda:client.status(row['id']))
            except RemoteError as error:
                if error.status_code!=404:raise
                save(row,state='prepared');continue
            try:cancel_original(row,state)
            except Exception as error:failure(row,'cancel',error)
            client.wait(row['id'])
            receipt=collect(row);results.append(receipt);finish(row['key'])
            if execution_failed(row,receipt):failure(row,'execution',receipt['status'])
        except KeyboardInterrupt:paused=True
        except Exception as error:failure(row,'drain',error)
    remaining=[key for key in catalog if key not in completed]
    blocked_keys=[key for key in catalog if key in blocked and key not in completed]
    cancelled=cancellation.is_set()or any(row['status']=='cancelled'for row in results)
    atomic_json(root/'batch.json',dict(status='failed'if failures else'cancelled'if cancelled else'paused'if remaining else'succeeded',
        completed=len(completed),accepted_results=len(results),skipped=results.skipped,remaining=remaining,
        failures=failures,blocked=blocked_keys))
    if failures:raise ExecutionFailure(failures,results,remaining,blocked_keys)
    if remaining:raise ExecutionPaused(results,remaining)
    return results

