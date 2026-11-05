"""One durable SDK pipeline owns preparation, admission and publication."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from hashlib import sha256
from pathlib import Path
import json,time,uuid,re,pickle
from .bundle import atomic_json,task_bundle,TERMINAL,native_path
from .client import RemoteError
from .worker import lock_root


class Deferred:
    """Business inputs are not yet ready; no worker thread waits on them."""
    def __init__(self,reason='Waiting for business inputs'):self.reason=reason


class Skipped:
    """The business gate has completed and this planned item needs no execution."""
    def __init__(self,reason='Business gate skipped this item'):self.reason=reason


class ExecutionResults(list):
    def __init__(self,values=(),skipped=()):
        super().__init__(values);self.skipped=list(skipped)


class ExecutionFailure(RuntimeError):
    def __init__(self,failures,results):
        super().__init__('DCO batch has '+str(len(failures))+' unresolved execution/publication errors')
        self.failures,self.results=failures,results


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


def execute(client,requests,*,project,output_dir,prepare=None,publish=None,ready_tasks=None,stop_when=None):
    """Execute a keyed immutable business plan.

    Each value is a request or {'spec': business_input, 'after': [business_keys]}.
    prepare(spec, job_id) returns task/files/inputs, Deferred, or Skipped.
    publish(receipt) is concurrent, idempotent scientific publication by original ID.
    A maintenance gate drains accepted work and raises ExecutionPaused.
    """
    root=native_path(output_dir,force=True);root.mkdir(parents=True,exist_ok=True)
    identity=sha256((client.url+'\0'+project).encode()).hexdigest()
    lock=lock_root(Path.cwd()/'.local/dco-client'/identity,'pipeline')
    try:return _execute(client,requests,project,root,prepare,publish,ready_tasks,stop_when)
    finally:lock.close()


def _execute(client,requests,project,root,prepare,publish,ready_tasks,stop_when):
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
    options={}if ready_tasks is None else dict(ready_tasks=ready_tasks)
    window=client.window(project,**options)
    pending={};preparing={};submitting={};collecting={};deferred={};rows={}
    results=ExecutionResults();failures=[];completed=set()
    ready=deque(key for key,after in dependencies.items()if not after)
    stopped=paused=False;next_poll=next_retry=next_gate=0.

    def save(row,**changes):
        row.update(changes);atomic_json(Path(row['directory'])/'state.json',row)

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
        rows[key]=row;return row

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
        if row['state']=='submitting':
            try:original=client._read_reconnecting(lambda:client.status(row['id']))
            except RemoteError as error:
                if error.status_code!=404:raise
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
        if stop_when and stop_when():save(row,state='prepared');return 'paused'
        save(row,state='submitting')
        try:accepted=client.submit_bundle(row['id'],data)
        except RemoteError as error:
            if error.status_code==409 and 'new task admissions'in str(error):
                save(row,state='prepared');return 'paused'
            # A lost reply never creates a new identity. Query the same request.
            try:accepted=client.status(row['id'])
            except RemoteError:raise error
        if accepted['id']!=row['id']or accepted['request_sha256']!=row['request_sha256']:
            raise RuntimeError('Original request identity differs')
        save(row,state='accepted');return 'accepted'

    def collect(row):
        directory=Path(row['directory']);receipt_path=directory/'receipt.json'
        if row['state']not in ('fetched','publication_failed','published','collected'):
            receipt=client.fetch(row['id'],directory/'result',delete=False)
            receipt['business_key']=row['key'];atomic_json(receipt_path,receipt);save(row,state='fetched')
        else:
            receipt=json.loads(receipt_path.read_bytes())
            if (directory/'result').is_dir():receipt['output_dir']=str(directory/'result')
        if row['state']not in ('published','collected'):
            try:
                if publish:publish(receipt)
            except BaseException:
                save(row,state='publication_failed');raise
            save(row,state='published')
        if row['state']!='collected':
            try:client.delete(row['id'])
            except RemoteError as error:
                if error.status_code!=404:raise
            save(row,state='collected')
        payload=directory/'request.zip'
        if payload.exists():payload.unlink()
        return receipt

    def finish(key):
        nonlocal next_retry
        completed.add(key);next_retry=0.
        for following in followers.get(key,[]):
            if dependencies[following].issubset(completed):ready.append(following)

    def failure(row,stage_name,error):
        nonlocal stopped
        failures.append(dict(id=row['id'],key=row['key'],stage=stage_name,error=str(error)));stopped=True

    with ThreadPoolExecutor(4,thread_name_prefix='dco-prepare')as prep_pool, \
         ThreadPoolExecutor(2,thread_name_prefix='dco-submit')as submit_pool, \
         ThreadPoolExecutor(2,thread_name_prefix='dco-collect')as collect_pool:
        recovering=set()
        # Recovery and collection continue while new admissions are closed.
        # Existing accepted IDs may be deeper in the dependency graph.
        for key,raw in catalog.items():
            path=root/'jobs'/sha256(key.encode()).hexdigest()/'state.json'
            if not path.exists()and not(isinstance(raw,dict)and raw.get('job_id')):continue
            row=row_for(key)
            if row['state']in ('collected','published','fetched','publication_failed'):
                collecting[collect_pool.submit(collect,row)]=row;recovering.add(key)
            elif row['state']=='accepted':pending[row['id']]=row;recovering.add(key)
            elif row['state']=='submitting'or row.get('recovery')and row['state']=='new':
                preparing[prep_pool.submit(stage,key,row)]=row;recovering.add(key)
        ready=deque(key for key in ready if key not in recovering)
        while len(completed)<len(catalog)or preparing or submitting or pending or collecting:
            try:
                if time.monotonic()>=next_gate:
                    client._read_reconnecting(window.current)
                    paused=paused or bool(stop_when and stop_when())or not window.value.get('accepting',True)
                    next_gate=time.monotonic()+1.
                if not stopped and not paused:
                    if deferred and time.monotonic()>=next_retry:
                        ready.extend(deferred);deferred.clear();next_retry=time.monotonic()+1.
                    available=window.available(len(preparing)+len(submitting)+len(pending)+len(collecting))
                    while ready and available and len(preparing)<4:
                        key=ready.popleft();row=row_for(key)
                        if key in recovering or key in completed:continue
                        if row['state']=='skipped':
                            results.skipped.append(key);finish(key);continue
                        if row['state']in ('collected','published','fetched','publication_failed'):
                            collecting[collect_pool.submit(collect,row)]=row
                        elif row['state']=='accepted':pending[row['id']]=row
                        else:preparing[prep_pool.submit(stage,key,row)]=row
                        available-=1
                for future in [f for f in preparing if f.done()]:
                    row=preparing.pop(future)
                    try:
                        data=future.result()
                        if isinstance(data,Deferred):
                            save(row,state='deferred',reason=data.reason);deferred[row['key']]=True
                        elif isinstance(data,Skipped):
                            save(row,state='skipped',reason=data.reason);results.skipped.append(row['key'])
                            client.release_assets(row['id']);finish(row['key'])
                        elif data=='accepted':pending[row['id']]=row
                        elif data=='fetched':collecting[collect_pool.submit(collect,row)]=row
                        elif not stopped and not paused:submitting[submit_pool.submit(submit,row,data)]=row
                    except KeyboardInterrupt:paused=True
                    except BaseException as error:
                        failure(row,'prepare',error)
                        try:client.release_assets(row['id'])
                        except Exception as cleanup:failures.append(dict(id=row['id'],stage='input release',error=str(cleanup)))
                for future in [f for f in submitting if f.done()]:
                    row=submitting.pop(future)
                    try:
                        if future.result()=='paused':paused=True
                        else:pending[row['id']]=row
                    except KeyboardInterrupt:
                        paused=True;preparing[prep_pool.submit(stage,row['key'],row)]=row
                    except BaseException as error:failure(row,'submit',error)
                if pending and time.monotonic()>=next_poll:
                    states=client._read_reconnecting(lambda:client.statuses(pending))
                    for job_id,state in states.items():
                        row=pending[job_id]
                        if state['status']in TERMINAL:
                            pending.pop(job_id);collecting[collect_pool.submit(collect,row)]=row
                        elif state['status']=='unknown':
                            failure(row,'recovery','Accepted original ID missing');pending.pop(job_id)
                    next_poll=time.monotonic()+.5
                for future in [f for f in collecting if f.done()]:
                    row=collecting.pop(future)
                    try:
                        receipt=future.result();results.append(receipt)
                        if receipt['status']!='succeeded':failure(row,'execution',receipt['status'])
                        finish(row['key'])
                    except KeyboardInterrupt:
                        paused=True
                        receipt_path=Path(row['directory'])/'receipt.json'
                        if row['state']in ('published','collected')and receipt_path.is_file():
                            results.append(json.loads(receipt_path.read_bytes()))
                            if row['state']=='collected':finish(row['key'])
                    except BaseException as error:failure(row,'collection/publication',error)
                if (stopped or paused)and not(preparing or submitting or pending or collecting):break
                futures=[*preparing,*submitting,*collecting]
                if futures:wait(futures,timeout=.05,return_when=FIRST_COMPLETED)
                elif pending or deferred:time.sleep(.05)
                elif ready:continue
                elif len(completed)<len(catalog):raise RuntimeError('Business dependency has no remaining runnable predecessor')
            except KeyboardInterrupt:
                paused=True
    # A signal can arrive between Executor.submit and registry assignment.
    # Joined submitters have durable rows; drain those original IDs as well.
    for row in rows.values():
        if (row['key']in completed or row['state']not in ('accepted','submitting')
                or any(f['id']==row['id']and f['stage']=='collection/publication'for f in failures)):continue
        try:
            try:client._read_reconnecting(lambda:client.status(row['id']))
            except RemoteError as error:
                if error.status_code!=404:raise
                save(row,state='prepared');continue
            client.wait(row['id'])
            receipt=collect(row);results.append(receipt);finish(row['key'])
            if receipt['status']!='succeeded':failure(row,'execution',receipt['status'])
        except KeyboardInterrupt:paused=True
        except Exception as error:failure(row,'drain',error)
    remaining=[key for key in catalog if key not in completed]
    atomic_json(root/'batch.json',dict(status='failed'if failures else'paused'if remaining else'succeeded',
        completed=len(completed),accepted_results=len(results),skipped=results.skipped,remaining=remaining))
    if failures:raise ExecutionFailure(failures,results)
    if remaining:raise ExecutionPaused(results,remaining)
    return results

