"""Resource interruption recovery, separate from business retry allowances."""
import io
import json
import zipfile

RESOURCE_RECOVERY_DELAYS=(5.0,15.0)


class BusinessItemFailure(ValueError):
    pass


def reentrant_computation(task):
    return (task.get('resource_recovery',True) and not task.get('cwd')
            and task.get('execution',{}).get('mode')in('map','call','entry'))


def resource_recovery(task,outcome):
    return outcome.get('error_kind')=='memory_pressure'and reentrant_computation(task)


def resume_map(task,identity,request_sha256,raw,previous=None):
    """Keep only published successes; running work remains an unknown prefix."""
    execution=task.get('execution',{})
    if execution.get('mode')!='map':return None
    identity_fields=dict(id=identity,request_sha256=request_sha256,function=execution['function'],total=len(execution['items']))
    if previous is not None and any(previous.get(key)!=value for key,value in identity_fields.items()):
        raise ValueError('Previous resource recovery does not match the original map')
    retained={row['index']:row for row in (previous or {}).get('items',[])}
    with zipfile.ZipFile(io.BytesIO(raw))as archive:
        if 'files/.dco-results.json'not in archive.namelist():return previous
        state=json.loads(archive.read('files/.dco-results.json'))
    rows=state.get('items',[])
    if any(row.get('status')=='failed'for row in rows):
        raise BusinessItemFailure('Business item failure recorded before resource interruption')
    for row in rows:
        if row.get('status')!='succeeded':continue
        index=row['index']
        if index in retained and retained[index]!=row:
            raise ValueError('A completed resource recovery item changed')
        retained[index]=row
    return dict(identity_fields,items=[retained[i]for i in sorted(retained)])


def carry_outputs(files,raw):
    """Restore declared checkpoints without replacing immutable input files."""
    with zipfile.ZipFile(io.BytesIO(raw))as archive:
        for name in archive.namelist():
            if not name.startswith('files/')or name.endswith('/'):continue
            relative=name[6:]
            if (relative in('.dco-results.json','.dco-resource-resume.json','.dco-execution.json')
                    or relative.startswith(('.dco-attempts/','.dco-memory-waits/'))):continue
            target='input/'+relative
            if target not in files:files[target]=archive.read(name)


def restore_map_results(files,snapshot,*,status,error=None):
    """The last attempt may stop before main; its paid successes still exist."""
    if snapshot is None:return
    key='files/.dco-results.json'
    state=json.loads(files[key])if key in files else dict(items=[],item_states=[])
    records={row['index']:row for row in state.get('items',[])}
    states={row['index']:row for row in state.get('item_states',[])}
    for row in snapshot['items']:
        index=row['index'];records.setdefault(index,row)
        states[index]=dict(states.get(index,{}),index=index,status='succeeded',resource_recovered=True)
    state.update(completed=len(records),total=snapshot['total'],status=status,
        items=[records[i]for i in sorted(records)],
        item_states=[states.get(i,dict(index=i,status='not_started'))for i in range(snapshot['total'])])
    if error:state['error']=error
    from .bundle import json_bytes
    files[key]=json_bytes(state)
