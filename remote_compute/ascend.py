"""Configure an installed Ascend PyTorch runtime without installing packages."""
import json
from pathlib import Path
import re
import subprocess
import sys


_PROBE = '''import json,os,torch,torch_npu
if not torch.npu.is_available():
    raise RuntimeError('No Ascend NPU is visible to this runtime')
count=torch.npu.device_count()
visible=os.environ.get('ASCEND_RT_VISIBLE_DEVICES')
ids=visible.split(',') if visible else [str(i) for i in range(count)]
if len(ids)!=count or any(not i.isdigit() for i in ids):
    raise RuntimeError('Ascend runtime visibility differs from its reported devices')
profiles=[]
for index,identity in enumerate(ids):
    properties=torch.npu.get_device_properties(index)
    x=torch.tensor([1.,2.],device='npu:'+str(index),requires_grad=True)
    loss=(x*x).sum()
    loss.backward()
    if x.grad.cpu().tolist()!=[2.,4.]:
        raise RuntimeError('Ascend backward probe failed')
    profiles.append(dict(id=identity,name=properties.name,memory_mb=int(properties.total_memory)//1048576))
env={k:v for k,v in os.environ.items() if k in ('PATH','LD_LIBRARY_PATH','PYTHONPATH') or k.startswith(('ASCEND_','CANN_'))}
print('DCO_ASCEND_PROBE='+json.dumps(dict(python=__import__('sys').executable,torch=torch.__version__,torch_npu=torch_npu.__version__,devices=profiles,env=env)))
'''


def configure(*, python=None, cann_env=None):
    """Probe the mounted devices once and return ordinary worker config fields."""
    if sys.platform != 'linux':
        raise ValueError('Ascend setup runs inside the Linux Ascend host or container')
    executable = str(Path(python or sys.executable).absolute())
    candidates = [Path(cann_env)] if cann_env else [
        Path('/usr/local/Ascend/ascend-toolkit/set_env.sh'),
        Path('/usr/local/Ascend/ascend-toolkit/latest/set_env.sh'),
        Path('/usr/local/Ascend/cann/set_env.sh')]
    script = next((p for p in candidates if p.is_file()), None)
    if cann_env and script is None:
        raise ValueError('CANN environment script does not exist: '+str(cann_env))
    argv = (['bash','-c','source "$1" >/dev/null && exec "$2" -c "$3"',
             'dco-ascend',str(script),executable,_PROBE] if script else [executable,'-c',_PROBE])
    completed = subprocess.run(argv,capture_output=True,encoding='utf-8',errors='replace',timeout=180)
    # Some installed CANN kernels print progress dots without a trailing LF.
    lines = [line.partition('DCO_ASCEND_PROBE=')[2] for line in completed.stdout.splitlines()
             if 'DCO_ASCEND_PROBE=' in line]
    if completed.returncode or not lines:
        raise ValueError('Ascend runtime probe failed (exit '+str(completed.returncode)+'): '+
                         (completed.stderr or completed.stdout)[-1800:])
    probe = json.loads(lines[-1])
    profiles = []
    for device in probe['devices']:
        name = re.sub('[^a-z0-9]+','',device['name'].lower())
        profiles.append(dict(kind='npu',count=1,memory_mb=device['memory_mb'],ids=[device['id']],
                             tags=['npu:pytorch','npu:training','npu:inference',*(['npu:'+name] if name else [])]))
    return dict(tools={},python_runtimes={'ml':[probe['python']],'npu':[probe['python']]},env=probe['env'],
                accelerators=profiles,ascend_runtime={
                    'torch':probe['torch'],'torch_npu':probe['torch_npu'],
                    'cann_env':str(script) if script else None,'devices':probe['devices']})
