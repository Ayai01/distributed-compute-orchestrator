"""Accepted work keeps its frozen request on disk, with no polling-time ZIP copy."""
import hashlib,sys,time,unittest
from pathlib import Path
from unittest.mock import patch
from remote_compute import Pool
from test_compute import provider,scratch,until,python
from test_center import center

class TransportLifetimeTests(unittest.TestCase):
 def test_accepted_body_releases_buffers_while_original_input_remains_readable(self):
  data=bytes(range(256))*8192;expected=hashlib.sha256(data).hexdigest()
  body="import hashlib,time\nfrom pathlib import Path\nPath('ready').write_text(hashlib.sha256(Path('large.bin').read_bytes()).hexdigest())\nwhile not Path('release').exists():time.sleep(.01)\nprint(Path('ready').read_text())"
  with scratch() as folder,provider(Path(folder)/'worker') as (_,remote),Pool([remote]) as pool,center(pool,Path(folder)/'center') as (state,client):
   identity=client.submit(python(body,timeout=20,retries=0),files={'large.bin':data})['id']
   try:
    def ready():
     try:return client.read_job_file(identity,'ready').decode()==expected
     except Exception as e:
      if getattr(e,'status_code',None)==404:return False
      raise
    until(ready,seconds=8);found=[]
    for top in list(sys._current_frames().values()):
     f=top
     while f is not None:
      if f.f_code.co_name=='_execute' and f.f_globals.get('__name__')=='remote_compute.coordinator' and f.f_locals.get('job',{}).get('result',{}).get('id')==identity:found.append(f)
      f=f.f_back
    self.assertEqual(len(found),1)
    self.assertIsNone(found[0].f_locals.get('payload'))
    self.assertIsNone(found[0].f_locals.get('files'))
    request=state.jobs[identity]['directory']/'request.zip';sha=hashlib.sha256(request.read_bytes()).hexdigest()
    self.assertEqual(client.status(identity)['request_sha256'],sha)
   finally:client.write_job_file(identity,'release','yes')
   result=client.wait(identity,wait_timeout=8);self.assertEqual(result['status'],'succeeded');self.assertEqual(result['stdout'].strip(),expected);self.assertEqual(result['request_sha256'],sha);self.assertEqual(result['attempts'],[])

 def test_recovery_rereads_original_large_input_after_transport_was_released(self):
  data=bytes(range(251))*8192;expected=hashlib.sha256(data).hexdigest()
  with scratch() as folder,provider(Path(folder)/'lost') as (_,lost),provider(Path(folder)/'healthy') as (_,healthy),Pool([lost,healthy],policy='round_robin') as pool,center(pool,Path(folder)/'center') as (_,client):
   def interrupted(*args,**kwargs):
    pool.nodes[0].enabled=False
    return dict(status='interrupted',error='fixture interruption after acceptance',finished=time.time(),cpu_seconds=1.25)
   with patch.object(lost,'submit_bundle',return_value={}),patch.object(lost,'status',side_effect=interrupted):
    accepted=client.submit(python("import hashlib;from pathlib import Path;print(hashlib.sha256(Path('large.bin').read_bytes()).hexdigest())",retries=1,timeout=15),files={'large.bin':data});result=client.wait(accepted['id'],wait_timeout=10)
   self.assertEqual(result['status'],'succeeded');self.assertEqual(result['id'],accepted['id']);self.assertEqual(result['request_sha256'],accepted['request_sha256']);self.assertEqual(result['stdout'].strip(),expected);self.assertEqual(result['attempts'][0]['cpu_seconds'],1.25);self.assertEqual(result['total_cpu_seconds'],1.25+result['cpu_seconds'])

if __name__=='__main__':unittest.main()
