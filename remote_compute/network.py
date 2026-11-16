"""Pinned frp STCP transport. Nginx terminates WSS at /~!frp on existing 443.

Each frpc owns one native process; frp supplies reconnection and multiplexing.
Worker/center expose STCP services; center/site visitors bind only loopback.
DCO device credentials remain required inside these authenticated tunnels.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import subprocess
import tarfile
import tempfile
import time
from urllib.request import urlopen
import zipfile

from .bundle import atomic_json


FRP_VERSION = '0.71.1'
FRP_RELEASE = 'https://github.com/fatedier/frp/releases/download/v' + FRP_VERSION
# GitHub official release asset digests, read 2026-11-16.
FRP_SHA256 = {
    'windows_amd64': 'b90074b2b6a1d792a7a40f085ac6d276bcab2d1bc60201f8a86ca53ce65ce821',
    'linux_amd64': '1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803',
    'linux_arm64': '8edf590b0c396fa42f9c147121a76af83cfc2002b08f5aef0ea1008c43482019',
}
NGINX_LOCATION = '''location = /~!frp {
    proxy_pass http://127.0.0.1:27000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}
'''


def _name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value):
        raise ValueError('Device/proxy names use 1..64 letters, digits, underscore or dash')
    return value


def _port(value):
    if type(value) is not int or not 0 < value < 65536:
        raise ValueError('Port must be an integer in 1..65535')
    return value


def _auth(token_file):
    value=os.fspath(token_file)
    # Deployment may render a Linux config on Windows before transferring it.
    path=value if value.startswith('/') or re.match(r'^[A-Za-z]:[\\/]',value) else str(Path(value).resolve())
    return dict(method='token', additionalScopes=['HeartBeats', 'NewWorkConns'],
                tokenSource=dict(type='file', file=dict(path=path)))


def stcp_proxy(name, secret_key, local_port=26133, *, allow_users=()):
    """Expose a local DCO service; visitors authenticate separately per service."""
    if not secret_key:raise ValueError('STCP secret_key is required')
    return dict(name=_name(name), type='stcp', secretKey=secret_key,
                localIP='127.0.0.1', localPort=_port(local_port),
                allowUsers=[_name(user) for user in allow_users],
                transport=dict(useEncryption=True, useCompression=False))


def stcp_visitor(name, server_user, server_name, secret_key, bind_port):
    if not secret_key:raise ValueError('STCP secret_key is required')
    return dict(name=_name(name), type='stcp', serverUser=_name(server_user),
                serverName=_name(server_name), secretKey=secret_key,
                bindAddr='127.0.0.1', bindPort=_port(bind_port),
                transport=dict(useEncryption=True, useCompression=False))


def frpc_config(device_id, token_file, *, server='nurikoi.com', server_port=443,
                proxies=(), visitors=(), ca_file):
    """One outbounds-only WSS client, with optional services and private visitors."""
    config=dict(user=_name(device_id), clientID=device_id, serverAddr=server,
                serverPort=_port(server_port), loginFailExit=False, auth=_auth(token_file),
                transport=dict(protocol='wss', tcpMux=True, tcpMuxKeepaliveInterval=30,
                               tls=dict(enable=True, disableCustomTLSFirstByte=True, serverName=server)),
                log=dict(to='console', level='warn', disablePrintColor=True),
                proxies=list(proxies), visitors=list(visitors))
    config['transport']['tls']['trustedCaFile']=str(Path(ca_file).resolve())
    return config


def frps_config(token_file, *, bind_port=27000):
    """Only Nginx reaches this listener. STCP allocates no public service ports."""
    return dict(bindAddr='127.0.0.1', bindPort=_port(bind_port), proxyBindAddr='127.0.0.1',
                auth=_auth(token_file), transport=dict(tcpMux=True),
                log=dict(to='console', level='warn', disablePrintColor=True))


def write_config(path, config):
    path=Path(path);path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, config, private=True)
    return path


def install_frp(destination, *, target=None):
    """Fetch official pinned archives, verify their fixed SHA256, extract binaries."""
    machine={'x86_64':'amd64','amd64':'amd64','aarch64':'arm64','arm64':'arm64'}.get(platform.machine().lower())
    target=target or platform.system().lower()+'_'+str(machine)
    if target not in FRP_SHA256:raise ValueError('Supported frp platforms: '+', '.join(FRP_SHA256))
    suffix='.zip' if target.startswith('windows') else '.tar.gz'
    stem='frp_'+FRP_VERSION+'_'+target
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    temp_root=Path(os.environ['LOCALAPPDATA'])/'Temp' if os.name=='nt' else None
    with tempfile.TemporaryDirectory(prefix='dco-frp-',dir=temp_root) as folder:
        archive=Path(folder)/('frp'+suffix)
        digest=hashlib.sha256()
        with urlopen(FRP_RELEASE+'/'+stem+suffix,timeout=60) as source, archive.open('wb') as output:
            while block:=source.read(1024*1024):digest.update(block);output.write(block)
        if digest.hexdigest()!=FRP_SHA256[target]:raise ValueError('Official frp archive SHA256 differs')
        extension='.exe' if target.startswith('windows') else ''
        if suffix=='.zip':
            with zipfile.ZipFile(archive) as package:
                for executable in ('frpc','frps'):
                    with package.open(stem+'/'+executable+extension) as source, (destination/(executable+extension)).open('wb') as output:
                        shutil.copyfileobj(source,output,1024*1024)
        else:
            with tarfile.open(archive,'r:gz') as package:
                for executable in ('frpc','frps'):
                    with package.extractfile(stem+'/'+executable) as source, (destination/executable).open('wb') as output:
                        shutil.copyfileobj(source,output,1024*1024)
        if os.name!='nt':
            for executable in ('frpc','frps'):(destination/(executable+extension)).chmod(0o700)
    return dict(version=FRP_VERSION,target=target,sha256=FRP_SHA256[target],directory=str(destination.resolve()))


class FrpProcess:
    """Own one verified child; native service managers own the enclosing lifetime."""
    def __init__(self, executable, config, log_file):
        self.executable=Path(executable).resolve();self.config=Path(config).resolve()
        self.log_file=Path(log_file);self.process=None;self.log=None

    def start(self):
        if self.process is not None:raise RuntimeError('frp process already started')
        flags={'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {}
        version=subprocess.run([str(self.executable),'-v'],capture_output=True,text=True,check=True,**flags).stdout.strip()
        if version!=FRP_VERSION:raise ValueError('Expected frp '+FRP_VERSION+', got '+version)
        subprocess.run([str(self.executable),'verify','-c',str(self.config)],check=True,
                       stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,**flags)
        self.log_file.parent.mkdir(parents=True,exist_ok=True)
        self.log=self.log_file.open('ab')
        try:
            self.process=subprocess.Popen([str(self.executable),'-c',str(self.config)],stdin=subprocess.DEVNULL,
                                          stdout=self.log,stderr=subprocess.STDOUT,**flags)
        except BaseException:
            self.log.close();self.log=None
            raise
        return self

    def wait_port(self, port, *, timeout=20):
        expires=time.monotonic()+timeout
        while time.monotonic()<expires:
            if self.process.poll() is not None:raise RuntimeError('frp exited; inspect '+str(self.log_file))
            try:
                with socket.create_connection(('127.0.0.1',_port(port)),timeout=.2):return self
            except OSError:time.sleep(.1)
        raise TimeoutError('frp visitor listener wait expired')

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
            self.process=None
        if self.log is not None:self.log.close();self.log=None

    def __enter__(self):return self.start()
    def __exit__(self,*args):self.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    install=commands.add_parser('install');install.add_argument('--destination',required=True);install.add_argument('--target',choices=FRP_SHA256)
    commands.add_parser('nginx-location')
    run=commands.add_parser('run');run.add_argument('--executable',required=True);run.add_argument('--config',required=True);run.add_argument('--log',required=True)
    args=parser.parse_args()
    if args.command=='install':print(json.dumps(install_frp(args.destination,target=args.target)))
    elif args.command=='nginx-location':print(NGINX_LOCATION,end='')
    else:
        try:
            with FrpProcess(args.executable,args.config,args.log) as process:
                raise SystemExit(process.process.wait())
        except KeyboardInterrupt:pass


if __name__=='__main__':main()
