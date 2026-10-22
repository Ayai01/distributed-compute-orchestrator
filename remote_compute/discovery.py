"""One explicit, bounded LAN discovery; no background scanning or wake packets."""

from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
import math
import os
import socket
import subprocess
from urllib.parse import urlsplit

from .auth import in_networks
from .client import Client, RemoteError


def local_networks():
    addresses=[]
    try:
        if os.name=='nt':
            script="Get-NetIPConfiguration | Where-Object {$_.NetAdapter.Status -eq 'Up' -and $_.IPv4DefaultGateway} | ForEach-Object {$_.IPv4Address | Select-Object IPAddress,PrefixLength} | ConvertTo-Json -Compress"
            raw=subprocess.check_output(['powershell.exe','-NoProfile','-Command',script],timeout=10,
                                        creationflags=subprocess.CREATE_NO_WINDOW)
            rows=json.loads(raw.decode('utf-8-sig') or '[]')
            for row in rows if isinstance(rows,list) else [rows]:
                addresses.append(str(row['IPAddress'])+'/'+str(row['PrefixLength']))
        else:
            rows=json.loads(subprocess.check_output(['ip','-j','-4','address','show','up'],timeout=5))
            addresses=[str(a['local'])+'/'+str(a['prefixlen']) for row in rows for a in row.get('addr_info',[])
                       if a.get('family')=='inet']
    except (OSError,ValueError,KeyError,subprocess.SubprocessError):
        pass
    if not addresses:
        addresses=[row[4][0]+'/24' for row in socket.getaddrinfo(socket.gethostname(),None,socket.AF_INET,socket.SOCK_STREAM)]
    return sorted({str(ipaddress.ip_network(a,strict=False)) for a in addresses
                   if in_networks(a.split('/')[0]) and not ipaddress.ip_address(a.split('/')[0]).is_loopback})


def scan(networks=(), *, port=26133, timeout=.7, known=()):
    if not 0<port<65536 or not math.isfinite(timeout) or timeout<=0:
        raise ValueError('scan需要有效端口和正数timeout')
    networks=[ipaddress.ip_network(n,strict=False) for n in (networks or local_networks())]
    if not networks:raise ValueError('未找到局域网IPv4网段；使用scan 192.168.1.0/24指定')
    if any(n.version!=4 or not in_networks(n.network_address) or not in_networks(n.broadcast_address) for n in networks):
        raise ValueError('scan只扫描局域网IPv4网段')
    if sum(n.num_addresses for n in networks)>4096:
        raise ValueError('一次scan最多4096个地址；请指定实际局域网网段')
    hosts=sorted({str(a) for n in networks for a in n.hosts()},key=ipaddress.ip_address)
    saved={}
    for item in known:
        parsed=urlsplit(item.get('url',''))
        if parsed.hostname and parsed.scheme in {'http','https'}:
            saved[(parsed.hostname,parsed.port or (443 if parsed.scheme=='https' else 80))]=item
    def probe(host):
        try:
            with socket.create_connection((host,port),timeout=timeout):pass
            row=saved.get((host,port),{})
            url=row.get('url') or 'http://'+host+':'+str(port)
            value=Client(url,row.get('token',''),ca_file=row.get('ca_file'),timeout=timeout,operator=True).health(brief=True)
            if value.get('role')=='worker' and value.get('node'):
                return {'name':value['node'],'url':url}
        except (OSError,ValueError,RemoteError):
            pass
    with ThreadPoolExecutor(max_workers=min(32,len(hosts))) as executor:
        found=[value for value in executor.map(probe,hosts) if value]
    return {'networks':[str(n) for n in networks],'nodes':found}
