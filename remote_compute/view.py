"""Cell-aligned terminal tables; one snapshot in, bounded visible rows out."""

import ipaddress
import logging
import math
import re
import shutil
import time
import unicodedata
from functools import lru_cache
from urllib.parse import urlsplit
from . import ABBREVIATION, __version__
from .policy import canonical_capabilities
from .runtimes import execution_capabilities
from .task_tags import compact_tag, quantity
from .telemetry import sampling_notice


_ambiguous_width = None


def set_width_probe(probe):
    global _ambiguous_width
    _ambiguous_width = probe
    _char_width.cache_clear()


@lru_cache(maxsize=4096)
def _char_width(char):
    if ord(char) < 32 or unicodedata.combining(char):
        return 0
    kind = unicodedata.east_asian_width(char)
    if kind in 'WF':
        return 2
    if kind == 'A' and _ambiguous_width is not None:
        return _ambiguous_width(char)
    return 1


def _width(text):
    return sum(_char_width(c) for c in str(text))


def _clip(text, width):
    result, used = [], 0
    for char in str(text).replace('\n',' ').replace('\r',''):
        size = _width(char)
        if used + size > width:
            break
        if ord(char) >= 32:
            result.append(char)
            used += size
    return ''.join(result)


def _short(text, width):
    text = _clip(text,_width(str(text).replace('\n',' ')))
    if _width(text) <= width:
        return text
    if width < 7:
        return _clip(text,width)
    left = (width-3)*2//3
    tail = _clip(text[::-1],width-3-left)[::-1]
    return _clip(text,left)+'...'+tail


def _progress_label(job):
    """Format an explicit task ratio without estimating from wall time."""
    progress = job.get('progress')
    if isinstance(progress,dict) and progress.get('schema')=='dco.progress.v1':
        done,total=progress.get('completed'),progress.get('total')
        if progress.get('source')=='executor' and total==1:return '—'
        if isinstance(done,(int,float)) and isinstance(total,(int,float)) and total>0:
            return f'{done/total*100:.0f}%'
        if isinstance(done,(int,float)):return quantity(done,progress.get('unit')or'done')
        return progress.get('phase')or '—'
    if isinstance(progress, dict):
        for key in ('ratio', 'fraction', 'percent'):
            value = progress.get(key)
            if isinstance(value, (int, float)):
                ratio = float(value) / (100 if key == 'percent' or value > 1 else 1)
                return f'{max(0, min(1, ratio)) * 100:.0f}%'
        job = {**job, **progress}
    for done_key, total_key in (('completed', 'total'), ('current', 'target'),
                                ('nfe', 'budget')):
        done, total = job.get(done_key), job.get(total_key)
        if isinstance(done, (int, float)) and isinstance(total, (int, float)) and total > 0:
            return f'{max(0, min(total, done)):g}/{total:g}'
    total = job.get('total_steps')
    if isinstance(total, int) and total > 0:
        steps = job.get('steps') or ()
        done = sum(1 for step in steps
                   if isinstance(step, dict)
                   and (step.get('finished') is not None
                        or step.get('returncode') is not None))
        return f'{min(done, total)}/{total}'
    value = job.get('progress')
    if isinstance(value, (int, float)):
        ratio = float(value) / (100 if value > 1 else 1)
        return f'{max(0, min(1, ratio)) * 100:.0f}%'
    return '—'


def _cell(text, width, right=False):
    text = _short(text,width)
    padding = ' '*max(0,width-_width(text))
    return padding+text if right else text+padding


def wrap_lines(text, width):
    remaining=''.join(c for c in str(text) if ord(c)>=32)
    while remaining:
        line=_clip(remaining,max(2,width))
        yield line
        remaining=remaining[len(line):]


def _rule(title, width):
    text = _clip(' '+title+' ' if title else '',max(0,width-2))
    return '+'+text+'-'*max(0,width-2-_width(text))+'+'


def _line(text, width):
    return '| '+_cell(text,max(0,width-4))+' |'


def _panel(title, rows, width, *, framed=False):
    return [_rule(title,width), *(rows if framed else [_line(row,width) for row in rows]), _rule('',width)]


def _table(headers, records, widths, width, right=()):
    widths = list(widths)
    flexible = widths.index(None)
    widths[flexible] = max(1,width-4-3*(len(widths)-1)-sum(n or 0 for n in widths))
    def row(values):
        cells=[]
        for index,(value,size) in enumerate(zip(values,widths)):
            if headers[index]=='任务标签':value=compact_tag({'label':value,'id':values[0]if values is not headers else''},size,measure=_width)
            cells.append(_cell(value,size,index in right))
        return ' | '.join(cells)
    divider = '+' + '+'.join('-'*(size+2) for size in widths) + '+'
    return [_line(row(headers),width), divider, *[_line(row(record),width) for record in records]]


def _gb(value):
    return f'{(value or 0)/1024:.1f}'


def _task_memory(job):
    """Current RSS / original phase estimate at this CPU grant, in GiB."""
    phase=job.get('memory_phase')or job.get('memory_phase_model')or job.get('memory_model')or{}
    declared=phase.get('memory_mb')
    if declared is None and phase.get('base_memory_mb')is not None:
        declared=phase['base_memory_mb']+phase['memory_per_cpu_mb']*max(1,job.get('cpus')or 1)
    if declared is None:declared=job.get('memory_mb')
    def number(value):return '-'if value is None else f'{max(.1,value/1024):.1f}'
    return number(job.get('rss_mb'))+'/'+number(declared)


def _capabilities(tags):
    labels={'ng47':'ng47','hsp':'HSP','chisel':'Chisel','ml':'ML','cuda':'CUDA','npu':'NPU','layout':'Layout','rtl':'RTL','onnx':'ONNX'}
    return ' '.join(labels.get(name,name) for name in execution_capabilities(tags or ())) or '-'



def _loopback_route(node, center_host):
    endpoint=urlsplit(node.get('url') or '')
    if endpoint.scheme not in ('http','https') or endpoint.hostname not in ('127.0.0.1','localhost','::1'):
        return None
    worker_host=node.get('node')
    if not worker_host:
        return '回环'
    return '本机' if worker_host.casefold()==center_host.casefold() else '本机中转'


def _device_address(node, url='', *, center_host=None):
    address=node.get('url') or url
    if address.startswith('local://'):
        return '本机'
    if node.get('host_ip'):
        return node['host_ip']
    if center_host is not None:
        route=_loopback_route(node,center_host)
        if route is not None:
            return route
    host=urlsplit(address).hostname if address else None
    if host in ('127.0.0.1','localhost','::1'):
        return '本机'
    return node.get('host_ip') or (host if host and host not in ('0.0.0.0','::') else '等待连接')


def _node_addresses(node, url='', *, center_host=None):
    """Render the worker interface separately from the configured visitor."""
    endpoint = urlsplit(node.get('url') or url)
    relay = urlsplit(node.get('relay_url') or '').netloc
    loopback = _loopback_route(node,center_host) if center_host is not None else None
    if loopback in ('本机中转','回环'):
        relay = endpoint.netloc
    relay = relay.rsplit('@',1)[-1] or '-'
    candidates = (node.get('node_ip'),node.get('host_ip'),
                  endpoint.hostname if endpoint.scheme in ('http','https') and not loopback else None)
    for candidate in candidates:
        if not candidate:
            continue
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if not (address.is_loopback or address.is_unspecified):
            return str(address),relay
    return '-',relay


def _accelerator_summary(node):
    """Compact capacity/availability text for the operator dashboard."""
    profiles = node.get('accelerators') or []
    if not profiles:
        return '-'
    used = int(node.get('accelerator_reserved', node.get('accelerator_used', 0)) or 0)
    parts=[]
    for profile in profiles:
        kind=str(profile.get('kind','accel')).upper()
        count=int(profile.get('count',0) or 0)
        memory=int(profile.get('memory_mb',0) or 0)
        memory_text=f'{memory/1024:.0f}G' if memory and memory%1024==0 else f'{memory}M'
        parts.append(f'{kind} {count}x{memory_text}')
    return f'{used}/{sum(int(p.get("count",0) or 0) for p in profiles)} ' + ','.join(parts)


def _network_text(network, loopback=None):
    network=network or {}
    if network.get('local'):
        return loopback or '本机'
    rtt=network.get('rtt_ms')
    loss=network.get('packet_loss_pct')
    if rtt is None and loss is None:
        return '--'
    return f"{rtt:.1f}ms/{loss:.0f}%" if isinstance(rtt,(int,float)) and isinstance(loss,(int,float)) else '--'


def _show_node(node):
    return not node.get('detached',False) and (node.get('enabled', True) or node.get('reserved', 0)
            or node.get('external_cpus', 0) or node.get('active_jobs'))


def _node_state(node):
    if node.get('detached'):
        return '失联'
    if not node.get('enabled', True):
        return '禁用'
    if node.get('error'):
        return '失联'
    if node.get('paused'):
        return '暂停'
    return '运行' if node.get('active_jobs') else '就绪'


class StatusRows(list):
    def __init__(self, value, url='', width=110, *, viewport=False):
        self.value,self.url,self.width = value,url,max(20,width)
        self.viewport=viewport
        self.task_page,self.page_count = 0,1
        self.device_page,self.device_page_count = 0,1
        self.detail = False
        self._build()
        super().__init__([] if viewport else self._render(None))

    def resize(self, width):
        if self.width != max(20,width):
            self.width=max(20,width)
            self._build()
            if not self.viewport:
                self[:]=self._render(None)

    def _build(self):
        value,width=self.value,self.width
        center=value.get('role')=='scheduler'
        port = urlsplit(self.url).port if self.url else None
        port_label = f' | 端口 {port}' if port else ''
        version_label = ' | ' + str(value['version']) if value.get('version') else ''
        role='混合模式' if center and value.get('mode')=='hybrid' else '调度中心' if center else '算力提供者'
        self.title=f"{ABBREVIATION} | {value.get('node','')} | {role}{port_label}{version_label}"
        if self.viewport:
            release = value.get('version') or __version__
            self.title = f"{ABBREVIATION} {release} | {value.get('node', '')} | {role}{port_label}"
            sample_time=value.get('health_sampled_at') or time.time()
            self.title+=' | 采样 '+time.strftime('%H:%M:%S',time.localtime(sample_time))
            if value.get('health_stale'):
                self.title+=f"（{value['health_age_seconds']:.1f} 秒前）"
        self.summary=f"线程 {value.get('used_cpus',0)}/{value.get('cpus',0)} | 排队 {value.get('queued',0)} | RAM预留 {_gb(value.get('memory_reserved_mb'))} GiB"
        if center:
            self.summary+=' | '+str(value.get('policy','adaptive'))
            if not value.get('accepting',True):self.summary+=' | 停止新增，收齐在途'
        nodes=value.get('nodes',[]) if center else [{**value,'name':value.get('node'),'capacity':value.get('cpus')}]
        self.devices,self.jobs=[],[]
        center_host=(value.get('node') or '') if center else None
        own_url='' if center else self.url
        for number,node in enumerate(nodes,1):
            if not self.viewport and not _show_node(node):
                continue
            name=node.get('display_name') or node.get('name') or node.get('node') or '-'
            allocated=(node.get('reserved',0)+node.get('external_cpus',0) if 'reserved' in node else node.get('used_cpus',0))
            budget=node.get('base_capacity',node.get('base_cpus',node.get('capacity',0)))
            threads=f"{allocated}/{budget}({node.get('logical_cpus','?')})"
            total=node.get('memory_total_mb')
            available=node.get('host_memory_available_mb')
            memory=f'{_gb(total-available)}/{_gb(total)}' if total and available is not None else '-'
            cpu=node.get('cpu_percent')
            cpu_percent=f'{cpu:.1f}%' if cpu is not None else '-'
            peers=list(dict.fromkeys(item['ip'] for item in node.get('peers') or []))
            external=[ip for ip in peers if ip not in ('local','127.0.0.1','::1')]
            loopback='本机'
            peers=', '.join(external or [loopback if ip in ('local','127.0.0.1','::1') else ip for ip in peers]) or '-'
            benchmark=node.get('benchmark') or {}
            measured=benchmark.get('score');single=(benchmark.get('single') or {}).get('score')
            benchmark_state=node.get('benchmark_state','ready')
            if benchmark_state in {'queued','running'}:
                score='校准中'
            elif benchmark.get('name')!='native-mixed-v4' and measured is not None:
                score='-/待校准'
            elif measured is not None:
                score=(f'{single:.3g}/' if single is not None else '-/')+f'{measured:.3g}'
            else:
                score='-/待依赖' if 'Install NumPy' in benchmark.get('reason','') else '-/待校准'
            node_ip,relay_entry = _node_addresses(node,own_url,center_host=center_host) if self.viewport else (
                _device_address(node,own_url,center_host=center_host),'-')
            self.devices.append((node,[name,threads,cpu_percent,memory,
                                       _gb(node.get('memory_available_mb')),
                                       _gb(node.get('rss_mb'))+'/'+_gb(node.get('memory_reserved_mb')),peers,score,
                                       benchmark.get('threads','-'),node_ip,
                                       number,_accelerator_summary(node),
                                       node.get('benchmark_state','ready'),_capabilities(node.get('tags')),relay_entry]))
            self.jobs.extend((name,job) for job in node.get('active_jobs') or [])
        self.projects=[]
        if center:
            for name,a in value.get('projects',{}).items():
                count=sum(job.get('project')==name for _,job in self.jobs)
                self.projects.append([name or '-',str(float(a['resource_share'])),f"{count}/{a['used']}",a['queued']])
            self.jobs.extend(('-',job) for job in value.get('queued_jobs',[]))
        self.jobs.sort(key=lambda item:(item[1].get('started') or math.inf,item[1].get('id','')))
        self.node_width=max([8]+[_width(r[0]) for _,r in self.devices if len(r[0])<=8])
        self.available_capabilities = _capabilities(tag for node, _ in self.devices
            if _node_state(node) in ('运行','就绪') for tag in node.get('tags') or ())
        now=time.time()
        offline_nodes={n.get('display_name')or n.get('name')for n in value.get('nodes',[])if n.get('error')}
        self.records=[]
        for name,job in self.jobs:
            state=job.get('status','running')
            from .progress import diagnose, LABELS
            diagnostic=diagnose(job,now=now,offline=name in offline_nodes)
            state_label=LABELS.get(diagnostic['code'],{'running':'运行','dispatching':'派发','queued':'排队'}.get(state,state))
            started=job.get('started')
            label = str(job.get('label') or '-')
            if job.get('observe_only'):
                details = []
                if job.get('phase'):
                    details.append(job['phase'])
                if job.get('measured_cpus') is not None:
                    details.append(f"实耗 {job['measured_cpus']:.2f} 核")
                if details:
                    label = ' · '.join([*details, label])
            self.records.append([job.get('id','-')[:4],name,job.get('project') or '-',f"{job.get('cpus',0)}/{job.get('max_cpus')or '?'}",
                                 _task_memory(job),state_label,label,
                                 str(max(0,int(now-started))) if started is not None else '-',
                                 f"{float(job.get('resource_share',value.get('projects',{}).get(job.get('project'),{}).get('resource_share',1.0)))}/{job.get('priority',0)}",
                                 _progress_label(job)])

    @property
    def device_labels(self):
        labels={self.value.get('node') or ''}
        width=max([11]+[_width(f'#{r[10]} ')+self.node_width for _,r in self.devices])
        for _,record in self.devices:
            name=record[0]
            labels.update((name,_short(name,self.node_width)))
            prefix=f'#{record[10]} '
            rendered=_short(prefix+name,width)
            if rendered.startswith(prefix):labels.add(rendered[len(prefix):])
        return labels-{'','-'}

    def _resources(self, width):
        records=[r for _,r in self.devices]
        cpu_width=max([10]+[_width(r[1])for r in records])
        if width>=100:
            table=_table(['节点','线程占/配','CPU%','单核/整机','内存用/总','可派','加速器','进程/预留'],
                         [[f'#{r[10]} {r[0]}',*r[1:3],r[7],*r[3:5],r[11],r[5]] for r in records],
                         [max([11]+[_width(f'#{r[10]} ')+self.node_width for r in records]),cpu_width,6,11,10,5,13,None],width,right=(1,2,3,4,5,6,7))
        elif width>=54:
            table=_table(['节点','线程占/配','CPU%','内存用/总','可派'],[r[:5] for r in records],
                         [self.node_width,cpu_width,6,None,5],width,right=(1,2,3,4))
        else:
            table=[_line(f'{r[0]}  {r[1]}  CPU {r[2]}',width) for r in records]
        for node,r in self.devices:
            if node.get('error'):
                table.append(_line(r[0]+': '+node['error'],width))
            elif node.get('paused') or not node.get('enabled',True):
                table.append(_line(r[0]+': 暂停分配',width))
        sections=[('设备资源 | 内存 GiB',table,0)]
        if width>=100:
            center_host=(self.value.get('node') or '') if self.value.get('role')=='scheduler' else None
            details=_table(['节点','单核/整机','进程/预留','IP','链路'],
                           [[r[0],r[7],r[5],r[9],
                             _network_text(node.get('network'),_loopback_route(node,center_host)
                                           if center_host is not None else None)] for node,r in self.devices],
                           [self.node_width,11,9,15,None],width,right=(1,2,3,4))
        elif width>=54:
            details=_table(['节点','单核/整机','进程/预留','IP'],
                           [[r[0],r[7],r[5],r[9]] for _,r in self.devices],
                           [self.node_width,11,9,None],width,right=(1,2,3))
        else:
            details=[_line(f'{r[0]} {r[7]} Miter/s',width) for r in records]
            details.extend(_line(f'#{r[10]} {r[0]} GPU {r[11]}',width) for r in records)
            for r in records:
                details.append(_line(f'{r[0]} RSS/Res {r[5]} GiB',width))
                details.append(_line(f'{r[0]} IP {r[9]}',width))
        sections.append(('启动基准 Miter/s | 进程/预留 GiB',details,1))
        if self.value.get('role')=='scheduler':
            if width>=42:
                table=_table(['工程','份额','任务/线程','排队'],self.projects,
                             [None,5,9,4],width,right=(1,2,3))
            else:
                table=[_line(' '.join(map(str,r)),width) for r in self.projects]
            sections.append(('工程份额',table if self.projects else [_line('等待工程任务',width)],0))
        capabilities=[]
        for node,r in self.devices:
            prefix=r[0]+': ';line=prefix
            for word in _capabilities(node.get('tags')).split():
                if _width(line+' '+word)>width-4 and line!=prefix:
                    capabilities.append(_line(line,width));line='  '+word
                else:line+=('' if line==prefix else ' ')+word
            capabilities.append(_line(line,width))
        sections.append(('运行能力',capabilities,2))
        accelerators=[_line(f'#{r[10]} {r[0]}: {r[11]}',width) for _,r in self.devices]
        if any(r[11] != '-' for _,r in self.devices):
            sections.append(('加速器可用性',accelerators,2))
        return sections

    def _device_slice(self, size):
        self.device_page_count=max(1,math.ceil(len(self.devices)/size))
        self.device_page%=self.device_page_count
        start=self.device_page*size
        return self.devices[start:start+size]

    def _fleet(self, limit):
        """Keep the same information in the table and the narrow viewport."""
        width = self.width
        headers = ['节点','状态','线程','CPU%','内存用/总 GiB','节点 IP','中转入口','能力']
        records = []
        for node, record in self.devices:
            state = _node_state(node)
            sampled = state not in ('禁用','失联')
            records.append([f'#{record[10]} {record[0]}',state,record[1].split('(')[0],
                            record[2] if sampled else '-',record[3] if sampled else '-',
                            record[9],record[14],record[13] if sampled else '-'])
        widths = [max(_width(header),*(_width(item) for item in column))
                  for header,column in zip(headers,zip(*records))] if records else []
        pages = []
        if records and limit >= 5 and width >= sum(widths)+4+3*(len(headers)-1):
            size = limit-4
            for start in range(0,len(records),size):
                pages.append(_table(headers,records[start:start+size],[*widths[:-1],None],width,right=(2,3,4)))
        else:
            size = max(1,limit-2)
            for name,state,threads,cpu,memory,address,relay_entry,capabilities in records:
                identity = f'{name}  {state}  线程 {threads}'
                fields = [identity,f'CPU% {cpu}  内存用/总 GiB {memory}',
                          f'节点 IP {address}  中转入口 {relay_entry}',f'能力 {capabilities}']
                lines = [_line(line,width) for field in fields for line in wrap_lines(field,width-4)]
                if len(lines) > size and size > 1:
                    heading = _line(name,width)
                    pages.extend([heading,*lines[start:start+size-1]] for start in range(0,len(lines),size-1))
                elif len(lines) > size:
                    pages.extend([line] for line in lines)
                elif pages and len(pages[-1])+len(lines) <= size:
                    pages[-1].extend(lines)
                else:
                    pages.append(lines)
        self.device_page_count = max(1,len(pages))
        self.device_page %= self.device_page_count
        present = [record[1] for record in records]
        title = f'设备资源 {len(present)} 台 {self.device_page+1}/{self.device_page_count}页'
        if width >= 54:
            title += f" | 失联 {present.count('失联')} | 暂停 {present.count('暂停')} | 禁用 {present.count('禁用')}"
        lines = pages[self.device_page] if pages else [_line('没有节点',width)]
        return [_rule(title, width), *lines, _rule('', width)][:limit]

    def _overview(self, limit):
        width = self.width
        metadata = ([_line(line,width) for line in wrap_lines('可调度能力  '+self.available_capabilities,width-4)]
                    if limit >= 10 else [])
        if self.projects and limit >= 15:
            summary = '  '.join(f'{name} {running} 排{queued}'
                                for name, _, running, queued in self.projects)
            metadata.append(_line('工程  '+summary,width))
        remaining = limit - len(metadata)
        # At ordinary window sizes, spend only as many rows as the fleet needs.
        # In a short terminal both panes keep at least one data row.
        fleet_height = max(0,remaining-min(7,max(4,remaining//2)))
        devices = self._fleet(fleet_height) if fleet_height else []
        task_height = max(0, remaining - len(devices))
        tasks = self._tasks(width, max(1, task_height - 4))[:task_height]
        return [*devices, *metadata, *tasks]

    def _paged_resources(self, width, limit, *, bottom):
        footer=int(bottom)
        show_table=width>=34 and limit>=5
        size=max(1,limit-1-footer-2*show_table)
        page=self._device_slice(size)
        title=f'设备资源 {len(self.devices)} 台 {self.device_page+1}/{self.device_page_count}页'
        if show_table and width>=54:
            records=[[f'#{r[10]} {r[0]}',r[1],r[2],r[3],r[4]] for _,r in page]
            table=_table(['节点','线程占/配','CPU%','内存用/总','可派'],
                         records,[None,10,5,10,4],width,right=(1,2,3,4))
        elif show_table:
            table=_table(['节点','线程占/配','CPU%'],
                         [[f'#{r[10]} {r[0]}',r[1],r[2]] for _,r in page],
                         [None,10,5],width,right=(1,2))
        else:
            table=[_line(f'#{r[10]} {r[0]} {r[1]} {r[2]}',width) for _,r in page]
        rows=[_rule(title,width),*table]
        if bottom:
            rows.append(_rule('',width))
        return rows[:limit]

    def _resource_rows(self, sections, width, limit=None, *, bottom=False):
        selected=[section for section in sections if section[2]==0]
        required=sum(1+len(s[1]) for s in selected)+int(bottom)
        if limit is not None and required>limit and limit>=len(self.devices)+3+int(bottom):
            rows=[_rule('设备资源',width)]
            rows.extend(_line(f'{r[0]} {r[1]} CPU {r[2]} 内存 {r[3]}',width) for _,r in self.devices)
            rows.extend([_rule('工程份额',width),_line(' | '.join(f'{r[0]} {r[2]} Q{r[3]}'
                         for r in self.projects) or '等待工程任务',width)])
            if bottom:
                rows.append(_rule('',width))
            self.device_page,self.device_page_count=0,1
            return rows
        if limit is not None and required>limit and len(self.devices)>1:
            return self._paged_resources(width,limit,bottom=bottom)
        self.device_page,self.device_page_count=0,1
        for section in sorted(sections,key=lambda item:item[2]):
            if section[2] and (limit is None or sum(1+len(s[1]) for s in selected)+1+len(section[1])+int(bottom)<=limit):
                selected.append(section)
        result=[]
        for title,rows,_ in sections:
            if any(title==section[0] for section in selected):
                result.extend([_rule(title,width),*rows])
        if limit is not None:
            result=result[:max(0,limit-int(bottom))]
        if bottom and (limit is None or limit>0):
            result.append(_rule('',width))
        return result

    def _details(self, width, limit):
        lines=[]
        for node,r in self.devices:
            details=self._node_details(node,r)
            lines.extend(line for item in details for line in wrap_lines(item,width-4))
            lines.append('')
        size=max(1,len(lines) if limit is None else limit-2)
        self.device_page_count=max(1,math.ceil(len(lines)/size))
        self.device_page%=self.device_page_count
        start=self.device_page*size
        return _panel(f'节点详情 {self.device_page+1}/{self.device_page_count}页',
                      lines[start:start+size] or ['没有节点'],width)

    def _node_details(self, node, record):
        center = (self.value.get('node') or '') if self.value.get('role') == 'scheduler' else None
        route = _network_text(node.get('network'), _loopback_route(node, center) if center is not None else None)
        key = node.get('name') or node.get('node') or record[0]
        name = f'#{record[10]} {record[0]}' + (f' · {key}' if key != record[0] else '')
        details = [
            name,
            f'状态 {_node_state(node)} | 任务 {len(node.get("active_jobs") or ())}',
            f'线程 已占/配额(逻辑) {record[1]} | CPU {record[2]} | 基准 单核/整机 {record[7]} Miter/s',
            f'内存 已用/总 {record[3]} | 可派 {record[4]} | 进程/预留 {record[5]} GiB',
            (f'节点 IP {record[9]} | 中转入口 {record[14]} | 链路 {route}' if self.viewport
             else f'IP {record[9]} | 链路 {route}'),
            f'来源 {record[6]} | 加速器 {record[11]}',
            '能力 ' + record[13],
        ]
        if node.get('error'):
            details.insert(2, '连接 ' + str(node['error']))
        elif node.get('paused') or not node.get('enabled',True):
            details.insert(2, '暂停分配')
        return details

    def _node_view(self, limit):
        width = self.width
        if not self.devices:
            self.device_page, self.device_page_count = 0, 1
            return _panel('节点详情', ['没有节点'], width)[:limit]
        size = max(1, limit-2)
        pages = []
        for index, (node, record) in enumerate(self.devices, 1):
            details = self._node_details(node, record)
            lines = [line for item in details for line in wrap_lines(item, width-4)]
            chunks = [lines[start:start+size] for start in range(0, len(lines), size)]
            for part, chunk in enumerate(chunks, 1):
                title = f'节点详情 {index}/{len(self.devices)} {record[0]}'
                if len(chunks) > 1:
                    title += f' | {part}/{len(chunks)}页'
                pages.append((title, chunk))
        self.device_page_count = len(pages)
        self.device_page %= self.device_page_count
        title, lines = pages[self.device_page]
        return _panel(title, lines, width)[:limit]

    def _tasks(self, width, size=None):
        size=max(1,size or len(self.records))
        self.page_count=max(1,math.ceil(len(self.records)/size))
        self.task_page%=self.page_count
        records=self.records[self.task_page*size:(self.task_page+1)*size]
        records=[r if len(r)>7 else [*r,'-'] for r in records]
        ram_width=max([9]+[_width(r[4])for r in records])
        state_width=max([4]+[_width(r[5])for r in records])
        def tag(record):
            return record[6] if record[9] in ('—','0/1','1/1') else f'{record[9]} {record[6]}'
        # The full progress column appears only when the task panel has room
        # for it.  Narrow panes retain every existing identity/resource field;
        # the same ratio remains available in the status/API payload.
        show_progress = width >= 140
        if show_progress:
            table=_table(['ID','节点','工程','CPU/上限','内存实/报','秒','状态','进度','任务标签'],
                         [[r[i] for i in (0,1,2,3,4,7,5,9,6)] for r in records],
                         [4,self.node_width,6,9,ram_width,6,state_width,8,None],width,right=(3,4,5,6))
        elif width>=100:
            table=_table(['ID','节点','工程','CPU/上限','内存实/报','秒','状态','任务标签'],
                         [[r[i] for i in (0,1,2,3,4,7,5)]+[tag(r)] for r in records],
                         [4,self.node_width,6,9,ram_width,6,state_width,None],width,right=(3,4,5))
        elif width>=80:
            table=_table(['ID','节点','CPU/上限','内存实/报','秒','状态','任务标签'],
                         [[r[i]for i in (0,1,3,4,7,5)]+[tag(r)]for r in records],
                         [4,self.node_width,9,ram_width,5,state_width,None],width,right=(2,3,4))
        elif width>=64:
            table=_table(['ID','节点','CPU/上限','秒','任务标签'],[[r[i] for i in (0,1,3,7)]+[tag(r)] for r in records],
                         [4,self.node_width,9,5,None],width,right=(2,3))
        elif width>=54:
            table=_table(['ID','节点','CPU/上限','任务标签'],
                         [[r[i] for i in (0,1,3)]+[tag(r)] for r in records],
                         [4,self.node_width,9,None],width,right=(2,))
        elif width>=38:
            table=_table(['ID','CPU/上限','秒','任务标签'],[[r[i] for i in (0,3,7)]+[tag(r)] for r in records],
                         [4,8,4,None],width,right=(1,2))
        else:
            table=[_line('ID CPU 任务',width)]
            for r in records:
                prefix=f'{r[0]} {r[3]} '
                fitted=compact_tag({'label':tag(r),'id':r[0]},max(1,width-4-_width(prefix)),measure=_width)
                table.append(_line(prefix+fitted,width))
        title=f'当前任务 {len(self.records)} 个 | {self.task_page+1}/{self.page_count}页'
        if not self.records:
            table.append(_line('空闲',width))
        return _panel(title,table,width,framed=True)

    def _render(self, limit):
        width=self.width
        result=[_rule(self.title,width),_line(self.summary,width)]
        if self.viewport:
            available = max(0, limit-2)
            content = self._node_view(available) if self.detail else self._overview(available)
            return [*result, *content][:limit]
        if self.detail:
            result.extend(self._details(width,None if limit is None else max(0,limit-2)))
            return result[:limit] if limit is not None else result
        if width>=120:
            left_width=56
            right_width=width-left_width+1
            sections=self._resources(left_width)
            count=(max(len(self._resource_rows(sections,left_width,bottom=True)),len(self.records)+4)
                   if limit is None else max(0,limit-2))
            left=self._resource_rows(sections,left_width,count,bottom=True)
            right=self._tasks(right_width,max(1,count-4))
            for panel,panel_width in ((left,left_width),(right,right_width)):
                panel[-1:-1]=[_line('',panel_width)]*max(0,count-len(panel))
            for lrow,rrow in zip(left[:count],right[:count]):
                junction='+' if lrow.endswith('+') or rrow.startswith('+') else '|'
                result.append(lrow[:-1]+junction+rrow[1:])
        else:
            sections=self._resources(width)
            non_limit=None if limit is None else max(1,limit-6-min(2,len(self.records)))
            result.extend(self._resource_rows(sections,width,non_limit))
            result.extend(self._tasks(width,None if limit is None else max(1,limit-len(result)-4)))
        return result[:limit] if limit is not None else result

    def fit(self, count):
        return self._render(max(0,count))


def format_status(value, url='', *, width=None):
    return StatusRows(value,url,width or shutil.get_terminal_size((110,32)).columns-1)


def format_notices(value):
    """Merge forwarded operation notices; the executing worker supplies the peer."""
    notices={}
    sources=[value]+list(value.get('nodes') or [])
    for source in sources:
        if source is not value and not _show_node(source):
            continue
        name=source.get('display_name') or source.get('name') or source.get('node') or '-'
        for event in source.get('events') or []:
            message=event['message']
            if sampling_notice(message):
                continue
            key=message if re.search(r'\[[0-9a-f]{8,32}\]',message) else (event['time'],event['ip'],message)
            notices[key]=dict(event,node=name)
    return [time.strftime('%H:%M:%S',time.localtime(e['time']))+' '+e['node']+' '+e['ip']+' '+
            re.sub(r'\[([0-9a-f]{8,32})\]',lambda m:'['+m[1][:4]+']',e['message'])
            for e in sorted(notices.values(),key=lambda e:e['time'])[-6:]]


def _layout(status, events, output, height, *, reserved=2):
    budget=max(0,height-reserved)
    if not budget:
        return []
    sections=int(bool(events))+int(bool(output))
    if isinstance(status,StatusRows):
        if status.viewport:
            panes = []
            if events and (not output or budget >= 16):
                panes.append(('最近通知', events))
            if output:
                panes.append(('命令输出', output))
            count = min(4 if status.detail else 3, max(0, (budget-10)//max(1,len(panes))-2))
            notices, command_output = [], []
            for title, data in panes if count else ():
                rows = _panel(title, data[-count:], status.width)
                if title == '命令输出':
                    command_output = rows
                else:
                    notices = rows
            try:
                head = status.fit(budget - len(notices) - len(command_output))
            except Exception as error:
                # Keep the command prompt and computations alive if a snapshot
                # cannot be drawn. The next health sample gets a fresh renderer.
                message = f'{type(error).__name__}: {error}'
                if _layout.last_error != message:
                    logging.getLogger(__name__).exception('Dashboard render failed')
                _layout.last_error = message
                head = [_line('面板显示异常，输入 status 或 nodes 查看资源',status.width)]
            else:
                _layout.last_error = None
            # Anchor status and recent notifications at the top; keep command
            # feedback beside the editable prompt. Spare rows belong between.
            content = (head + notices)[:budget - len(command_output)]
            return content + [''] * (budget - len(content) - len(command_output)) + command_output
        reserve=max(6,min(25,budget-3*sections))
        count=min(6,max(1,(budget-reserve-2*sections)//sections)) if sections else 0
        def footer_rows(count):
            return ((_panel('最近通知',events[-count:],status.width) if events else []) +
                    (_panel('命令输出',output[-count:],status.width) if output else []))
        footer=footer_rows(count)
        head=status.fit(max(0,budget-len(footer)))
        if sections:
            spare=budget-len(head)-len(footer)
            count=min(6,count+max(0,spare)//sections)
            footer=footer_rows(count)
            head=status.fit(max(0,budget-len(footer)))
        return (head+footer)[-budget:]
    count=min(6,max(1,(budget-3-sections)//sections)) if sections else 0
    footer=(['最近操作（最多6条）:']+events[-count:] if events else [])
    footer+=(['终端输出：']+output[-count:] if output else [])
    remaining=max(0,budget-len(footer));head=status[:remaining]
    if len(status)>remaining and head:
        head[-1]='… status 查看完整资源；help 查看操作'
    return (head+footer)[-budget:]


_layout.last_error = None
