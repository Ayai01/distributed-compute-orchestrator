"""Cell-aligned terminal tables; one snapshot in, bounded visible rows out."""

import math
import re
import shutil
import time
import unicodedata
from functools import lru_cache
from urllib.parse import urlsplit
from . import ABBREVIATION


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
        return ' | '.join(_cell(value,size,index in right) for index,(value,size) in enumerate(zip(values,widths)))
    divider = '+' + '+'.join('-'*(size+2) for size in widths) + '+'
    return [_line(row(headers),width), divider, *[_line(row(record),width) for record in records]]


def _gb(value):
    return f'{(value or 0)/1024:.1f}'


def _capabilities(tags):
    tags=set(tags or [])
    groups=[('ng47' if {'ng47','ngspice47'} & tags else 'ng',{'ng','ng47','ngspice','ngspice47','spice'}),
            ('HSPICE',{'hspice'}),('RTL',{'rtl','verilator','iverilog'}),
            ('Chisel',{'java','scala-cli','firtool'}),('CIRCT',{'circt','circt-translate'}),
            ('C++',{'cxx','gcc','g++','clang','clang++'}),
            ('ONNX',{'onnx','onnx_cpu','onnxruntime'}),
            ('Python',{'python'}|{t for t in tags if '_python' in t}),
            ('ML',{'rgcn_train'}),('GPU',{'gpu','cuda','rocm'})]
    known={'cpu','win32','linux','darwin'}|{t for t in tags if t.startswith('shared:')}
    result=[]
    for name,members in groups:
        known.update(members)
        if (members<=tags if name=='Chisel' else tags & members):result.append(name)
    # The operator view is a compact routing summary.  Keep the protocol tags
    # stable in the worker/API and shorten only what is painted in the table.
    labels={'ngspice47':'ng47','ngspice':'ng','HSPICE':'HS','Python':'Py'}
    result=[labels.get(item,item) for item in result]
    custom=sorted(tags-known)
    result.extend(_short_capability(item) for item in custom)
    return ' '.join(result) or '-'


def _short_capability(value):
    """Keep a custom capability readable without exposing project internals."""
    value=str(value)
    aliases={'aop_hspice_python':'AOP-HS','rgcn_python':'RGCN-Py','rgcn_train':'RGCN-ML',
             'rgcn_python_linux':'RGCN-Linux','onnx_cpu':'ONNX-CPU','shared:rgcn':'shared:RGCN','shared:aop':'shared:AOP'}
    return aliases.get(value, value)


def _project(name):
    return {'Analog-Opt-Bench':'Bench','ARH-DSE':'ARH','rocket-nn-host':'NCNN',
            'yolov8-quant-lab':'YOLO'}.get(name,name or '-')


def _device_address(node, url=''):
    address=node.get('url') or url
    if address.startswith('local://'):
        return '本机'
    if node.get('host_ip'):
        return node['host_ip']
    host=urlsplit(address).hostname if address else None
    if host in ('127.0.0.1','localhost','::1'):
        return '本机'
    return node.get('host_ip') or (host if host and host not in ('0.0.0.0','::') else '等待连接')


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


def _network_text(network):
    network=network or {}
    if network.get('local'):
        return '本机'
    rtt=network.get('rtt_ms')
    loss=network.get('packet_loss_pct')
    if rtt is None and loss is None:
        return '--'
    return f"{rtt:.1f}ms/{loss:.0f}%" if isinstance(rtt,(int,float)) and isinstance(loss,(int,float)) else '--'


def _label(job):
    parts = job.get('label','').replace('\\','/').split('/')
    project = (job.get('project') or '').casefold()
    positions = [i for i,part in enumerate(parts) if project and part.casefold()==project]
    if positions:
        parts = parts[positions[-1]+1:]
    if not parts:
        return '-'
    if project == 'aop' and len(parts)>=2:
        circuit=parts[-2].split('_',1)[0]
        if circuit.upper() in {'BGR','TSA','FSA','LDO','CLASSAB'}:
            return circuit+'/'+parts[-1]
    if project=='rocket-nn-host':
        names={'rocket':'Rocket','boom':'BOOM','shufflenet_v2_x0_5':'ShuffleV2-0.5',
               'mobilenet_v2':'MobileV2','yolov8n':'YOLOv8n','resnet18':'ResNet18'}
        tokens=[]
        for part in re.split(r'[/\s]+',' '.join(parts)):
            if not part or part.casefold() in {'rocket-nn-host','rocket'}:
                continue
            if re.fullmatch(r'[0-9a-f]{12,64}',part,re.IGNORECASE):
                continue
            match=re.fullmatch(r'(.+?)[_-](dog|kitten|cat|car|person)',part,re.IGNORECASE)
            if match and match[1].casefold() in names:
                tokens.extend((names[match[1].casefold()],match[2].lower()))
            else:
                tokens.append(names.get(part.casefold(),part))
        compact=[]
        for token in tokens:
            if token not in compact:
                compact.append(token)
        return ' '.join(compact[-4:]) or '-'
    if project=='rgcn':
        text=re.sub(r'^rgcn-', '', '/'.join(parts[-3:]), flags=re.IGNORECASE)
        for before,after in (('-frozen-model',''),('-fresh',''),('analytic_previous_control','ana-prev'),
                             ('neural_previous_control','nn-prev'),('neural_shared_gate_control','nn-gate'),
                             ('analytic','ana'),('neural','nn'),('-seed','-s')):
            text=text.replace(before,after)
        tokens=text.split('/',1)
        head,sep,tail=tokens[0].partition('-')
        head={'ldo':'LDO','tsa':'TSA','fsa':'FSA','bgr':'BGR','classab':'ClassAB'}.get(head,head)
        tokens[0]=head+(' '+tail if sep else '')
        return '/'.join(tokens)
    if project in ('analog-opt-bench','bench') or parts and parts[0]=='Analog':
        if parts[0]=='Analog':parts=parts[1:]
        text='/'.join(parts[-3:])
        return re.sub(r'\bb(\d{4,})\b',lambda m:'b'+format(int(m[1])/1000,'g')+'k',text)
    if project=='yolov8-quant-lab' and parts[-1]=='integer-range-and-cost':
        return 'INT4 range/cost'
    return '/'.join(parts[-2:]) or job.get('label','-')


class StatusRows(list):
    def __init__(self, value, url='', width=110):
        self.value,self.url,self.width = value,url,max(20,width)
        self.task_page,self.page_count = 0,1
        self._build()
        super().__init__(self._render(None))

    def resize(self, width):
        if self.width != max(20,width):
            self.width=max(20,width)
            self._build()
            self[:]=self._render(None)

    def _build(self):
        value,width=self.value,self.width
        center=value.get('role')=='scheduler'
        self.title=f"{ABBREVIATION} | {value.get('node','')} | {'调度中心' if center else '算力提供者'}"
        self.summary=f"线程 {value.get('used_cpus',0)}/{value.get('cpus',0)} | 排队 {value.get('queued',0)} | RAM预留 {_gb(value.get('memory_reserved_mb'))} GiB"
        if center:
            self.summary+=' | '+str(value.get('policy','adaptive'))
        nodes=value.get('nodes',[]) if center else [{**value,'name':value.get('node'),'capacity':value.get('cpus')}]
        self.devices,self.jobs=[],[]
        for number,node in enumerate(nodes,1):
            name=node.get('node') or node.get('name') or '-'
            allocated=(node.get('reserved',0)+node.get('external_cpus',0) if 'reserved' in node else node.get('used_cpus',0))
            threads=f"{allocated}/{node.get('capacity',0)}({node.get('logical_cpus','?')})"
            total=node.get('memory_total_mb') or 0
            available=node.get('host_memory_available_mb') or 0
            memory=f'{_gb(total-available)}/{_gb(total)}'
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
            self.devices.append((node,[name,threads,f"{node.get('cpu_percent') or 0:.1f}%",memory,
                                       _gb(node.get('memory_available_mb')),
                                       _gb(node.get('rss_mb'))+'/'+_gb(node.get('memory_reserved_mb')),peers,score,
                                       benchmark.get('threads','-'),_device_address(node,'' if center else self.url),
                                       number,_accelerator_summary(node),
                                       node.get('benchmark_state','ready')]))
            self.jobs.extend((name,job) for job in node.get('active_jobs') or [])
        self.projects=[]
        if center:
            for name,a in value.get('projects',{}).items():
                count=sum(job.get('project')==name for _,job in self.jobs)
                self.projects.append([_project(name),f"{a['resource_share']:g}",f"{count}/{a['used']}",a['target'],a['queued']])
        self.jobs.sort(key=lambda item:(item[1].get('started') or math.inf,item[1].get('id','')))
        now=time.time()
        self.records=[]
        for name,job in self.jobs:
            state=job.get('status','running')
            started=job.get('started')
            self.records.append([job.get('id','-')[:4],name,_project(job.get('project')),f"1/{job.get('cpus',0)}",
                                 _gb(job.get('memory_mb')),{'running':'运行','dispatching':'派发','queued':'排队'}.get(state,state),_label(job),
                                 str(max(0,int(now-started))) if started is not None else '-'])

    def _resources(self, width):
        records=[r for _,r in self.devices]
        if width>=100:
            table=_table(['设备','线程(总)','CPU','单/总','RAM','可配','GPU','进程RAM/预留'],
                         [[f'#{r[10]} {r[0]}',*r[1:3],r[7],*r[3:5],r[11],r[5]] for r in records],
                         [11,10,6,11,10,5,13,None],width,right=(1,2,3,4,5,6,7))
        elif width>=54:
            table=_table(['设备','线程(总)','CPU','RAM','可配'],[r[:5] for r in records],
                         [7,10,6,None,5],width,right=(1,2,3,4))
        else:
            table=[_line(f'{r[0]}  {r[1]}  CPU {r[2]}',width) for r in records]
        for node,r in self.devices:
            if node.get('error'):
                table.append(_line(r[0]+': '+node['error'],width))
            elif node.get('paused') or not node.get('enabled',True):
                table.append(_line(r[0]+': 暂停分配',width))
        sections=[('设备资源 | RAM GiB | 算力 Miter/s',table,0)]
        if width>=100:
            details=_table(['设备','单/总','进程RAM/预留','设备IP','网络'],
                           [[r[0],r[7],r[5],r[9],
                             _network_text(node.get('network'))] for node,r in self.devices],
                           [7,11,9,15,None],width,right=(1,2,3,4))
        elif width>=54:
            details=_table(['设备','单/总','进程RAM/预留','设备IP'],
                           [[r[0],r[7],r[5],r[9]] for _,r in self.devices],
                           [7,11,9,None],width,right=(1,2,3))
        else:
            details=[_line(f'{r[0]} {r[7]} Miter/s',width) for r in records]
            details.extend(_line(f'#{r[10]} {r[0]} GPU {r[11]}',width) for r in records)
            details.extend(_line(f'{r[0]} 进程RAM/预留 {r[5]} GiB | IP {r[9]}',width) for r in records)
        sections.append(('启动基准 Miter/s | 进程RAM/预留 GiB',details,1))
        if self.value.get('role')=='scheduler':
            if width>=42:
                table=_table(['工程','权重','任务/线程','目标','排队'],self.projects,
                             [None,4,9,4,4],width,right=(1,2,3,4))
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

    def _resource_rows(self, sections, width, limit=None, *, bottom=False):
        selected=[section for section in sections if section[2]==0]
        for section in sorted(sections,key=lambda item:item[2]):
            if section[2] and (limit is None or sum(1+len(s[1]) for s in selected)+1+len(section[1])+int(bottom)<=limit):
                selected.append(section)
        selected=[s for s in sections if s in selected]
        if limit is not None and sum(1+len(s[1]) for s in selected)+int(bottom)>limit:
            # Short windows keep every device and a compact quota summary.
            result=[_rule('设备资源',width)]
            result.extend(_line(f'{r[0]} {r[1]} CPU {r[2]} RAM {r[3]}',width) for _,r in self.devices)
            if self.value.get('role')=='scheduler':
                result.extend([_rule('工程份额',width),_line(' | '.join(f'{r[0]} {r[2]}/{r[3]}'
                               for r in self.projects) or '等待工程任务',width)])
        else:
            result=[]
            for title,rows,_priority in selected:
                result.extend([_rule(title,width),*rows])
        if limit is not None:
            result=result[:max(0,limit-int(bottom))]
        if bottom and (limit is None or limit>0):
            result.append(_rule('',width))
        return result

    def _tasks(self, width, size=None):
        size=max(1,size or len(self.records))
        self.page_count=max(1,math.ceil(len(self.records)/size))
        self.task_page%=self.page_count
        records=self.records[self.task_page*size:(self.task_page+1)*size]
        records=[r if len(r)>7 else [*r,'-'] for r in records]
        if width>=80:
            table=_table(['ID','设备','工程','任务/线程','RAM','运行秒','状态','标签'],
                         [[r[i] for i in (0,1,2,3,4,7,5,6)] for r in records],
                         [4,7,6,9,4,6,4,None],width,right=(3,4,5))
        elif width>=54:
            table=_table(['ID','设备','任务/线程','运行秒','标签'],[[r[i] for i in (0,1,3,7,6)] for r in records],
                         [4,7,9,6,None],width,right=(2,3))
        elif width>=38:
            table=_table(['ID','任务/线程','运行秒','标签'],[[r[i] for i in (0,3,7,6)] for r in records],
                         [4,9,6,None],width,right=(1,2))
        else:
            table=[_line('ID 任务/线程 运行秒 标签',width)]+[_line(f'{r[0]} {r[3]} {r[7]} {r[6]}',width) for r in records]
        title=f'当前任务 {len(self.records)} 个 | {self.task_page+1}/{self.page_count}页 | Ctrl+Tab 翻页'
        if not self.records:
            table.append(_line('空闲',width))
        return _panel(title,table,width,framed=True)

    def _render(self, limit):
        width=self.width
        result=[_rule(self.title,width),_line(self.summary,width)]
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
            non_limit=None if limit is None else max(len(sections),limit-5-min(2,len(self.records)))
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
        name=source.get('node') or source.get('name') or '-'
        for event in source.get('events') or []:
            message=event['message']
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
        reserve=max(6,min(25,budget-3*sections))
        count=min(6,max(1,(budget-reserve-2*sections)//sections)) if sections else 0
        def footer_rows(count):
            return ((_panel('最近通知',events[-count:],status.width) if events else []) +
                    (_panel('命令输出 | PgUp/PgDn',output[-count:],status.width) if output else []))
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
    footer+=(['终端输出（PgUp/PgDn翻页）:']+output[-count:] if output else [])
    remaining=max(0,budget-len(footer));head=status[:remaining]
    if len(status)>remaining and head:
        head[-1]='… status 查看完整资源；help 查看操作'
    return (head+footer)[-budget:]
