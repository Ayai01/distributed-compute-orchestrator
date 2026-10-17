"""Cell-aligned terminal tables; one snapshot in, bounded visible rows out."""

import math
import shutil
import unicodedata


def _width(text):
    return sum(0 if ord(c) < 32 or unicodedata.combining(c) else
               2 if unicodedata.east_asian_width(c) in 'WF' else 1 for c in str(text))


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


def _panel(title, rows, width):
    return [_rule(title,width), *[_line(row,width) for row in rows], _rule('',width)]


def _table(headers, records, widths, width, right=()):
    widths = list(widths)
    flexible = widths.index(None)
    widths[flexible] = max(1,width-4-3*(len(widths)-1)-sum(n or 0 for n in widths))
    def row(values):
        return ' | '.join(_cell(value,size,index in right) for index,(value,size) in enumerate(zip(values,widths)))
    return [row(headers), *[row(record) for record in records]]


def _gb(value):
    return f'{(value or 0)/1024:.1f}'


def _capabilities(tags):
    result=[]
    for tag in tags or []:
        if tag in {'cpu','win32','linux','darwin'}:
            continue
        name = 'py' if tag=='python' else 'rtl' if tag=='verilator' else tag.removesuffix('_python')
        if name not in result:
            result.append(name)
    return '/'.join(result) or '-'


def _project(name):
    return {'Analog-Opt-Bench':'Bench','ARH-DSE':'ARH'}.get(name,name or '-')


def _label(job):
    parts = job.get('label','').replace('\\','/').split('/')
    project = (job.get('project') or '').casefold()
    positions = [i for i,part in enumerate(parts) if project and part.casefold()==project]
    if positions:
        parts = parts[positions[-1]+1:]
    if project == 'aop' and len(parts)>=2:
        circuit=parts[-2].split('_',1)[0]
        if circuit.upper() in {'BGR','TSA','FSA','LDO','CLASSAB'}:
            return circuit+'/'+parts[-1]
    if parts and parts[0]=='Analog' and len(parts)>=3:
        return '/'.join(parts[1:3])+(' '+parts[3] if len(parts)>3 else '')
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
        self.title=f"Remote Compute | {value.get('node','')} | {'调度中心' if center else '算力提供者'}"
        self.summary=f"线程 {value.get('used_cpus',0)}/{value.get('cpus',0)} | 排队 {value.get('queued',0)} | 预留 {_gb(value.get('memory_reserved_mb'))} GiB"
        if center:
            self.summary+=' | '+str(value.get('policy','adaptive'))
        nodes=value.get('nodes',[]) if center else [{**value,'name':value.get('node'),'capacity':value.get('cpus')}]
        self.devices,self.jobs=[],[]
        for node in nodes:
            name=node.get('node') or node.get('name') or '-'
            allocated=(node.get('reserved',0)+node.get('external_cpus',0) if 'reserved' in node else node.get('used_cpus',0))
            threads=f"{allocated}/{node.get('capacity',0)}({node.get('logical_cpus','?')})"
            total=node.get('memory_total_mb') or 0
            available=node.get('host_memory_available_mb') or 0
            memory=f'{_gb(total-available)}/{_gb(total)}'
            peers=', '.join('本机' if item['ip']=='local' else item['ip'] for item in node.get('peers') or []) or '-'
            benchmark=node.get('benchmark') or {}
            score=f"{benchmark['score']:.1f}" if benchmark.get('score') is not None else '待测'
            self.devices.append((node,[name,threads,f"{node.get('cpu_percent') or 0:.1f}%",memory,
                                       _gb(node.get('memory_available_mb')),
                                       _gb(node.get('rss_mb'))+'/'+_gb(node.get('memory_reserved_mb')),peers,score,
                                       benchmark.get('threads','-')]))
            self.jobs.extend((name,job) for job in node.get('active_jobs') or [])
        self.projects=[]
        if center:
            for name,a in value.get('projects',{}).items():
                count=sum(job.get('project')==name for _,job in self.jobs)
                self.projects.append([_project(name),f"{a['resource_share']:g}",f"{count}/{a['used']}",a['target'],a['queued']])
        self.records=[]
        for name,job in self.jobs:
            state=job.get('status','running')
            self.records.append([job.get('id','-')[:4],name,_project(job.get('project')),f"1/{job.get('cpus',0)}",
                                 _gb(job.get('memory_mb')),{'running':'运行','dispatching':'派发','queued':'排队'}.get(state,state),_label(job)])

    def _resources(self, width, *, compact=False):
        rows,details=[],[]
        if compact:
            for node,r in self.devices:
                baseline=f'{r[7]} kiter/s · {r[8]}线程' if r[8]!='-' else '待测'
                rows.extend([(0,f'{r[0]}  线程 {r[1]}  CPU {r[2]}'),
                             (1,'算力 '+baseline),
                             (0,f'内存 {r[3]} GiB | 可配 {r[4]}'),
                             (2,f'RSS/预留 {r[5]} GiB'),
                             (3,f'访问IP {r[6]}'),
                             (3,f'能力 {_capabilities(node.get("tags"))}')])
        else:
            records=[r for _,r in self.devices]
            if width>=100:
                table=_table(['设备','线程(总)','CPU','kiter/s(T)','内存GiB','可配','RSS/预留','访问IP'],
                             [[*r[:3],f'{r[7]}({r[8]})' if r[8]!='-' else r[7],*r[3:7]] for r in records],
                             [10,10,6,11,11,5,9,None],width,right=(1,2,3,4,5,6))
            elif width>=70:
                table=_table(['设备','线程','CPU','内存GiB','可配'],[r[:5] for r in records],
                             [10,10,6,11,None],width,right=(1,2,3,4))
            else:
                table=['设备 / 线程 / CPU / 内存 GiB']+[f'{r[0]} {r[1]} {r[2]} {r[3]}' for r in records]
            rows.extend((0,row) for row in table)
            for node,r in self.devices:
                details.append((3,f'{r[0]}: RSS/预留 {r[5]} GiB | 访问IP {r[6]} | 能力 {_capabilities(node.get("tags"))}'))
                if width<100:
                    baseline=f'{r[7]} kiter/s · {r[8]}线程' if r[8]!='-' else '待测'
                    details.append((1,f'{r[0]}: 算力 '+baseline))
        for node,r in self.devices:
            if node.get('error'):
                details.append((0,r[0]+': '+node['error']))
            elif node.get('paused') or not node.get('enabled',True):
                details.append((0,r[0]+': 暂停分配'))
        sections=[('设备资源',rows+details)]
        if self.value.get('role')=='scheduler':
            if width>=42:
                table=_table(['工程','权重','任务/线程','目标','排队'],self.projects,[None,4,9,4,4],width,right=(1,2,3,4))
            else:
                table=['工程 / 权重 / 任务/线程 / 目标 / 排队']+[' '.join(map(str,r)) for r in self.projects]
            sections.append(('工程份额',[(0,table[0])]+[(1,row) for row in table[1:]] if self.projects else [(1,'等待工程任务')]))
        return sections

    @staticmethod
    def _resource_rows(sections, width, limit=None, *, bottom=False):
        fixed=len(sections)+int(bottom)
        flat=[(priority,s,i) for s,(_,rows) in enumerate(sections) for i,(priority,_) in enumerate(rows)]
        selected={(s,i) for _,s,i in sorted(flat)[:max(0,limit-fixed)]} if limit is not None else None
        result=[]
        for s,(title,rows) in enumerate(sections):
            result.append(_rule(title,width))
            result.extend(_line(text,width) for i,(_,text) in enumerate(rows) if selected is None or (s,i) in selected)
        if bottom:
            result.append(_rule('',width))
        return result[:limit] if limit is not None else result

    def _tasks(self, width, size=None):
        size=max(1,size or len(self.records))
        self.page_count=max(1,math.ceil(len(self.records)/size))
        self.task_page%=self.page_count
        records=self.records[self.task_page*size:(self.task_page+1)*size]
        if width>=70:
            table=_table(['ID','设备','工程','任务/线程','GiB','状态','标签'],records,
                         [4,7,6,9,4,4,None],width,right=(3,4))
        elif width>=42:
            table=_table(['ID','设备','任务/线程','标签'],[[r[i] for i in (0,1,3,6)] for r in records],
                         [4,7,9,None],width,right=(2,))
        else:
            table=['ID 设备 任务/线程 标签']+[f'{r[0]} {r[1]} {r[3]} {r[6]}' for r in records]
        title=f'当前任务 {len(self.records)} 个 · {self.task_page+1}/{self.page_count}页 · Tab/ShiftTab'
        return _panel(title,table if self.records else ['空闲'],width)

    def _render(self, limit):
        width=self.width
        result=[_rule(self.title,width),_line(self.summary,width)]
        if width>=120:
            left_width=46
            right_width=width-left_width-1
            sections=self._resources(left_width,compact=True)
            count=(max(len(self._resource_rows(sections,left_width,bottom=True)),len(self.records)+3)
                   if limit is None else max(0,limit-2))
            left=self._resource_rows(sections,left_width,count,bottom=True)
            right=self._tasks(right_width,max(1,count-3))
            for i in range(min(count,max(len(left),len(right)))):
                result.append(_cell(left[i] if i<len(left) else '',left_width)+' '+
                              _cell(right[i] if i<len(right) else '',right_width))
        else:
            sections=self._resources(width)
            non_limit=None if limit is None else max(len(sections),limit-5-min(2,len(self.records)))
            result.extend(self._resource_rows(sections,width,non_limit))
            result.extend(self._tasks(width,None if limit is None else max(1,limit-len(result)-3)))
        return result[:limit] if limit is not None else result

    def fit(self, count):
        return self._render(max(0,count))


def format_status(value, url='', *, width=None):
    return StatusRows(value,url,width or shutil.get_terminal_size((110,32)).columns-1)


def _layout(status, events, output, height):
    budget=max(1,height-2)
    sections=int(bool(events))+int(bool(output))
    if isinstance(status,StatusRows):
        reserve=max(6,min(25,budget-3*sections))
        count=min(6,max(1,(budget-reserve-2*sections)//sections)) if sections else 0
        footer=(_panel('最近通知',events[-count:],status.width) if events else [])
        footer+=(_panel('命令输出 · PgUp/PgDn',output[-count:],status.width) if output else [])
        return (status.fit(max(0,budget-len(footer)))+footer)[-budget:]
    count=min(6,max(1,(budget-3-sections)//sections)) if sections else 0
    footer=(['最近操作（最多6条）:']+events[-count:] if events else [])
    footer+=(['终端输出（PgUp/PgDn翻页）:']+output[-count:] if output else [])
    remaining=max(0,budget-len(footer));head=status[:remaining]
    if len(status)>remaining and head:
        head[-1]='… status 查看完整资源；help 查看操作'
    return (head+footer)[-budget:]
