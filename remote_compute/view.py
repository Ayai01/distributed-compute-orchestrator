"""Cell-aligned terminal tables; one snapshot in, bounded visible rows out."""

from collections import OrderedDict
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
        self.sections=[]
        self._build()
        super().__init__(self._render(None))

    def resize(self, width):
        if self.width != max(20,width):
            self.width=max(20,width)
            self.sections=[]
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
        device_records,details=[],[]
        jobs=[]
        for node in nodes:
            name=node.get('node') or node.get('name') or '-'
            state='离线' if node.get('error') else '暂停' if node.get('paused') or not node.get('enabled',True) else '在线'
            allocated=(node.get('reserved',0)+node.get('external_cpus',0) if 'reserved' in node else node.get('used_cpus',0))
            threads=f"{allocated}/{node.get('capacity',0)}({node.get('logical_cpus','?')})"
            if width<100:
                threads=f"{allocated}/{node.get('capacity',0)}"
            total=node.get('memory_total_mb') or 0
            available=node.get('host_memory_available_mb') or 0
            memory=f'{_gb(total-available)}/{_gb(total)}'
            temp=node.get('temperature_c')
            temperature=f'{temp}°C' if temp is not None else '-'
            peers=', '.join('本机' if item['ip']=='local' else item['ip'] for item in node.get('peers') or []) or '-'
            record=[name,state,threads,f"{node.get('cpu_percent') or 0:.1f}%",memory,_gb(node.get('memory_available_mb')),temperature,peers,
                    _gb(node.get('rss_mb'))+'/'+_gb(node.get('memory_reserved_mb'))]
            device_records.append(record)
            location='进程内' if node.get('url','').startswith('local://') else node.get('url',self.url)
            if width>=100:
                details.append((3,f"{name}: {location} | 能力 {_capabilities(node.get('tags'))}"))
            else:
                details.append((3,f"{name}: 设备 {node.get('logical_cpus','?')} 线程 | RSS {_gb(node.get('rss_mb'))} GiB | 预留 {_gb(node.get('memory_reserved_mb'))} GiB"))
                details.append((3,f"{name}: 能力 {_capabilities(node.get('tags'))} | 访问IP {peers}"))
            if node.get('error'):
                details.append((1,name+': '+node['error']))
            jobs.extend((name,job) for job in node.get('active_jobs') or [])
        if width>=100:
            device_table=_table(['设备','状态','线程(总)','CPU','内存GiB','可配','RSS/预留','温度','访问IP'],
                                [[r[i] for i in (0,1,2,3,4,5,8,6,7)] for r in device_records],
                                [10,4,10,6,11,5,8,5,None],width,right=(2,3,4,5,6,7))
        elif width>=80:
            device_table=_table(['设备','线程','CPU','内存 GiB','可分配','温度'],[[r[i] for i in (0,2,3,4,5,6)] for r in device_records],[10,7,6,12,7,None],width,right=(1,2,3,4,5))
        else:
            device_table=['设备 / 线程 / CPU / 内存 GiB']+[f'{r[0]}  {r[2]}  {r[3]}  {r[4]}' for r in device_records]
        self.sections.append(('设备资源',[(0,row) for row in device_table]+details))
        if center:
            records=[[_project(name),f"{a['resource_share']:g}",f"{a['used']}/{a['target']}",a['queued']] for name,a in value.get('projects',{}).items()]
            table=_table(['工程','权重','使用/目标','排队'],records,[None,6,11,6],width,right=(1,2,3)) if width>=50 else ['工程 / 权重 / 使用/目标 / 排队']+['  '.join(map(str,r)) for r in records]
            self.sections.append(('工程份额',[(0,table[0])]+[(1,row) for row in table[1:]] if records else [(1,'等待工程任务')]))
        groups=OrderedDict()
        for name,job in jobs:
            key=(name,_project(job.get('project')),_label(job),job.get('status','running'))
            group=groups.setdefault(key,dict(count=0,cpus=0,memory=0,identity=job.get('id','')[:8]))
            group['count']+=1;group['cpus']+=job.get('cpus',0);group['memory']+=job.get('memory_mb',0)
        records=[]
        for (name,project,label,state),group in groups.items():
            records.append([name,project,group['count'],group['cpus'],_gb(group['memory']),{'running':'运行','dispatching':'派发','queued':'排队'}.get(state,state),label,group['identity'] if group['count']==1 else '-'])
        if width>=100:
            table=_table(['设备','工程','任务','线程','内存GiB','状态','标签','ID'],records,[10,9,4,5,7,4,None,8],width,right=(2,3,4))
        elif width>=64:
            table=_table(['设备','工程','任务','线程','标签'],[[r[i] for i in (0,1,2,3,6)] for r in records],[10,7,4,5,None],width,right=(2,3))
        else:
            table=['设备 / 工程 / 数量 / 标签']+[f'{r[0]} {r[1]} x{r[2]} {r[6]}' for r in records]
        first=set();task_rows=[(0,table[0])]
        for record,row in zip(records,table[1:]):
            task_rows.append((0 if record[0] not in first else 2,row));first.add(record[0])
        self.sections.append((f'当前任务 {len(jobs)} 个 / {len(groups)} 组 · jobs 查看明细',task_rows if records else [(1,'空闲')]))

    def _render(self, limit):
        fixed=3+len(self.sections)
        flat=[(priority,section,index) for section,(_,rows) in enumerate(self.sections) for index,(priority,_) in enumerate(rows)]
        selected={(section,index) for _,section,index in sorted(flat)[:max(0,limit-fixed)]} if limit is not None else None
        result=[_rule(self.title,self.width),_line(self.summary,self.width)]
        for section,(title,rows) in enumerate(self.sections):
            result.append(_rule(title,self.width))
            result.extend(_line(text,self.width) for index,(_,text) in enumerate(rows) if selected is None or (section,index) in selected)
        result.append(_rule('',self.width))
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
