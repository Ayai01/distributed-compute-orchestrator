"""Semantic display tags; full requests and labels remain unchanged."""
import re

MAX_TAG=24
_METHODS={'fiber-mo':'F','fiber_mo':'F','agemoea2':'AGE','age-moea2':'AGE','age2':'AGE',
          'maoea_hap':'HAP','maoea-hap':'HAP','nsga-ii':'N2','nsga2':'N2','nsga-iii':'N3','nsga3':'N3',
          'nsde-r':'NDR','nsder':'NDR','hybrid_v4':'HV4','hybrid_mf':'HMF','readout':'RD','ladder':'LDR'}
_PROGRESS=r'^(\d+/\d+|\d+(?:\.\d+)?%|\d+(?:\.\d+)?[kKmMgG]?(?:cycles|events|records|steps|images|epochs|done|cy|ev|rec|st|img|ep|op))\s+'
_ROLES={'开发':('评估','DEV'),'快评':('快评','DEV'),'L1快评':('L1快评','L1'),
        'L2筛评':('L2筛评','L2'),'L3开发':('L3开发','L3'),
        '独立验证':('曲线','CUR'),'独立曲线':('曲线','CUR'),'独立端点':('端点','END'),
        '完整Clean':('无故障','CLN'),'Clean':('无故障','CLN'),'构建':('构建','BLD'),
        '准备':('准备','PREP'),'模型发布':('模型发布','PUB'),'导出':('导出','OUT')}


def quantity(value,unit=''):
    """Compact explicit work counters while retaining their unit."""
    count=float(value)
    suffix=next((s for scale,s in ((1e9,'G'),(1e6,'M'),(1e3,'k'))if count>=scale),'')
    divisor={'G':1e9,'M':1e6,'k':1e3}.get(suffix,1)
    number=format(count/divisor,'.3g') if suffix else format(count,'g')
    return number+suffix+{'cycles':'cy','events':'ev','records':'rec','steps':'st'}.get(unit,unit)


def _semantic_tag(raw,progress,limit,measure):
    parts=[part.strip()for part in raw.split(' | ')]
    explicit=len(parts)>=3
    if not explicit:parts=[part.strip()for part in raw.split('/')]
    if len(parts)<3 or (not explicit and parts[1]not in _ROLES):return None
    subject=re.sub(r'^[0-9a-f]{6,64}\s+','',parts[0],flags=re.I)
    role,short_role=_ROLES.get(parts[1],(parts[1],parts[1]))
    if explicit:
        scope=parts[2];identity=parts[3]if len(parts)>3 else''
    else:
        identity='#'+parts[2][:4]if re.fullmatch('[0-9a-f]{4,64}',parts[2],re.I)else''
        sample=' '.join(parts[3:]);dose=re.search(r'μ([0-9.eE+-]+)',sample)
        scope='n1'if 'i:'in sample else''
        if dose:scope+=' μ'+quantity(float(dose[1]))
    counter=re.search(r'\bn\d+\b',scope)
    clock=re.search(r'(?:src|~)?\d+(?:\.\d+)?[kMG]?cy\b',scope)
    scopes=list(dict.fromkeys([scope,' '.join(x[0]for x in (counter,clock)if x),
        counter[0]if counter else(clock[0]if clock else'')]))
    acronym=''.join(c for c in subject if c.isupper())
    subjects=[subject]+([acronym]if len(acronym)>=2 else[])
    def line(s,r,w,ident=''):return' '.join(x for x in (s,r,w,progress,ident)if x)
    # Work and real progress have priority over a redundant identity suffix.
    for identity_value in (identity,''):
        value=line(subject,role,scope,identity_value)
        if measure(value)<=limit:return value
    for s in subjects:
        for r in dict.fromkeys((role,short_role)):
            for w in scopes:
                value=line(s,r,w)
                if measure(value)<=limit:return value
    for s in subjects:
        value=' '.join((s,short_role))
        if measure(value)<=limit:return value
    return next((s for s in subjects if measure(s)<=limit),None)


def _word(value,limit):
    value=re.sub(r'[^\w:+-]','',str(value))or'RUN'
    if len(value)<=limit:return value
    return value[:limit]


def compact_tag(job,width=MAX_TAG,preferred=None,measure=len):
    available=max(1,int(width));limit=min(MAX_TAG,available)
    raw=' '.join(str(job.get('label','')).split());text=' '.join(str(preferred if preferred is not None else raw).split())
    progress=re.match(_PROGRESS,text)
    raw_progress=re.match(_PROGRESS,raw)
    clean_raw=raw[raw_progress.end():]if raw_progress else raw
    semantic=_semantic_tag(clean_raw,(progress or raw_progress)[1]if(progress or raw_progress)else'',min(64,available),measure)
    if semantic is not None:return semantic
    text=re.sub(r'(?<!\w)[0-9a-f]{12,64}(?!\w)',lambda match:':'+match[0][-4:],text,flags=re.I)
    if re.fullmatch(r':[0-9a-f]{4}',text,re.I):text='RUN '+text
    if not text:text='RUN'
    clean=text.replace('...','').replace('\u2026','')
    if clean==text and measure(text)<=limit:return text
    progress=re.match(_PROGRESS,clean)
    if progress:clean=clean[progress.end():]
    source=raw+' '+clean;lower=source.lower()
    case=re.search(r'(?:p75_)?(precision|dynamic)(\d+)_(local|relaxed)(\d+)',lower)
    if case:
        family,n,kind,size=case.groups();subject='AB'+family[0].upper()+n+('L'if kind=='local'else'R')
        if(kind,size)not in(('local','10'),('relaxed','50')):subject+=size
    else:
        known=re.search(r'\b(classab|class-ab|ldo|bgr|tsa|fsa|rocket_dma|rocket|boom_dma|boom|fir_stream|fir|mac|conv_pool|rv_dma_yolo)(?=\b|_)',source,re.I)
        names={'classab':'AB','class-ab':'AB','rocket_dma':'Rocket','rocket':'Rocket','boom_dma':'BOOM','boom':'BOOM','fir_stream':'FIR','conv_pool':'Conv','rv_dma_yolo':'YOLO'}
        if known:subject=names.get(known[1].lower(),known[1].upper())
        elif 'class bounded'in lower or 'class tt'in lower:subject='AB'
        else:subject=_word(re.split(r'[/\s]+',clean)[0],7)
    existing=re.search(r'\bs([0-9a-f]{4})(?:b\d+|:[0-9a-f]{4})?\b',source,re.I)
    pair=re.search(r'\b([A-Za-z][\w-]*)\s+(?:seed[=: ]*)?(\d{4,})\b',raw)
    full_seed=re.search(r'\bseed[=: ]+(\d+)\b',source,re.I)
    counter=re.search(r'\b(block|group|batch)[-:= ]+(\d+)\b',source,re.I)
    compact_counter=re.search(r'\b([bg])(\d+)\b',source,re.I)
    if existing:identity='s'+existing[1].lower()
    elif full_seed:identity='s'+format(int(full_seed[1])&0xffff,'04x')
    elif counter:identity=('g'if counter[1].lower()=='group'else'b')+counter[2]
    elif compact_counter:identity=compact_counter[0].lower()
    elif pair:identity='s'+format(int(pair[2])&0xffff,'04x')
    else:
        short=re.search(r'(?<!\w)([0-9a-f]{4})(?!\w)',clean,re.I)
        identity=':'+short[1].lower()if short else':'+str(job['id'])[-4:]if job.get('id')else''
    role=re.search(r'(?<!\w)-(KCL|GEO|ADJ|INV|C[123]|PROXY|GATE)\b',source,re.I)
    if pair and not counter:method=pair[1]
    elif role:method='-'+role[1].upper()
    else:
        method=next((key for key in _METHODS if re.search(r'(?<!\w)'+re.escape(key)+r'(?!\w)',source,re.I)),None)
        if method is None:
            parts=re.split(r'[/\s]+',clean)
            method=next((p for p in parts[1:]if re.fullmatch(r'[A-Za-z-][A-Za-z0-9+-]{0,8}',p)and p.lower()not in('bounded','staged','pilot','fast','paper')),None)
            if method is None:method='RD'if re.search(r'analysis|readout|prediction|processing',source,re.I)else'RUN'
    method=_METHODS.get(method.lower(),method)
    if method.lower()in('n','full','f111','fiber'):method='F'
    method=_word(method,5)
    work=re.search(r'\b(ng|ev|ep):\s*(\d+(?:k|m)?)\b',source,re.I)
    if work:amount=work[1].lower()+':'+work[2]
    else:
        work=re.search(r'\b(\d+) (?:new )?real(?:9-PVT)? designs\b',source,re.I);amount='ng:'+work[1]if work else''
    eta=re.search(r'~\d+(?:\.\d+)?s\b',source);pieces=[subject,method,identity,amount]
    def joined():return' '.join(p for p in pieces if p)
    result=joined()
    if measure(result)>limit:pieces[2]=identity.lstrip('s:');result=joined()
    if measure(result)>limit:pieces[1]={'AGE':'A','-KCL':'-K','-GEO':'-G','-ADJ':'-A','-INV':'-I'}.get(method,method);result=joined()
    if measure(result)>limit:pieces[0]=re.sub(r'^AB[PD](\d)',r'AB\1',subject);result=joined()
    if measure(result)>limit:pieces[3]=re.sub(r'^(ng|ev|ep):',lambda m:{'ng':'n','ev':'v','ep':'e'}[m[1]],amount);result=joined()
    if measure(result)>limit:pieces[0]=_word(pieces[0],2);pieces[1]=_word(pieces[1],2);result=joined()
    if measure(result)>limit:
        result=next((word for word in (subject,method,'RUN')if measure(word)<=limit),'')
        if not result:
            for letter in subject:
                if measure(result+letter)>limit:break
                result+=letter
        result=result or'-'
    if progress and measure(progress[1]+' '+result)<=limit:result=progress[1]+' '+result
    if eta and measure(result+' '+eta[0])<=limit:result+=' '+eta[0]
    return result
