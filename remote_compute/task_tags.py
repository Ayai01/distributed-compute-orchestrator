"""Semantic display tags; full requests and labels remain unchanged."""
import hashlib
import re

MAX_TAG=24
_METHODS={'fiber-mo':'F','fiber_mo':'F','agemoea2':'AGE','age-moea2':'AGE','age2':'AGE',
          'maoea_hap':'HAP','maoea-hap':'HAP','nsga-ii':'N2','nsga2':'N2','nsga-iii':'N3','nsga3':'N3',
          'nsde-r':'NDR','nsder':'NDR','hybrid_v4':'HV4','hybrid_mf':'HMF','readout':'RD'}


def _word(value,limit):
    value=re.sub(r'[^A-Za-z0-9:+-]','',str(value))or'JOB'
    if len(value)<=limit:return value
    tail=hashlib.blake2s(value.encode(),digest_size=2).hexdigest()[:2]
    return value[:max(0,limit-2)]+tail[:limit]


def compact_tag(job,width=MAX_TAG,preferred=None,measure=len):
    limit=max(1,min(MAX_TAG,int(width)))
    raw=' '.join(str(job.get('label','')).split());text=' '.join(str(preferred if preferred is not None else raw).split())
    clean=text.replace('...','').replace('\u2026','')
    if clean==text and measure(text)<=limit:return text
    progress=re.match(r'^(\d+/\d+|\d+%)\s+',clean)
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
    if existing:identity='s'+existing[1].lower()
    elif full_seed or pair:identity='s'+format(int(full_seed[1]if full_seed else pair[2])&0xffff,'04x')
    else:
        short=re.search(r'(?<!\w)([0-9a-f]{4})(?!\w)',clean,re.I)
        identity=':'+short[1].lower()if short else':'+str(job.get('id','????'))[-4:]
    role=re.search(r'(?<!\w)-(KCL|GEO|ADJ|INV|C[123]|PROXY|GATE)\b',source,re.I)
    if pair:method=pair[1]
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
    if len(result)>limit:pieces[2]=identity.lstrip('s:');result=joined()
    if len(result)>limit:pieces[1]={'AGE':'A','-KCL':'-K','-GEO':'-G','-ADJ':'-A','-INV':'-I'}.get(method,method);result=joined()
    if len(result)>limit:pieces[0]=re.sub(r'^AB[PD](\d)',r'AB\1',subject);result=joined()
    if len(result)>limit:pieces[3]=re.sub(r'^(ng|ev|ep):',lambda m:{'ng':'n','ev':'v','ep':'e'}[m[1]],amount);result=joined()
    if len(result)>limit:pieces[0]=_word(pieces[0],2);pieces[1]=_word(pieces[1],2);result=joined()
    if len(result)>limit:result=(pieces[2]or pieces[0])[-limit:]
    if progress and len(progress[1]+' '+result)<=limit:result=progress[1]+' '+result
    if eta and len(result+' '+eta[0])<=limit:result+=' '+eta[0]
    return result
