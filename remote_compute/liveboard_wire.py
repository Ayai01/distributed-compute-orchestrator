"""CBOR JSON-value encoding for the display transport (RFC 8949)."""
import math
import struct


def encode(value):
    """Keep integers and binary64 numbers typed; use definite containers only."""
    def head(major, count):
        if count < 24:return bytes([(major << 5) | count])
        for limit, tag, format in ((256,24,'B'),(65536,25,'H'),(2**32,26,'I'),(2**64,27,'Q')):
            if count < limit:return bytes([(major << 5) | tag])+struct.pack('>'+format,count)
        raise ValueError('Display integer exceeds CBOR uint64 range')

    def item(v, depth):
        if depth > 24:raise ValueError('Display CBOR nesting exceeds budget')
        if v is None:return b'\xf6'
        if v is False:return b'\xf4'
        if v is True:return b'\xf5'
        if type(v) is int:return head(0,v) if v >= 0 else head(1,-1-v)
        if type(v) is float:
            if not math.isfinite(v):raise ValueError('Display numbers must be finite')
            return b'\xfb'+struct.pack('>d',v)
        if isinstance(v,str):
            data=v.encode('utf8');return head(3,len(data))+data
        if isinstance(v,(list,tuple)):
            return head(4,len(v))+b''.join(item(x,depth+1) for x in v)
        if isinstance(v,dict):
            if any(not isinstance(k,str) for k in v):raise ValueError('Display map keys must be text')
            return head(5,len(v))+b''.join(item(k,depth+1)+item(x,depth+1) for k,x in v.items())
        raise ValueError('Unsupported display CBOR value')

    return item(value,0)
