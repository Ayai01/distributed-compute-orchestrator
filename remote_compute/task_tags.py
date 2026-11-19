"""Width-aware formatting for opaque task labels and protocol counters."""

MAX_TAG = 24


def quantity(value,unit=''):
    """Compact explicit work counters while retaining their unit."""
    count=float(value)
    suffix=next((s for scale,s in ((1e9,'G'),(1e6,'M'),(1e3,'k'))if count>=scale),'')
    divisor={'G':1e9,'M':1e6,'k':1e3}.get(suffix,1)
    number=format(count/divisor,'.3g') if suffix else format(count,'g')
    return number+suffix+{'cycles':'cy','events':'ev','records':'rec','steps':'st'}.get(unit,unit)


def compact_tag(job, width=MAX_TAG, preferred=None, measure=len):
    """Display the submitted label without applying project rules."""
    text = preferred if preferred is not None else job.get('label')
    label = ' '.join(str(text or '-').split())
    label = ''.join(character for character in label if ord(character) >= 32 and character != '\x7f')
    width = max(1, int(width))
    end = len(label)
    while end and measure(label[:end]) > width:
        end -= 1
    return label[:end].rstrip() or '-'
