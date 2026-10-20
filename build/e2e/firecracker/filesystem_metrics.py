"""Read-byte counters used as evidence, never infer zero from an unknown metric."""

import re


def juicefs_read_bytes(text):
    names = ('juicefs_object_request_data_bytes', 'juicefs_object_request_data_bytes_total')
    if not (any(line.startswith('# TYPE ' + name + ' counter') for name in names for line in text.splitlines())
            or '# TYPE juicefs_blockcache_miss counter' in text.splitlines()):
        raise ValueError('JuiceFS object read counter is unavailable')
    # CounterVec is omitted until the first object operation. The always-registered
    # blockcache counter identifies the pinned client registry in that case.
    return sum(float(line.split()[-1]) for line in text.splitlines()
               if any(line.startswith(name + '{') for name in names)
               and re.search(r'(?:\{|,)method="GET"(?:,|\})', line))
