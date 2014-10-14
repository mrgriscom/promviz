import struct
import collections
import util as u

def load_full(mode):
    path = '/tmp/promnet-%s' % mode
    fmt = '>QQ'
    size = struct.calcsize(fmt)
    with open(path) as f:
        while True:
            data = f.read(size)
            if not data:
                return
            yield tuple(parse_ix(p) for p in struct.unpack_from(fmt, data))

def parse_ix(ix):
    if ix == 0xFFFFFFFFFFFFFFFF:
        return None

    frame = ix >> 48
    x = (ix >> 24) & 0xFFFFFF
    y = ix & 0xFFFFFF
    signed = lambda a: (a + 2**23) % 2**24 - 2**23
    return (frame, signed(x), signed(y))

def pack_ix(ix):
    frame, x, y = ix
    return (frame << 48) | ((x & 0xffffff) << 24) | (y & 0xffffff)

def prefix(ix, bits):
    chop = lambda a: a - a % 2**bits
    return (ix[0], chop(ix[1]), chop(ix[2]))

BASE_RES = 5
def initial_tally(edges):
    buckets = collections.defaultdict(lambda: 0)
    for e in edges:
        if e[1] is None:
            continue

        pf1 = prefix(e[0], BASE_RES)
        pf2 = prefix(e[1], BASE_RES)
        buckets[pf1] += 1
        if pf2 != pf1:
            buckets[pf2] += 1

    return buckets

BUCKET_MAX_POINTS = 2048
def consolidate_tally(tally):
    res = BASE_RES
    prev_level = tally
    grouped = {}
    lineage = {}
    while True:
        grouped.update(((res, k), v) for k, v in prev_level.iteritems())
        res += 1
        next_level = u.map_reduce(prev_level.iteritems(), lambda (k, v): [(prefix(k, res), v)], sum)
        if len(next_level) == len(prev_level):
            break
        lineage.update(((res, pf), children) for pf, children in u.map_reduce(prev_level.keys(), lambda e: [(prefix(e, res), (res - 1, e))]).iteritems())
        prev_level = next_level

    max_level = max(k[0] for k in grouped.keys())
    roots = [k for k in grouped.keys() if k[0] == max_level]

    buckets = set()
    def descend(q):
        if grouped[q] <= BUCKET_MAX_POINTS or q not in lineage:
            buckets.add(q)
        else:
            for child in lineage[q]:
                descend(child)
    for root in roots:
        descend(root)

    return buckets

def partition(mode, buckets):
    def bucket_path(b):
        return '/tmp/pnet/%s-%d,%d,%d,%d' % (mode, b[0], b[1][0], b[1][1], b[1][2])
    f_ix = dict((b, open(bucket_path(b), 'w')) for b in buckets)

    def get_bucket(ix):
        for res in xrange(BASE_RES, 100):
            b = (res, prefix(ix, res))
            if b in buckets:
                return b

    def write_edge(bucket, e):
        data = struct.pack('>QQ', pack_ix(e[0]), pack_ix(e[1]) if e[1] is not None else 0xFFFFFFFFFFFFFFFF)
        f_ix[bucket].write(data)

    data = load_full(mode)
    for e in data:
        bucket1 = get_bucket(e[0])
        bucket2 = get_bucket(e[1]) if e[1] is not None else None

        write_edge(bucket1, e)
        if bucket2 is not None and bucket2 != bucket1:
            write_edge(bucket2, e)

    for f in f_ix.values():
        f.close()

if __name__ == "__main__":

    tally = initial_tally(load_full('up'))
    buckets = consolidate_tally(tally)
    partition('up', buckets)
