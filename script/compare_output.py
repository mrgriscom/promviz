import os
import json
import sys
import tempfile
import pprint
import os.path

def points():
    for f in os.listdir('/tmp/promviz/out'):
        if f.startswith('prom'):
            yield f

def path(f, ref=False):
    dir = {
        True: sys.argv[1],
        False: '/tmp/promviz/out',
    }[ref]
    return os.path.join(dir, f)

dcur = tempfile.mkdtemp()
dref = tempfile.mkdtemp()

diffs = False
for k in points():
    fcur = path(k, False)
    fref = path(k, True)

    if not os.path.exists(fref):
        print 'missing', k
        continue

    with open(fcur) as f:
        cur = json.load(f)
    with open(fref) as f:
        ref = json.load(f)

    #psimp = []
    #for p in ref['threshold_path']:
    #    prev = psimp[-1] if psimp else None
    #    dup = all(abs(a-b)<1e-6 for a, b in zip(p, prev)) if prev else False
    #    if not dup:
    #        psimp.append(p)
    #ref['threshold_path'] = psimp

    if cur != ref:
        diffs = True
        print cur['peak']['prom'], 'mismatch', k
        with open(os.path.join(dcur, k), 'w') as f:
            pprint.pprint(cur, f)
        with open(os.path.join(dref, k), 'w') as f:
            pprint.pprint(ref, f)

if diffs:
    os.popen('meld %s %s' % (dcur, dref))
else:
    print 'no diffs!'
