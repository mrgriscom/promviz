import settings
import os
import os.path
import json
import util as u

def all_points():
    files = os.listdir(settings.dir_out)
    for fn in files:
        if not fn.endswith('.json'):
            continue
        path = os.path.join(settings.dir_out, fn)
        with open(path) as f:
            yield path, json.load(f)

if __name__ == "__main__":

    with open('/tmp/thresh') as f:
        pmap = dict(ln.strip().split() for ln in f.readlines())

    for path, p in all_points():
        pthresh = pmap.get(p['_thresh']['geo'])
        #del p['_thresh']
        if pthresh:
            p['pthresh'] = {'geo': pthresh}

        with open(path, 'w') as f:
            json.dump(p, f)

