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
        with open(os.path.join(settings.dir_out, fn)) as f:
            yield json.load(f)

if __name__ == "__main__":

    def _info():
        for p in all_points():
            parent = p.get('parent')
            if parent:
                yield {'p': p[p['type']]['geo'], 'parent': parent['geo'], 'prom': p[p['type']]['prom']}

    children = u.map_reduce(_info(),
                            lambda inf: [(inf['parent'], inf)],
                            lambda v: [inf['p'] for inf in sorted(v, key=lambda e: e['prom'], reverse=True)])
    print 'loaded: %s children, %s parents' % (sum(map(len, children.values())), len(children))

    for k, v in children.iteritems():
        path = os.path.join(settings.dir_out, 'prom%s.json' % k)
        with open(path) as f:
            data = json.load(f)
        data['children'] = v
        with open(path, 'w') as f:
            json.dump(data, f)
