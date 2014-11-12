import os
import json
import sys
import tempfile
import uuid
from subprocess import Popen, PIPE
#import psycopg2
#from psycopg2.extras import DictCursor
import util as u
import time
import settings
import os.path

def calc_prom():
    projroot = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'promviz')
    f = os.popen('/usr/lib/jvm/java-7-openjdk-amd64/bin/java -Xms%(memory)s -Xloggc:/tmp/gc -Dfile.encoding=UTF-8 -classpath %(root)s/bin:%(root)s/lib/guava-14.0.1.jar:%(root)s/lib/gson-2.2.4.jar promviz.DEMManager %(args)s' % {'args': ' '.join("'%s'" % k for k in sys.argv[1:]), 'memory': settings.memory, 'root': projroot})

    while True:
        ln = f.readline()
        if not ln:
            break

        try:
            yield json.loads(ln)
        except:
            print 'invalid json'

def get_name(conn, pos, type, res=40030000./360/3600):
    return None

    cur = conn.cursor(cursor_factory=DictCursor)
    feature_classes = {
        'peak': ['Summit', 'Pillar', 'Ridge'],
        'pit': ['Basin', 'Crater', 'Flat', 'Lake', 'Valley'],
        'saddle': ['Canal', 'Gap', 'Glacier', 'Isthmus', 'Swamp'],
    }[type]
    tolerance = {
        'peak': 8,
        'pit': 8,
        'saddle': 8,
    }[type]

    cur.execute("""select name from features where ST_DWithin(loc, %s, %s) and class = any (%s) limit 1""",
                ['point(%s %s)' % (pos[1], pos[0]), tolerance*res, feature_classes])
    results = cur.fetchall()
    return results[0]['name'] if results else None
    
def process_point(p):
    type = 'peak' if p['up'] else 'pit'
    out = {
        'type': type,
        type: {
            'prom': p['summit']['prom'],
            'elev': p['summit']['elev'],
            'min_bound': p.get('min_bound', False),
            'coords': p['summit']['coords'],
            'geo': p['summit']['geo'],
        },
        'saddle': {
            'elev': p['saddle']['elev'],
            'coords': p['saddle']['coords'],
            'geo': p['saddle']['geo'],
        },
        'threshold_path': p['higher_path'],
        'parent_path': p['parent_path'],
    }
    if p.get('higher'):
        out['threshold'] = {
            'coords': p['higher']['coords'],
            'geo': p['higher']['geo'],
        }
    if p.get('parent'):
        out['parent'] = {
            'geo': p['parent']['geo'],
        }
    
    return out

def add_name(p, conn):
    def _name(type):
        data = p[type]
        name = get_name(conn, data['coords'], type)
        if name:
            data['name'] = name

    _name(p['type'])
    _name('saddle')

def set_children(points):
    index = dict((p[p['type']]['geo'], p) for p in points)
    children = u.map_reduce(filter(lambda p: p.get('parent'), points),
                            lambda p: [(p['parent']['geo'], p)],
                            lambda v: [p[p['type']]['geo'] for p in sorted(v, key=lambda p: p[p['type']]['prom'], reverse=True)])
    for k, v in children.iteritems():
        index[k]['children'] = v

def write_master(ix, mode, i):
    ix.sort(key=lambda e: e['prom'], reverse=True)
    with open('/tmp/pvindex', 'w') as f:
        json.dump(ix, f)
    os.popen('mv /tmp/pvindex %s' % os.path.join(settings.dir_out, '_index_%s_%d' % (mode, i + 1)))

def save_point(p):
    path = os.path.join(settings.dir_out, 'prom%s.json' % p[p['type']]['geo'])
    with open(path, 'w') as f:
        content = json.dumps(p, indent=2)
        f.write(content)

if __name__ == "__main__":

    for d in (settings.dir_dem, settings.dir_net, settings.dir_out, settings.dir_netdump):
        os.popen('mkdir -p "%s"' % d)

    os.popen('python demregion.py "%s" > /dev/null' % sys.argv[2])

    conn = None #psycopg2.connect('dbname=%s' % 'gazetteer')

    if '--searchup' in sys.argv:
        prom_mode = 'up'
    elif '--searchdown' in sys.argv:
        prom_mode = 'down'
    else:
        prom_mode = None

    def core(p):
        _core = p[p['type']]
        _core['type'] = p['type']
        return _core

    index = {
        'data': [],
        'i': 0,
        'last_interim': time.time(),
    }
    def flush():
        if not prom_mode:
            return

        print 'flushing... (%d)' % len(index['data'])
        write_master(index['data'], prom_mode, index['i'])
        index['i'] += 1
        index['last_interim'] = time.time()
        index['data'] = []

    INTERIM_INTERVAL = 300
    for p in calc_prom():
        try:
            p = process_point(p)
        except:
            sys.stderr.write('error on %s\n' % p)

        index['data'].append(core(p))
        add_name(p, conn)

        #set_children(points)

        save_point(p)

        if time.time() - index['last_interim'] > INTERIM_INTERVAL:
            flush()

    flush()

