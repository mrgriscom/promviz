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
    f = os.popen(' '.join([
            '/usr/lib/jvm/java-7-openjdk-amd64/bin/java',
            '-ea',
            '-Xms%(memory)s',
            '-Xloggc:/tmp/gc',
            '-Dfile.encoding=UTF-8',
            '-classpath %(root)s/bin:%(root)s/lib/guava-14.0.1.jar:%(root)s/lib/gson-2.2.4.jar',
            'promviz.Main %(args)s',
        ]) % {
            'args': ' '.join("'%s'" % k for k in sys.argv[1:]),
            'memory': settings.memory,
            'root': projroot,
        })

    while True:
        ln = f.readline()
        if not ln:
            break

        try:
            yield json.loads(ln)
        except Exception:
            print 'invalid json [%s]' % ln

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
    out = load_point(p) or {}
    addendum = bool(out)

    for fact in p['facts']:
        if 'summit' in fact:
            type = 'peak' if p['up'] else 'pit'
            out.update({
                'type': type,
                type: {
                    'prom': fact['summit']['prom'],
                    'elev': fact['summit']['elev'],
                    'min_bound': fact['min_bound'],
                    'coords': fact['summit']['coords'],
                    'geo': fact['summit']['geo'],
                },
                'saddle': {
                    'elev': fact['saddle']['elev'],
                    'coords': fact['saddle']['coords'],
                    'geo': fact['saddle']['geo'],
                },
                'threshold_path': fact['path'],
            })
            if 'thresh' in fact:
                out['threshold'] = {
                    'coords': fact['thresh']['coords'],
                    'geo': fact['thresh']['geo'],
                }
        elif 'parent' in fact:
            out.update({
                'parent': {
                    'geo': fact['parent']['geo'],
                },
                'parent_path': fact['path'],
            })
        elif 'pthresh' in fact:
            out['pthresh'] = {
                'geo': fact['pthresh']['geo'],
            }
        elif 'subsaddle' in fact:
            ss = {
                'saddle': {
                    'elev': fact['saddle']['elev'],
                    'coords': fact['saddle']['coords'],
                    'geo': fact['saddle']['geo'],
                },
                'for': {
                    'geo': fact['forPeak']['geo'],
                },
            }
            sskey = '%s_subsaddles' % ('prom' if fact['isDomain'] else 'height')
            subsaddles = out.get(sskey, [])
            subsaddles.append(ss)
            out[sskey] = sorted(subsaddles, key=lambda ss: ss['saddle']['geo'])  

    return out, addendum

def add_name(p, conn):
    def _name(type):
        data = p[type]
        name = get_name(conn, data['coords'], type)
        if name:
            data['name'] = name

    _name(p['type'])
    _name('saddle')

def write_master(ix, mode, i):
    ix.sort(key=lambda e: e['prom'], reverse=True)
    with open('/tmp/pvindex', 'w') as f:
        json.dump(ix, f)
    os.popen('mv /tmp/pvindex %s' % os.path.join(settings.dir_out, '_index_%s_%d' % (mode, i + 1)))

def save_point(p):
    if not p:
        return

    path = os.path.join(settings.dir_out, 'prom%s.json' % p[p['type']]['geo'])
    with open(path, 'w') as f:
        content = json.dumps(p, indent=2)
        f.write(content)

def load_point(p):
    path = os.path.join(settings.dir_out, 'prom%s.json' % p['key'])
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)

if __name__ == "__main__":

    for d in [getattr(settings, k) for k in dir(settings) if k.startswith('dir_')]:
        os.popen('mkdir -p "%s"' % d)

    # java runs this too, but swallows stderr
    os.popen('python demregion.py "%s" > /dev/null' % sys.argv[2])

    conn = None #psycopg2.connect('dbname=%s' % 'gazetteer')

    if '--searchup' in sys.argv:
        prom_mode = 'up'
    elif '--searchdown' in sys.argv:
        prom_mode = 'down'
    else:
        prom_mode = None

    def core(p):
        try:
            _core = p[p['type']]
            _core['type'] = p['type']
            return _core
        except:
            print 'orphan promfact'
            raise

    index = {
        'data': [],
        'i': 0,
        'last_interim': time.time(),
    }
    def flush():
        if not prom_mode:
            return

        print 'flushing... (%d)' % len(index['data'])
        index['last_interim'] = time.time()
        if not index['data']:
            return

        write_master(index['data'], prom_mode, index['i'])
        index['i'] += 1
        index['data'] = []

    INTERIM_INTERVAL = 300
    for p in calc_prom():
        try:
            p, addendum = process_point(p)
        except Exception, e:
            sys.stderr.write('error [%s, %s] on %s\n' % (type(e), e, p))

        if not addendum:
            index['data'].append(core(p))
            add_name(p, conn)

        save_point(p)

        if time.time() - index['last_interim'] > INTERIM_INTERVAL:
            flush()

    flush()

