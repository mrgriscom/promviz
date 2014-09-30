import os
import json
import sys
import tempfile
import uuid
from subprocess import Popen, PIPE
import psycopg2
from psycopg2.extras import DictCursor
import util as u

def calc_prom():
    os.popen('/usr/lib/jvm/java-7-openjdk-amd64/bin/java -Xms6000m -Xloggc:/tmp/gc -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar:/home/drew/dev/promviz/promviz/lib/gson-2.2.4.jar promviz.DEMManager %s > /tmp/prominprogress' % ' '.join(sys.argv[1:]))
    os.popen('mv /tmp/prominprogress ~/tmp/pvout/prombackup')

    with open('/home/drew/tmp/pvout/prombackup') as f:
        data = json.load(f)
    data.sort(key=lambda e: e['summit']['prom'], reverse=True)
    return data

def get_name(conn, pos, type, res=40030000./360/3600):
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

def write_master(points):
    def core(p):
        _core = p[p['type']]
        _core['type'] = p['type']
        return _core
    data = map(core, points)
    with open('/home/drew/tmp/pvout/_index', 'w') as f:
        json.dump(data, f)

def save_point(p):
    path = '/home/drew/tmp/pvout/prom%s.json' % p[p['type']]['geo']
    with open(path, 'w') as f:
        content = json.dumps(p, indent=2)
        f.write(content)

if __name__ == "__main__":

    data = calc_prom()
    def _process(p):
        try:
            return process_point(p)
        except:
            sys.stderr.write('error on %s\n' % p)
    points = map(_process, data)

    conn = psycopg2.connect('dbname=%s' % 'gazetteer')
    for p in points:
        add_name(p, conn)

    set_children(points)

    for p in points:
        save_point(p)

    write_master(points)

