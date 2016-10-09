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

def runoff(up, saddles):
    args = ['u' if up else 'd']
    args.extend(saddles)

    projroot = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'promviz')
    f = os.popen(' '.join([
            '/usr/lib/jvm/java-8-openjdk-amd64/bin/java',
            '-ea',
            '-Xms%(memory)s',
            '-Xloggc:/tmp/gc',
            '-Dfile.encoding=UTF-8',
            '-classpath %(root)s/bin:%(root)s/lib/guava-14.0.1.jar:%(root)s/lib/gson-2.2.4.jar',
            'promviz.debug.Runoff %(args)s',
        ]) % {
            'args': ' '.join("'%s'" % k for k in args),
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

    peak = load_point({'key': sys.argv[1]})

    up = peak['type'] == 'peak'
    saddles = [peak['saddle']['geo']]
    saddles.extend(k['saddle']['geo'] for k in peak.get('prom_subsaddles', []))

    traces = runoff(up, saddles).next()

    peak['runoff'] = traces
    save_point(peak)

