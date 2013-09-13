import os
import json
import sys
import tempfile
import uuid

tag = uuid.uuid4().hex[:12]

raw = os.popen('/usr/lib/jvm/java-7-openjdk-amd64/bin/java -Xms6000m -Xloggc:/tmp/gc -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar promviz.DEMManager %s' % ' '.join(sys.argv[1:])).readlines()
#raw = open('/tmp/debug.output').readlines()

data = json.loads('[%s]' % ', '.join(raw))

with open('/tmp/prombackup', 'w') as f:
    json.dump(data, f)

data.sort(key=lambda e: e['prom'], reverse=True)

def to_geojson(k):
    def feature(coords, **props):
        def coord(c):
            return [c[1], c[0]]

        if hasattr(coords[0], '__iter__'):
            type = 'LineString'
            coords = [coord(c) for c in coords]
        else:
            type = 'Point'
            coords = coord(coords)

        return {
            'type': 'Feature',
            'geometry': {
                'type': type,
                'coordinates': coords,
            },
            'properties': props,
        }

    content = json.dumps({
        'type': 'FeatureCollection',
        'features': [
            feature(k['summit'],
                    type='summit',
                    prom_ft=k['prom'] / .3048,
                    elev_ft=k['elev'] / .3048,
                    min_bound=k['min_bound'],
                    geo=k['summitgeo'],
                    ),
            feature(k['saddle'],
                    type='saddle',
                    elev_ft=(k['elev'] - k['prom']) / .3048,
                    geo=k['saddlegeo'],
                    ),
            feature(k['path'], type='divide'),
        ]
    }, indent=2)

    path = '/tmp/prom%s.geojson' % k['summitgeo']
    with open(path, 'w') as f:
        f.write(content)
    
    print '<a target="_blank" href="http://localhost:8000/view/%(path)s?%(tag)s"><span style="font-family: monospace;">%(peakgeo)s %(saddlegeostem)s</span> %(prom).1f%(minbound)s (%(pathlen)d)</a><br>' % {
        'path': path[5:],
        'prom': k['prom'] / .3048,
        'minbound': '*' if k['min_bound'] else '',
        'pathlen': len(k['path']),
        'peakgeo': k['summitgeo'][:11],
        'saddlegeostem': common_prefix(k['saddlegeo'], k['summitgeo'])[:11],
        'tag': tag,
    }

def common_prefix(sub, sup, pad='-'):
    x = list(sub)
    for i in range(len(sub)):
        if sub[i] == sup[i]:
            x[i] = pad
        else:
            break
    return ''.join(x)

for k in data:
    try:
        to_geojson(k)
    except:
        sys.stderr.write('error on %s\n' % k)

