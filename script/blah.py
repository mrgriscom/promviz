import os
import json
import sys
import tempfile
import uuid
from subprocess import Popen, PIPE

tag = uuid.uuid4().hex[:12]

os.popen('/usr/lib/jvm/java-7-openjdk-amd64/bin/java -Xms6000m -Xloggc:/tmp/gc -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar:/home/drew/dev/promviz/promviz/lib/gson-2.2.4.jar promviz.DEMManager %s > /tmp/prombackup' % ' '.join(sys.argv[1:]))

with open('/tmp/prombackup') as f:
    data = json.load(f)

data.sort(key=lambda e: e['summit']['prom'], reverse=True)

def to_geojson(k):
    def feature(coords, **props):
        def coord(c):
            return [c[1], c[0]]

        if hasattr(coords[0], '__iter__'):
            if hasattr(coords[0][0], '__iter__'):
                type = 'MultiLineString'
                coords = [[coord(c) for c in k] for k in coords]
            else:
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

    data = {
        'type': 'FeatureCollection',
        'features': [
            feature(k['summit']['coords'],
                    type='summit',
                    prom_ft=k['summit']['prom'],
                    elev_ft=k['summit']['elev'],
                    min_bound=k.get('min_bound', False),
                    geo=k['summit']['geo'],
                    ),
            feature(k['saddle']['coords'],
                    type='saddle',
                    elev_ft=k['saddle']['elev'],
                    geo=k['saddle']['geo'],
                    ),
            feature(k['higher_path'], type='divide'),
            feature(k['parent_path'], type='toparent'),
        ]
    }
    if k.get('higher'):
        data['features'].append(
            feature(k['higher']['coords'],
                    type='higher',
                    elev_ft=k['higher']['elev'],
                    geo=k['higher']['geo'],
                ),
        )
    if k.get('parent'):
        data['features'].append(
            feature(k['parent']['coords'],
                    type='parent',
                    prom_ft=k['parent']['prom'],
                    elev_ft=k['parent']['elev'],
                    geo=k['parent']['geo'],
                ),
        )
    if k.get('runoff'):
        data['features'].append(feature(k['runoff'], type='domain'))

    content = json.dumps(data, indent=2)

    path = '/tmp/prom%s.geojson' % k['summit']['geo']
    with open(path, 'w') as f:
        f.write(content)
    
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

