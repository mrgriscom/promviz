from tornado.ioloop import IOLoop
import tornado.web as web
import tornado.gen as gen
from tornado.template import Template
import logging
import os
from optparse import OptionParser
import json
from datetime import datetime, timedelta
import sys
import re
import collections
import math

import os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import settings
import util

def summit_feature(s, **props):
    f = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [s['coords'][1], s['coords'][0]],
        },
        'properties': {
            'type': s.get('type'),
            'name': s.get('name'),
            'prom_ft': s['prom'] / .3048,
            'elev_ft': s['elev'] / .3048,
            'min_bound': s.get('min_bound', False),
            'geo': s['geo'],
        },
    }
    f['properties'].update(props)
    return f

def to_geojson(k):
    def _feature(coords, **props):
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
            summit_feature(k[k['type']], type=k['type']),
            _feature(k['saddle']['coords'],
                     type='saddle',
                     name=k['saddle'].get('name'),
                     elev_ft=k['saddle']['elev'] / .3048,
                     geo=k['saddle']['geo'],
                    ),
            _feature(k['threshold_path'], type='divide'),
            _feature(k['parent_path'], type='toparent'),
        ]
    }
    if k.get('threshold'):
        data['features'].append(
            _feature(k['threshold']['coords'],
                     type='threshold',
                 ),
        )
    if k.get('parent'):
        data['features'].append(summit_feature(k['_parent'][k['_parent']['type']], type='parent'))
    for child in k.get('_children', []):
        data['features'].append(summit_feature(child[child['type']], type='child', ix=child['ix']))
    #if k.get('runoff'):
    #    data['features'].append(feature(k['runoff'], type='domain'))

    return data

class MapViewHandler(web.RequestHandler):
    def get(self, tag):
        def loadgeo(geo):
            with open(os.path.join(settings.dir_out, 'prom%s.json' % geo)) as f:
                return json.load(f)
        
        data = loadgeo(tag)
        data['_parent'] = loadgeo(data['parent']['geo']) if 'parent' in data else None
        data['_children'] = [loadgeo(c) for c in data.get('children', [])]
        for i, ch in enumerate(data['_children']):
            ch['ix'] = i + 1

        self.render('map.html', mode='single', data=to_geojson(data))

class SummitsHandler(web.RequestHandler):
    def get(self, prefix):
        max_n = int(self.get_argument('max', 1000))

        summits = filter(lambda e: e['geo'].startswith(prefix), index)
        summits = sorted(summits, key=lambda e: e['prom'], reverse=True)[:max_n]

        data = {
            'type': 'FeatureCollection',
            'features': [summit_feature(s) for s in summits],
        }

        self.render('map.html', mode='many', data=data)

def _load_points():
    i = 0
    while True:
        i += 1
        path = os.path.join(settings.dir_out, '_index%d' % i)
        if not os.path.exists(path):
            break
        with open(path) as f:
            for p in json.load(f):
                yield p

def load_points(cull):
    if cull > .999999:
        index = list(_load_points())
    else:
        count = 0
        for p in _load_points():
            count += 1
        retain = int(math.ceil(count * cull))

        index = []
        for chunk in util.chunker(_load_points(), 50000):
            index.extend(chunk)
            index.sort(key=lambda e: e['prom'], reverse=True)
            index = index[:retain]

    print 'loaded (%d)' % len(index)
    return index

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-c", "--cull", dest="cull", type='int', default=100,
                  help="only retain top N% of points")
    (options, args) = parser.parse_args()

    try:
        port = int(args[0])
    except IndexError:
        port = 8000

    application = web.Application([
        (r'/view/(?P<tag>.*)', MapViewHandler),
        (r'/summits/(?P<prefix>[0-9a-f]*)', SummitsHandler),
        (r'/(.*)', web.StaticFileHandler, {'path': 'static'}),
    ], template_path='templates', debug=True)
    application.listen(port)

    index = load_points(.01 * options.cull)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')
