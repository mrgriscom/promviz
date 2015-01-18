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

meat = lambda k: k[k['type']]

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
            'prom_m': s['prom'],
            'elev_m': s['elev'],
            'prom_ft': s['prom'] / .3048,
            'elev_ft': s['elev'] / .3048,
            'min_bound': s.get('min_bound', False),
            'geo': s['geo'],
        },
    }
    f['properties'].update(props)
    return f

def saddle_feature(k, type='saddle', **props):
    f = _feature(k['saddle']['coords'],
                 type=type,
                 name=k['saddle'].get('name'),
                 elev_m=k['saddle']['elev'],
                 elev_ft=k['saddle']['elev'] / .3048,
                 geo=k['saddle']['geo'],
                 )
    if k.get('_for'):
        props['peak'] = summit_feature(meat(k['_for']), type='sspeak')
    f['properties'].update(props)
    return f

def to_geojson(k):
    data = {
        'type': 'FeatureCollection',
        'features': [
            summit_feature(k[k['type']], type=k['type']),
            saddle_feature(k),
            _feature(k['threshold_path'], type='divide'),
            _feature(k['parent_path'], type='toparent'),
        ]
    }
    for ss in k.get('subsaddles', []):
        data['features'].append(saddle_feature(ss, 'subsaddle', higher=ss['for']['higher'], domain=ss['domain']))

    if k.get('threshold'):
        data['features'].append(
            _feature(k['threshold']['coords'],
                     type='threshold',
                 ),
        )
    if k.get('parent'):
        data['features'].append(summit_feature(meat(k['_parent']), type='parent'))
    for child in k.get('_children', []):
        data['features'].append(summit_feature(meat(child), type='child', ix=child['ix']))
        data['features'].append(saddle_feature(child, type='childsaddle', ix=child['ix']))
    if k.get('pthresh'):
        data['features'].append(summit_feature(meat(k['_pthresh']), type='pthresh'))
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
        for ss in data.get('subsaddles', []):
            ss['_for'] = loadgeo(ss['for']['geo'])
        data['_pthresh'] = loadgeo(data['pthresh']['geo']) if 'pthresh' in data else None        

        self.render('map.html', mode='single', data=to_geojson(data))

class SummitsHandler(web.RequestHandler):
    def initialize(self, mode):
        self.mode = mode

    def get(self, prefix):
        max_n = int(self.get_argument('max', 1000))

        summits = filter(lambda e: e['geo'].startswith(prefix), {'up': index_up, 'down': index_down}[self.mode])
        summits = sorted(summits, key=lambda e: e['prom'], reverse=True)[:max_n]
        if summits:
            print 'threshold: %s' % summits[-1]['prom']

        data = {
            'type': 'FeatureCollection',
            'features': [summit_feature(s) for s in summits],
        }

        self.render('map.html', mode='many', data=data)

def _load_points(mode):
    i = 0
    while True:
        i += 1
        path = os.path.join(settings.dir_out, '_index_%s_%d' % (mode, i))
        if not os.path.exists(path):
            break
        with open(path) as f:
            for p in json.load(f):
                yield p

def load_points(mode, maxnum):
    if not maxnum:
        index = list(_load_points(mode))
    else:
        maxnum *= 1000
        chunksize = max(.2 * maxnum, 50000)

        index = []
        for chunk in util.chunker(_load_points(mode), chunksize):
            index.extend(chunk)
            index.sort(key=lambda e: e['prom'], reverse=True)
            index = index[:maxnum]

    print 'loaded (%d)' % len(index)
    return index

if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-m", "--max", dest="max", type='int',
                  help="only load <max> thousand greatest points")
    parser.add_option("-d", "--dir", dest="dir", help="load from directory")
    (options, args) = parser.parse_args()

    try:
        port = int(args[0])
    except IndexError:
        port = 8000

    if options.dir:
        settings.dir_out = options.dir

    application = web.Application([
        (r'/view/(?P<tag>.*)', MapViewHandler),
        (r'/summits/(?P<prefix>[0-9a-f]*)', SummitsHandler, {'mode': 'up'}),
        (r'/sinks/(?P<prefix>[0-9a-f]*)', SummitsHandler, {'mode': 'down'}),
        (r'/(.*)', web.StaticFileHandler, {'path': 'static'}),
    ], template_path='templates', debug=True)
    application.listen(port)

    index_up = load_points('up', options.max)
    index_down = load_points('down', options.max)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')
