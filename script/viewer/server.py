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

def feature(s):
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [s['summit']['coords'][1], s['summit']['coords'][0]],
        },
        'properties': {
            'prom_ft': s['summit']['prom'],
            'elev_ft': s['summit']['elev'],
            'min_bound': s.get('min_bound', False),
            'geo': s['summit']['geo'],
        },
    }

class MapViewHandler(web.RequestHandler):
    def get(self, tag):
        with open(os.path.join('/home/drew/tmp/pvout', tag)) as f:
            geojson = json.load(f)
        
        summit = [k for k in geojson['features'] if k['properties']['type'] == 'summit'][0]
        children = [by_geo[childgeo] for childgeo in hierarchy[summit['properties']['geo']]]
        children.sort(key=lambda s: s['summit']['prom'], reverse=True)
        for i, child in enumerate(children):
            f = feature(child)
            f['properties']['type'] = 'child'
            f['properties']['order'] = i + 1
            geojson['features'].append(f)

        self.render('map.html', mode='single', data=geojson)

class SummitsHandler(web.RequestHandler):
    def get(self, prefix):
        max_n = int(self.get_argument('max', 1000))

        summits = filter(lambda e: e['summit']['geo'].startswith(prefix), alldata)
        summits = sorted(summits, key=lambda e: e['summit']['prom'], reverse=True)[:max_n]

        data = {
            'type': 'FeatureCollection',
            'features': [feature(s) for s in summits],
        }

        self.render('map.html', mode='many', data=data)

if __name__ == "__main__":

    parser = OptionParser()
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

    with open('/home/drew/tmp/pvout/prombackup') as f:
        alldata = json.load(f)
    by_geo = dict((p['summit']['geo'], p) for p in alldata)
    hierarchy = collections.defaultdict(set)
    for p in alldata:
        hierarchy[p['parent']['geo'] if 'parent' in p else None].add(p['summit']['geo'])

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')
