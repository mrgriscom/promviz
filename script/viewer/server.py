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

class MapViewHandler(web.RequestHandler):
    def get(self, tag):
        with open(os.path.join('/tmp', tag)) as f:
            geojson = json.load(f)
        self.render('map.html', mode='single', data=geojson)

class SummitsHandler(web.RequestHandler):
    def get(self, prefix):
        max_n = int(self.get_argument('max', 1000))

        summits = filter(lambda e: e['summitgeo'].startswith(prefix), alldata)
        summits = sorted(summits, key=lambda e: e['prom'], reverse=True)[:max_n]

        def feature(s):
            return {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [s['summit'][1], s['summit'][0]],
                },
                'properties': {
                    'prom_ft': s['prom'] / .3048,
                    'elev_ft': s['elev'] / .3048,
                    'geo': s['summitgeo'],
                },
            }

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

    with open('/tmp/prombackup') as f:
        alldata = json.load(f)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')
