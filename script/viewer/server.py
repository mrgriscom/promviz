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
        self.render('map.html', data=geojson)


if __name__ == "__main__":

    parser = OptionParser()
    (options, args) = parser.parse_args()

    try:
        port = int(args[0])
    except IndexError:
        port = 8000

    application = web.Application([
        (r'/view/(?P<tag>.*)', MapViewHandler),
        (r'/(.*)', web.StaticFileHandler, {'path': 'static'}),
    ], template_path='templates', debug=True)
    application.listen(port)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
        raise

    logging.info('shutting down...')
