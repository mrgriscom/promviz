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
import sqlite3
import geojson
import shapely.wkt
import shapely.geometry
import itertools

import os.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import settings
import util

def fetch_prom(where_clause, args):
    results = conn.execute('select point, saddle, prom_mm, min_bound, prom_parent, line_parent, prom_rank, AsWkt(thresh_path), AsWkt(parent_path), AsWkt(domain) from prom where %s' % where_clause, args)
    def to_rec(row):
        return {
            'point': geo_itos(row[0]),
            'saddle': geo_itos(row[1]),
            'prom': row[2]/1000.,
            'min_bound': bool(row[3]),
            'parent': tx(geo_itos, row[4]),
            'pthresh': tx(geo_itos, row[5]),
            'prom_rank': row[6],
            'thresh_path': row[7],
            'parent_path': row[8],
            'domain': row[9],
        }
    return map(to_rec, results)

def fetch_points(ids):
    ids = set(ids)
    results = conn.execute('select geocode, type, elev_mm, AsWkt(loc) from points where geocode in (%s)' % ','.join(['?']*len(ids)), map(geo_stoi, ids))
    def to_rec(row):
        return {
            'geo': geo_itos(row[0]),
            'type': {1: 'peak', -1: 'pit', 0: 'saddle'}[row[1]],
            'elev': row[2]/1000.,
            'coords': row[3],
        }
    return dict((rec['geo'], rec) for rec in map(to_rec, results))

def fetch_subsaddles(point):
    results = conn.execute('select saddle, is_elev, is_prom from subsaddles where point = ?', (geo_stoi(point),))
    def to_rec(row):
        return {
            'saddle': geo_itos(row[0]),
            'elev': bool(row[1]),
            'prom': bool(row[2]),
        }
    return map(to_rec, results)

def build_prom_info(proms):
    proms = list(proms)
    return {
        'proms': dict((p['point'], p) for p in proms),
        'proms_by_saddle': dict((p['saddle'], p) for p in proms),
        'points': fetch_points(prom[k] for k in ('point', 'saddle') for prom in proms),
    }

def _feature(coords, **props):
    return {
        'type': 'Feature',
        'geometry': geojson.Feature(geometry=shapely.wkt.loads(coords)).geometry,
        'properties': props,
    }

def summit_feature(info, geo, **props):
    prom = info['proms'][geo]
    point = info['points'][geo]

    _props = {
        'type': point.get('type'),
        #'name': s.get('name'),
        'prom_m': prom['prom'],
        'elev_m': point['elev'],
        'prom_ft': prom['prom'] / .3048,
        'elev_ft': point['elev'] / .3048,
        'min_bound': prom.get('min_bound', False),
        'geo': geo,
    }
    _props.update(props)
    return _feature(point['coords'], **_props)

def saddle_feature(info, geo, type='saddle', ref_peak=False, **props):
    point = info['points'][geo]
    prom = info['proms_by_saddle'][geo]
    
    if ref_peak:
        props['peak'] = summit_feature(info, prom['point'], type='sspeak')
    props.update({
        'type': type,
        #'name': s.get('name'),
        'elev_m': point['elev'],
        'elev_ft': point['elev'] / .3048,
        'geo': geo,
    })
    return _feature(point['coords'], **props)

def geo_stoi(s):
    i = int(s, 16)
    if i >= 2**63:
        i -= 2**64
    return i

def geo_itos(i):
    if i < 0:
        i += 2**64
    return '%016x' % i

def tx(func, val):
    return func(val) if val is not None else None

class MapViewHandler(web.RequestHandler):
    def get(self, tag):
        main = fetch_prom('point = ?', (geo_stoi(tag),))[0]
        parents = fetch_prom('point in (?,?)', (tx(geo_stoi, main['parent']), tx(geo_stoi, main['pthresh'])))
        children = fetch_prom('prom_parent = ?', (geo_stoi(tag),))
        subsaddle_peaks = fetch_prom('saddle in (select saddle from subsaddles where point = ?)', (geo_stoi(tag),))

        info = build_prom_info(itertools.chain([main], parents, children, subsaddle_peaks))
        subsaddles = fetch_subsaddles(tag)

        data = {
            'type': 'FeatureCollection',
            'features': [
                summit_feature(info, tag, type='peak'),
                saddle_feature(info, main['saddle']),
                _feature(main['thresh_path'], type='divide'),
                _feature(main['parent_path'], type='toparent'),
            ]
        }
        if not main['min_bound']:
            threshold_point = shapely.geometry.Point(shapely.wkt.loads(main['thresh_path']).coords[-1])
            data['features'].append(_feature(shapely.wkt.dumps(threshold_point), type='threshold'))
        for ss in subsaddles:
            if ss['elev']:
                data['features'].append(saddle_feature(info, ss['saddle'], 'subsaddle', ref_peak=True, domain=False))
            if ss['prom']:
                data['features'].append(saddle_feature(info, ss['saddle'], 'subsaddle', ref_peak=True, domain=True))    
        if main['parent'] is not None:
            data['features'].append(
                summit_feature(info, main['parent'], type='parent'),
            )
        for i, ch in enumerate(sorted(children, key=lambda ch: ch['prom_rank'])):
            ix = i + 1
            data['features'].extend([
                summit_feature(info, ch['point'], type='child', ix=ix),
                # better to replace with child paths to parent + child domains
                saddle_feature(info, ch['saddle'], type='childsaddle', ix=ix),
                _feature(ch['parent_path'], type='tochild'),
            ])
            if ch['domain'] is not None:
                data['features'].append(_feature(ch['domain'], type='child-domain'))
        if main['pthresh'] is not None:
            data['features'].append(
                summit_feature(info, main['pthresh'], type='pthresh'),
            )
        if main['domain'] is not None:
            data['features'].append(_feature(main['domain'], type='domain'))
            
        self.render('map.html', mode='single', data=data)

class SummitsHandler(web.RequestHandler):
    def initialize(self, mode):
        self.mode = mode

    def get(self, prefix):
        max_n = int(self.get_argument('max', 5000))

        def hex_limit(prefix, upper):
            if not prefix:
                return 2**63 - 1 if upper else -2**63
            
            dec = int((prefix + (('f' if upper else '0')*16))[:16], 16)
            if dec >= 2**63:
                dec -= 2**64
            return dec

        geo_min = hex_limit(prefix, False)
        geo_max = hex_limit(prefix, True)
        
        results = conn.execute('''
          select point, AsWkt(loc), elev_mm, prom_mm, min_bound
          from prom join points on (prom.point = points.geocode)
          where point between ? and ? and type = ?
          order by prom_mm desc
          limit ?;
        ''', (geo_min, geo_max, {'up': 1, 'down': -1}[self.mode], max_n))
        results = list(results)
        
        def to_prom(row): 
            return {
                'point': geo_itos(row[0]),
                'prom': row[3] / 1000.,
                'min_bound': bool(row[4]),
            }
        def to_point(row):
            return {
                'geo': geo_itos(row[0]),
                'coords': row[1],
                'elev': row[2] / 1000.,
            }
        info = {
            'proms': dict((e['point'], e) for e in map(to_prom, results)),
            'points': dict((e['geo'], e) for e in map(to_point, results)),
        }
        if results:
            print 'threshold: %s' % to_prom(results[-1])['prom']

        data = {
            'type': 'FeatureCollection',
            'features': [summit_feature(info, geo) for geo in info['proms'].keys()],
        }

        self.render('map.html', mode='many', data=data)

if __name__ == "__main__":

    parser = OptionParser()
    (options, args) = parser.parse_args()

    dbpath = args[0]
    
    try:
        port = int(args[1])
    except IndexError:
        port = 8000

    conn = sqlite3.connect(dbpath)
    conn.enable_load_extension(True)
    conn.load_extension("mod_spatialite.so")
    
    application = web.Application([
        (r'/view/(?P<tag>.*)', MapViewHandler),
        (r'/summits/(?P<prefix>[0-9a-f]*)', SummitsHandler, {'mode': 'up'}),
        (r'/sinks/(?P<prefix>[0-9a-f]*)', SummitsHandler, {'mode': 'down'}),
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
