from Polygon import *
import os.path

reg = '39.33430,-73.74023 44.99588,-76.15723 48.66194,-70.00488 49.63918,-66.44531 49.18170,-63.28125 47.12995,-59.72168 45.30580,-59.23828 40.78054,-68.95020'

def quadrant(xmin, xmax, ymin, ymax):
    """generate a polygon for the rectangle with the given bounds"""
    return Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

def cells(poly, res):
    for lon in xrange(-180, 180, res):
        for lat in xrange(-90, 90, res):
            cell = quadrant(lat, lat+res, lon, lon+res)
            if cell.overlaps(poly):
                yield (lat, lon)

def fmtll(lat, lon):
    return '%s%02d%s%03d' % ('N' if lat >= 0 else 'S', abs(lat), 'E' if lon >= 0 else 'W', abs(lon))

if __name__ == "__main__":

    res = 1
    poly = Polygon([[float(f) for f in p.split(',')] for p in reg.split()])
    for cell in cells(poly, res):
        lat, lon = cell
        path = '/mnt/ext/gis/ferranti/hgts/%s.hgt' % fmtll(lat, lon)
        if os.path.exists(path):
            print 'dm.DEMs.add(new SRTMDEM("%s", 1201, 1201, %s, %s, 1));' % (path, lat, lon)

