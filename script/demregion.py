from Polygon import *
import os.path

reg = '43.78696,-77.60742 45.33670,-75.25635 46.98025,-72.55371 48.06340,-70.18066 49.33944,-67.43408 49.53947,-65.14893 48.93693,-63.67676 47.78363,-61.10596 47.24941,-59.85352 46.10371,-59.15039 45.04248,-60.73242 43.27721,-65.45654 41.02964,-69.74121 40.22922,-73.23486 38.66836,-74.72900 38.73695,-75.47607 40.22922,-77.25586'

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

