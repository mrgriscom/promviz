from Polygon import *
import os.path
import sys
import requests
import settings

# north america mainland
#reg = '34.93999,-74.79492 30.90222,-80.22217 25.60190,-79.36523 24.26700,-80.33203 23.90593,-82.22168 28.57487,-84.77051 27.39128,-95.71289 21.06400,-96.10840 19.82873,-92.85645 22.02455,-90.02197 22.14671,-85.60547 17.39258,-86.48438 16.23577,-81.69434 10.79014,-82.08984 10.01213,-76.68457 7.36247,-76.86035 6.33714,-78.70605 6.79554,-83.32031 11.84585,-90.15381 15.17818,-100.67871 20.13847,-107.60010 24.02640,-113.46680 30.05008,-118.03711 34.36158,-122.27783 39.11301,-125.33203 44.77794,-126.36475 48.26857,-127.08984 52.50953,-133.19824 56.48676,-137.04346 58.12432,-139.65820 58.82512,-148.05176 55.60318,-152.79785 54.25239,-159.87305 52.84259,-166.88232 54.68653,-167.29980 57.51582,-161.41113 58.91599,-168.09082 60.33782,-168.31055 63.29294,-166.42090 65.30265,-168.88184 66.00015,-168.57422 67.12729,-166.46484 68.35870,-167.67334 69.31832,-167.34375 71.20192,-160.29053 71.74643,-156.42334 70.34093,-141.30615 70.16275,-134.14307 70.94536,-128.29834 70.00557,-120.41016 69.11561,-114.85107 68.33438,-109.90723 69.12344,-106.08398 68.21237,-100.30518 68.86352,-97.91016 69.13127,-96.06445 70.64177,-97.51465 71.85623,-96.63574 72.20196,-96.21826 72.19525,-92.98828 69.27171,-87.09961 70.28912,-85.91309 70.09553,-81.56250 62.91523,-79.10156 62.93523,-72.90527 60.73769,-63.98438 55.77657,-57.52441 52.45601,-53.96484 47.42809,-51.67969 45.73686,-53.08594 43.42101,-63.58887 38.95941,-72.90527'

# northeast us
#reg = '43.78696,-77.60742 45.33670,-75.25635 46.98025,-72.55371 48.06340,-70.18066 49.33944,-67.43408 49.53947,-65.14893 48.93693,-63.67676 47.78363,-61.10596 47.24941,-59.85352 46.10371,-59.15039 45.04248,-60.73242 43.27721,-65.45654 41.02964,-69.74121 40.22922,-73.23486 38.66836,-74.72900 38.73695,-75.47607 40.22922,-77.25586'

# eastern us
#reg = '39.87602,-73.12500 41.24477,-68.11523 45.39845,-58.93066 47.51720,-60.02930 49.12422,-63.85254 49.69606,-66.48926 48.31243,-70.31250 46.01222,-74.04785 46.86019,-78.22266 46.67206,-81.33179 46.92026,-84.47388 47.62838,-87.62695 47.09257,-92.59277 44.53567,-93.69141 42.17969,-91.50513 40.07807,-92.26318 36.78289,-90.32959 33.48644,-92.05444 29.28640,-92.26318 28.65203,-89.07715 23.83560,-83.43018 24.60707,-79.40918 31.12820,-78.96973 34.45222,-74.48730'

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

def s3url(tag):
    return settings.s3_server + '/' + tag

def s3fetch(tag):
    return requests.get(s3url(tag)).text

def load_index():
    return set(s3fetch('index').split())

if __name__ == "__main__":

    region = sys.argv[1]
    all_tiles = load_index()

    res = 1
    poly = Polygon([[float(f) for f in p.split(',')] for p in region.split()])
    for cell in cells(poly, res):
        lat, lon = cell
        tag = fmtll(lat, lon)
        if tag not in all_tiles:
            continue

        path = os.path.join(settings.dir_dem, '%s.hgt' % tag)
        if not os.path.exists(path):
            os.popen('curl -s "%s" | gunzip > %s' % (s3url('%s.hgt.gz' % tag), path))
            sys.stderr.write('downloaded %s\n' % tag)
        print 'srtm,%s,1201,1201,%s,%s,1' % (path, lat, lon)

