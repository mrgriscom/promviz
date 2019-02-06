import os
import sys
import math
import glob
import os.path
import itertools
from osgeo import gdal, ogr, osr
from shapely.geometry import Polygon, box
from shapely.affinity import translate
from pprint import pprint
import json

osr.UseExceptions()

filesets = [
    ('/home/drew/nobk/gis/ferranti/zipped/', '*.gz', [], 'ferranti3'),
    ('/home/drew/nobk/gis/gmted/', '*.tif', [], 'gmted'),
    ('/home/drew/nobk/gis/tmp/ned/', '*/*.flt', ['prj', 'hdr'], 'ned1-3'),
    ('/home/drew/Downloads/10m_Grid_GeoTiff/', '10m_BA.tif', [], 'misc-cpt'),
]

epsg_remaps = {
    4269: 4326,  # geographic nad83 -> wgs84
}

def transform_px(transform, px, py):
    tx = transform
    return (tx[0] + tx[1]*px + tx[2]*py,
            tx[3] + tx[4]*px + tx[5]*py)

def non_geo_bound(srs, x0, y0, x1, y1, tolerance):
    LL_EPSILON = 1e-5
    geo = osr.SpatialReference()
    geo.ImportFromEPSG(4326)
    def make_tx(src, dst):
        transform = osr.CoordinateTransformation(src, dst)
        def tx(p):
            try:
                return list(transform.TransformPoint(*p)[:2])
            except:
                return None
        return tx
    tx = make_tx(srs, geo)
    inv_tx = make_tx(geo, srs)
    midp = lambda p0, p1: [(a+b)/2. for a, b in zip(p0, p1)]
    dist = lambda p0, p1: sum((a-b)**2. for a, b in zip(p0, p1))**.5 if p0 and p1 else float('+inf')
    lonnorm = lambda x: (x + 180.) % 360. - 180.
    is_polar = lambda p: abs(p[1]) > 90. - LL_EPSILON
    is_antip = lambda p0, p1: abs(lonnorm(p0[0] - p1[0])) > 180. - LL_EPSILON
    
    def yield_seg(start, end):
        # progressively sub-divide a line segment until the midpoint in lat/lon space closely approximates
        # the midpoint in source projection space, with special handling for pole crossings
        mid = midp(start, end)
        llstart = tx(start)
        llend = tx(end)
        llmid = None
        polar = True
        extra_points = []
        # for pole crossings, use a lat/lon midpoint on a (possibly discontinuous) vertical line directly
        # to/from the pole. emit the segment to close the discontinuity as necessary
        if is_polar(llstart):
            llstart[0] = llend[0]
        elif is_polar(llend):
            llend[0] = llstart[0]
            extra_points = [llend]
        elif is_antip(llstart, llend):
            sign = 1 if llstart[1] + llend[1] > 0 else -1
            lat_delta = (llstart[1] - llend[1]) * sign / 2.
            llmid = ((llend if lat_delta > 0 else llstart)[0], sign * (90 - abs(lat_delta)))
            extra_points = [(llstart[0], sign * 90), (llend[0], sign * 90)]
        else:
            polar = False
        # don't compute lat/lon midpoint if already computed with special logic above
        if not llmid:
            llmid = midp(llstart, llend)
        # alternative midpoint if there is a shorter connecting segment that crosses the IDL
        llmid_idl = (lonnorm(llmid[0] + 180.), llmid[1])

        if (dist(start, end) < tolerance or
            dist(mid, inv_tx(llmid)) < tolerance or
            (not polar and dist(mid, inv_tx(llmid_idl)) < tolerance)):
            yield llstart
            for ep in extra_points:
                yield ep
        else:
            for p in yield_seg(start, mid):
                yield p
            for p in yield_seg(mid, end):
                yield p

    corners = [(x0, y0), (x0, y1), (x1, y1), (x1, y0)]
    segs = [(e, corners[(i + 1) % len(corners)]) for i, e in enumerate(corners)]
    poly = list(itertools.chain(*(yield_seg(*seg) for seg in segs)))

    poly_wrapped = poly[:1]
    for p in poly[1:]:
        # normalize for IDL wraparound by setting lon to +/- 180 of the previous point's lon
        last_lon = poly_wrapped[-1][0]
        delta_lon = lonnorm(p[0] - last_lon)
        # special handling for pole discontinuity segments - move in the direction of the original poly start point
        if abs(delta_lon) > 180. - LL_EPSILON and is_polar(p):
            delta_lon = math.copysign(180, poly_wrapped[0][0] - last_lon)
        poly_wrapped.append((last_lon + delta_lon, p[1]))

    # handle pole caps
    winding = int(round(abs(poly_wrapped[0][0] - poly_wrapped[-1][0]) / 360.))
    assert winding <= 1, 'double wind'
    if winding == 1:
        poly_wrapped = [(lonnorm(p[0]), p[1]) for p in poly_wrapped]
        prev_ix = lambda i: (i-1) % len(poly_wrapped)
        for i in xrange(len(poly_wrapped)):
            if abs(poly_wrapped[i][0] - poly_wrapped[prev_ix(i)][0]) > 180.:
                discont_ix = i
                break
        start = poly_wrapped[prev_ix(discont_ix)]
        end = poly_wrapped[discont_ix]
        lon_k = lonnorm(180 - start[0]) / lonnorm(end[0] - start[0])
        cross_lat = start[1] * (1 - lon_k) + end[1] * lon_k
        sign = math.copysign(1, start[0])
        pole_lat = math.copysign(90, cross_lat) # not 100% robust
        poly_wrapped[discont_ix:discont_ix] = [(sign*180, cross_lat), (sign*180, pole_lat), (-sign*180, pole_lat), (-sign*180, cross_lat)]

    return Polygon(poly_wrapped)

def idl_normalize_bound(bound):
    def slices():
        world = box(-180, -90, 180, 90)
        xmin, _, xmax, _ = bound.bounds
        to_epoch = lambda x: int(math.floor(x / 360. + .5))
        for epoch in xrange(to_epoch(xmin), to_epoch(xmax) + 1):
            yield translate(bound, -360 * epoch, 0).intersection(world)
    return reduce(lambda a, b: a.union(b), slices())

def process_series(root, _glob, sidecars, series):
    for path in sorted(glob.glob(os.path.join(root, _glob))):
        yield process_dem(root, path, sidecars, series)

GRIDS = []
def process_dem(root, path, sidecars, series):
    def to_key(local_path):
        assert os.path.exists(local_path)
        return os.path.join(series, os.path.relpath(local_path, root))
    
    grid = {}
    dem = {'path': to_key(path)}

    sidecars = ['%s.%s' % (os.path.splitext(path)[0], ext) for ext in sidecars]
    if sidecars:
        dem['sidecars'] = map(to_key, sidecars)
    
    raster= ('/vsigzip/' if path.endswith('.gz') else '') + path
    ds=gdal.Open(raster)

    srs=osr.SpatialReference()
    srs.ImportFromWkt(ds.GetProjection())
    try:
        srs.AutoIdentifyEPSG()
        epsg = int(srs.GetAuthorityCode(None))
        epsg = epsg_remaps.get(epsg, epsg)
        grid['srs'] = 'epsg:%d' % epsg
    except:
        grid['srs'] = 'proj:' + srs.ExportToProj4()
        
    gt = ds.GetGeoTransform()
    if gt[2] == 0 and gt[4] == 0:
        dx = gt[1]
        dy = -gt[5]
        flip = False
    elif gt[1] == 0 and gt[5] == 0:
        dy = gt[2]
        dx = -gt[4]
        flip = True
    else:
        assert False, 'grid is rotated or skewed'

    grid['spacing'] = abs(dy)
    x_stretch = abs(dx) / grid['spacing']
    if x_stretch != 1:
        grid['x_stretch'] = x_stretch
    if flip:
        dem['flip_xy'] = flip
    if dx < 0:
        dem['inv_x'] = True
    if dy < 0:
        dem['inv_y'] = True
    
    dem['width'] = ds.RasterXSize
    dem['height'] = ds.RasterYSize

    x0, y0 = transform_px(gt, .5, dem['height'] - .5)
    SUBPX_PREC = 4
    to_grid = lambda t, stretch=1.: round(t / (grid['spacing'] * stretch), SUBPX_PREC)
    grid0 = (to_grid(x0, x_stretch), to_grid(y0))
    grid['subpx_offset'] = [round(k % 1., SUBPX_PREC) for k in grid0]
    dem['origin'] = [int(math.floor(k)) for k in grid0]

    # TODO handle bounds for other projections -- include 1/2 pixel buffer and subdivide lines until projection error is < epsilon*pixel
    x0, y0 = transform_px(gt, 0, 0)
    x1, y1 = transform_px(gt, dem['width'], dem['height'])
    if grid['srs'] == 'epsg:4326':
        bound = box(x0, y0, x1, y1)
    else:
        bound = non_geo_bound(srs, x0, y0, x1, y1, 0.1 * grid['spacing'])
    dem['bound'] = idl_normalize_bound(bound).wkt
        
    band = ds.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    if nodata:
        dem['nodata'] = nodata
    unit = band.GetUnitType()
    if not unit or unit in ('m', 'metre', 'meter'):
        zunit = 1.
    elif unit in ('ft', 'foot', 'feet'):
        zunit = .3048
    if zunit != 1:
        dem['z_unit'] = zunit

    try:
        grid_id = GRIDS.index(grid)
    except ValueError:
        grid_id = len(GRIDS)
        GRIDS.append(grid)
    dem['grid_id'] = grid_id

    sys.stderr.write(path + '\n')
    return dem

if __name__ == '__main__':
    DEMS = list(itertools.chain(*(process_series(*fs) for fs in filesets)))
    for i, grid in enumerate(GRIDS):
        grid['id'] = i
    print json.dumps({'dems': DEMS, 'grids': GRIDS}, sort_keys=True, indent=4)
