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

def lon_epoch(x):
    return int(math.floor(x / 360. + .5))

def normalize_epoch(bound, epoch):
    world = box(-180, -90, 180, 90)
    return translate(bound, -360 * epoch, 0).intersection(world)

def non_geo_bound(srs, x0, y0, x1, y1, tolerance):
    LL_EPSILON = 1e-7
    def make_tx(src, dst):
        transform = osr.CoordinateTransformation(src, dst)
        def tx(p):
            try:
                return list(transform.TransformPoint(*p)[:2])
            except:
                return [float('nan')]*2
        return tx
    geo = osr.SpatialReference()
    geo.ImportFromEPSG(4326)
    tx = make_tx(srs, geo)
    inv_tx = make_tx(geo, srs)
    midp = lambda p0, p1: [(a+b)/2. for a, b in zip(p0, p1)]
    dist = lambda p0, p1: sum((a-b)**2. for a, b in zip(p0, p1))**.5
    lonnorm = lambda x: (x + 180.) % 360. - 180.
    is_polar = lambda p: abs(p[1]) > 90. - LL_EPSILON
    is_lon_oppo = lambda p0, p1: abs(lonnorm(p0[0] - p1[0])) > 180. - LL_EPSILON
    
    def yield_seg(start, end):
        # progressively sub-divide a line segment until the midpoint in lat/lon space closely approximates
        # the midpoint in source projection space, with special handling for pole crossings
        mid = midp(start, end)
        llstart = tx(start)
        llend = tx(end)
        llmid = None
        polar = True
        pole_discontinuities = []
        # for pole crossings, use a lat/lon midpoint on a (possibly discontinuous) vertical line directly
        # to/from the pole. emit the segment to close the discontinuity as necessary
        if is_polar(llstart):
            llstart[0] = llend[0]
        elif is_polar(llend):
            llend[0] = llstart[0]
            pole_discontinuities = [llend]
        elif is_lon_oppo(llstart, llend):
            north = 1 if llstart[1] + llend[1] > 0 else -1
            lat_delta = (llstart[1] - llend[1]) * north / 2.
            llmid = ((llend if lat_delta > 0 else llstart)[0], north * (90 - abs(lat_delta)))
            pole_discontinuities = [(p[0], north * 90) for p in (llstart, llend)]
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
            for p in pole_discontinuities:
                yield p
        else:
            for p in yield_seg(start, mid):
                yield p
            for p in yield_seg(mid, end):
                yield p

    corners = [(x0, y0), (x0, y1), (x1, y1), (x1, y0)]
    segs = [(e, corners[(i + 1) % len(corners)]) for i, e in enumerate(corners)]
    poly = list(itertools.chain(*(yield_seg(*seg) for seg in segs)))

    def rel_append(poly, points):
        for p in points:
            # normalize for IDL wraparound by setting lon to +/- 180 of the previous point's lon
            last_lon = poly[-1][0]
            if is_lon_oppo(p, poly[-1]) and is_polar(p):
                # special handling for pole discontinuity segments - move in the direction of the original poly start point
                delta_lon = math.copysign(180, poly[0][0] - last_lon)
            else:
                delta_lon = lonnorm(p[0] - last_lon)
            poly.append((last_lon + delta_lon, p[1]))
    poly_wrapped = poly[:1]
    rel_append(poly_wrapped, poly[1:])

    # handle pole caps
    winding = int(round(abs(poly_wrapped[0][0] - poly_wrapped[-1][0]) / 360.))
    assert winding <= 1, 'double wind'
    if winding == 1:
        proj_np = inv_tx((0, 90))
        north = 1 if (x0 <= proj_np[0] <= x1) and (y0 <= proj_np[1] <= y1) else -1

        # repeat ring until we fully span a [-180,180] interval
        while True:
            rel_append(poly_wrapped, poly)
            min_epoch, max_epoch = [lon_epoch(f(p[0] for p in poly_wrapped)) for f in (min, max)]
            if max_epoch - min_epoch >= 2:
                break

        # add the pole cap and normalize lons
        poly_wrapped.extend((poly_wrapped[i][0], north * 90) for i in (-1, 0))
        return normalize_epoch(Polygon(poly_wrapped), min_epoch + 1)

    return Polygon(poly_wrapped)

def idl_normalize_bound(bound):
    xmin, _, xmax, _ = bound.bounds
    return reduce(lambda a, b: a.union(b), (normalize_epoch(bound, epoch) for epoch in xrange(lon_epoch(xmin), lon_epoch(xmax) + 1)))

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

    x0, y0 = transform_px(gt, 0, 0)
    x1, y1 = transform_px(gt, dem['width'], dem['height'])
    if grid['srs'] == 'epsg:4326':
        bound = box(x0, y0, x1, y1)
    else:
        tolerance = 0.1
        bound = non_geo_bound(srs, x0, y0, x1, y1, tolerance * grid['spacing'])
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
