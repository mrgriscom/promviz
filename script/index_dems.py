import os
import sys
import math
import glob
import os.path
import itertools
from osgeo import gdal,ogr,osr
from shapely.geometry import Polygon, box
from pprint import pprint
import json

filesets = [
    ('/home/drew/nobk/gis/ferranti/zipped/', '*.gz', 'ferranti3'),
    ('/home/drew/nobk/gis/gmted/', '*.tif', 'gmted'),
    ('/home/drew/nobk/gis/tmp/ned/', '*/*.flt', 'ned1-3'),
    ('/home/drew/Downloads/10m_Grid_GeoTiff/', '10m_BA.tif', 'misc-cpt'),
]

epsg_remaps = {
    4269: 4326,  # geographic nad83 -> wgs84
}

def transform_px(transform, px, py):
    tx = transform
    return (tx[0] + tx[1]*px + tx[2]*py,
            tx[3] + tx[4]*px + tx[5]*py)

def process_series(root, _glob, series):
    for path in sorted(glob.glob(os.path.join(root, _glob))):
        yield process_dem(os.path.join(series, os.path.relpath(path, root)), path)

GRIDS = []
def process_dem(key, path):
    grid = {}
    dem = {'path': key}

    raster= ('/vsigzip/' if path.endswith('.gz') else '') + path
    ds=gdal.Open(raster)

    srs=osr.SpatialReference()
    srs.ImportFromWkt(ds.GetProjection())
    srs.AutoIdentifyEPSG()
    epsg = srs.GetAuthorityCode(None)
    if epsg:
        epsg = int(epsg)
        epsg = epsg_remaps.get(epsg, epsg)
        grid['srs'] = 'epsg:%d' % epsg
    else:
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
    if grid['srs'] == 'epsg:4326':
        x0, y0 = transform_px(gt, 0, 0)
        x1, y1 = transform_px(gt, dem['width'], dem['height'])
        bound = box(x0, y0, x1, y1)
        dem['bound'] = bound.wkt
        
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
