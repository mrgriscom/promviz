import os
import math
import glob
from osgeo import gdal,ogr,osr
from pprint import pprint

dems = (glob.glob('/home/drew/nobk/gis/ferranti/zipped/*.gz') +
        glob.glob('/home/drew/nobk/gis/gmted/*.tif') +
        [
            '/home/drew/nobk/gis/tmp/ned/n42w073/floatn42w073_13.flt',
            '/home/drew/Downloads/10m_Grid_GeoTiff/10m_BA.tif',
        ])

epsg_remaps = {
    4269: 4326,  # geographic nad83 -> wgs84
}

def transform_px(transform, px, py):
    tx = transform
    return (tx[0] + tx[1]*px + tx[2]*py,
            tx[3] + tx[4]*px + tx[5]*py)

GRIDS = []
for dempath in sorted(dems):
    grid = {}
    dem = {'path': dempath}

    raster= ('/vsigzip/' if dempath.endswith('.gz') else '') + dempath
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
    
    width = ds.RasterXSize
    height = ds.RasterYSize
    dw, dh = (dy, dx) if flip else (dx, dy)
    dem['width'] = width * (-1 if dw < 0 else 1)
    dem['height'] = height * (-1 if dh < 0 else 1)

    x0, y0 = transform_px(gt, .5, height - .5)
    SUBPX_PREC = 4
    to_grid = lambda t, stretch=1.: round(t / (grid['spacing'] * stretch), SUBPX_PREC)
    grid0 = (to_grid(x0, x_stretch), to_grid(y0))
    grid['subpx_offset'] = [round(k % 1., SUBPX_PREC) for k in grid0]
    dem['origin'] = [int(math.floor(k)) for k in grid0]

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
        
    pprint(dem)
    print

pprint(list(enumerate(GRIDS)))
