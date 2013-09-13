import sys
import os.path
import tempfile
import shutil

DATADIR = '/mnt/ext/pvdata'
DOWNSAMPLE = 3
WIDTH = 6001

def srtm_tag(row, col):
    return 'srtm_%02d_%02d' % (col, row)

def file_tag(row, col):
    lon = (col - 1) * 5 - 180
    lat = 60 - 5 * row
    return '%s%02d%s%03d' % ('n' if lat >= 0 else 's', abs(lat), 'e' if lon >= 0 else 'w', abs(lon))

def dl_url(row, col):
    return 'http://srtm.csi.cgiar.org/SRT-ZIP/SRTM_V41/SRTM_Data_GeoTiff/%s.zip' % srtm_tag(row, col)

def dstpath(row, col, downsample=1):
    return os.path.join(DATADIR, file_tag(row, col) + ('ds%d' % downsample if downsample > 1 else ''))

def download(row, col):
    ziptmp = tempfile.mktemp()
    os.popen('wget -O %s "%s"' % (ziptmp, dl_url(row, col)))
    if not os.path.exists(ziptmp) or os.stat(ziptmp).st_size == 0:
        return False

    unzipdir = tempfile.mktemp()
    os.popen('unzip %s -d %s' % (ziptmp, unzipdir))
    os.remove(ziptmp)

    tiffpath = os.path.join(unzipdir, '%s.tif' % srtm_tag(row, col))
    os.popen('convert %s -depth 16 gray:%s' % (tiffpath, dstpath(row, col)))
    shutil.rmtree(unzipdir)

    return True

def downsample(row, col):
    with open(dstpath(row, col), 'rb') as in_:
        with open(dstpath(row, col, DOWNSAMPLE), 'wb') as out:
            r, c = 0, 0
            while True:
                p = in_.read(2)
                if not p:
                    break

                if r % DOWNSAMPLE == 0 and c % DOWNSAMPLE == 0:
                    out.write(p)

                c += 1
                if c == WIDTH:
                    c = 0
                    r += 1

def process(row, col):
    if not os.path.exists(dstpath(row, col)):
        exists = download(row, col)
        if exists and DOWNSAMPLE > 1:
            downsample(row, col)

if __name__ == "__main__":
    rowmin = int(sys.argv[1])
    colmin = int(sys.argv[2])
    try:
        rowmax = int(sys.argv[3])
        colmax = int(sys.argv[4])
    except IndexError:
        rowmax = rowmin
        colmax = colmin

    for row in range(rowmin, rowmax + 1):
        for col in range(colmin, colmax + 1):
            process(row, col)

