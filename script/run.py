import sys
import subprocess
from shapely.geometry import Polygon, box
import index_dems
from datetime import datetime

regions = {
    'north-america': '34.93999,-74.79492 30.90222,-80.22217 25.60190,-79.36523 24.26700,-80.33203 23.90593,-82.22168 28.57487,-84.77051 27.39128,-95.71289 21.06400,-96.10840 19.82873,-92.85645 22.02455,-90.02197 22.14671,-85.60547 17.39258,-86.48438 16.23577,-81.69434 10.79014,-82.08984 10.01213,-76.68457 7.36247,-76.86035 6.33714,-78.70605 6.79554,-83.32031 11.84585,-90.15381 15.17818,-100.67871 20.13847,-107.60010 24.02640,-113.46680 30.05008,-118.03711 34.36158,-122.27783 39.11301,-125.33203 44.77794,-126.36475 48.26857,-127.08984 52.50953,-133.19824 56.48676,-137.04346 58.12432,-139.65820 58.82512,-148.05176 55.60318,-152.79785 54.25239,-159.87305 52.84259,-166.88232 54.68653,-167.29980 57.51582,-161.41113 58.91599,-168.09082 60.33782,-168.31055 63.29294,-166.42090 65.30265,-168.88184 66.00015,-168.57422 67.12729,-166.46484 68.35870,-167.67334 69.31832,-167.34375 71.20192,-160.29053 71.74643,-156.42334 70.34093,-141.30615 70.16275,-134.14307 70.94536,-128.29834 70.00557,-120.41016 69.11561,-114.85107 68.33438,-109.90723 69.12344,-106.08398 68.21237,-100.30518 68.86352,-97.91016 69.13127,-96.06445 70.64177,-97.51465 71.85623,-96.63574 72.20196,-96.21826 72.19525,-92.98828 69.27171,-87.09961 70.28912,-85.91309 70.09553,-81.56250 62.91523,-79.10156 62.93523,-72.90527 60.73769,-63.98438 55.77657,-57.52441 52.45601,-53.96484 47.42809,-51.67969 45.73686,-53.08594 43.42101,-63.58887 38.95941,-72.90527',
    'northeast-us': '43.78696,-77.60742 45.33670,-75.25635 46.98025,-72.55371 48.06340,-70.18066 49.33944,-67.43408 49.53947,-65.14893 48.93693,-63.67676 47.78363,-61.10596 47.24941,-59.85352 46.10371,-59.15039 45.04248,-60.73242 43.27721,-65.45654 41.02964,-69.74121 40.22922,-73.23486 38.66836,-74.72900 38.73695,-75.47607 40.22922,-77.25586',
    'africa': '38.06539,11.51367 36.77409,-5.88867 27.21556,-15.24902 21.90228,-18.32520 11.43696,-17.70996 2.94304,-7.51465 -37.43997,18.45703 -34.74161,30.05859 -26.74561,48.69141 -13.41099,52.55859 13.75272,53.26172 16.46769,41.04492 32.62087,35.50781',
    'western-us': '48.90806,-123.35449 48.89362,-112.93945 45.84411,-108.30322 44.22946,-102.98584 40.81381,-101.16211 37.94420,-101.66748 34.37971,-101.95313 30.69461,-104.52393 31.54109,-108.85254 31.18461,-113.79639 31.24099,-117.39990 37.38762,-123.13477 43.22920,-125.39795',
    'high-asia': '29.15216,75.84961 26.58853,82.48535 25.56227,88.72559 20.79720,91.31836 15.70766,93.29590 14.85985,97.08618 13.42168,100.26123 13.51784,103.31543 18.41708,106.36963 20.17972,107.40234 25.75042,111.89575 28.98892,116.24634 31.45678,117.31201 34.74161,114.69727 39.30030,116.89453 39.36828,121.02539 44.84029,123.83789 54.34855,128.74878 59.66774,122.95898 61.60640,117.59766 61.27023,107.66602 60.71620,101.60156 59.17593,94.74609 57.23150,83.67188 51.86292,78.66211 52.69636,75.76172 54.67383,68.77441 51.20688,62.27051 47.57653,57.08496 45.85941,49.57031 48.34165,39.11133 47.27923,34.36523 43.48481,37.22168 41.86956,40.38574 42.81152,33.75000 41.24477,25.66406 38.34166,25.40039 35.67515,27.33398 34.37971,33.22266 34.08906,35.04639 32.73184,34.69482 32.26856,35.52979 33.35806,36.60645 34.08906,39.81445 35.40696,41.74805 30.88337,48.36182 26.58853,51.15234 25.56227,54.79980 26.37219,56.46973 25.02588,57.48047 24.12670,61.17188 23.64452,67.76367',
    'satest': '-33.39476,17.64954 -34.60156,18.20984 -35.12440,19.91272 -34.37064,23.74695 -34.18909,26.12000 -32.99024,28.29529 -31.01057,30.58594 -29.95018,31.18469 -27.82450,30.21240 -26.98083,26.98242 -30.72295,20.86304 -29.89781,19.11621 -30.43447,17.10571',
}

pipeline = (
#    'Full'
#    'TopologyNetwork'
#    'Prominence'
    'Paths'
)
bound = regions.get(sys.argv[2], sys.argv[2])
series = sys.argv[1]
if pipeline in ('Full', 'TopologyNetwork'):
    timestamp = datetime.now().strftime('%Y%m%d%H%M')
    print timestamp
else:
    timestamp = sys.argv[3]

def bound_to_wkt(bound):
    if bound.startswith(':'):
        return bound
    elif bound.startswith('box:'):
        x0, x1, y0, y1 = map(float, bound[len('box:'):].split(','))
        coords = list(box(x0, y0, x1, y1).exterior.coords)[:-1]
    else:
        coords = [tuple(reversed(map(float, p.split(',')))) for p in filter(None, bound.split(' '))]
    coords = index_dems.relativize_poly(coords)
    return index_dems.idl_normalize_bound(Polygon(coords)).wkt
bound_wkt = bound_to_wkt(bound)

subprocess.call(r"""export GOOGLE_APPLICATION_CREDENTIALS=private/credentials/prominence-0e0ffcfbf903.json && \
    mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.mrgris.prominence.%(pipeline)s \
	-Dexec.args="--project=prominence-163319 \
          --gcpTempLocation=gs://mrgris-promviz/tmp/ \
          --stagingLocation=gs://mrgris-promviz/staging/ \
          --runner=DataflowRunner \
          --numWorkers=%(workers)s \
          --bound='%(bound)s' \
          --series=%(series)s \
        "
""" % {
    'bound': bound_wkt,
    'series': series,
    'pipeline': '%sPipeline' % pipeline,
    'timestamp': timestamp,
    'workers': 20,
}, shell=True)
