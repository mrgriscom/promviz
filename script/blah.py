import os
import json
import sys
import tempfile

raw = os.popen('/usr/lib/jvm/java-6-openjdk/bin/java -Xms2400m -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar promviz.DEMManager %s' % ' '.join(sys.argv[1:])).readlines()
#raw = open('/tmp/debug.output').readlines()

data = json.loads('[%s]' % ', '.join(raw))

data.sort(key=lambda e: e['prom'], reverse=True)

def to_link(k):
    print '<a target="_blank" href="http://mapper.acme.com/?ll=%(peaklat)f,%(peaklon)f&z=12&marker0=%(peaklat)f,%(peaklon)f,%(elev).1f&marker1=%(saddlelat)f,%(saddlelon)f,%(saddleelev).1f">%(peakgeo)s %(saddlegeo)s %(prom).1f%(minbound)s</a><br>' % {
        'peaklat': k['summit'][0],
        'peaklon': k['summit'][1],
        'saddlelat': k['saddle'][0],
        'saddlelon': k['saddle'][1],
        'elev':  k['elev'] / .3048,
        'saddleelev': (k['elev'] - k['prom']) / .3048,
        'prom': k['prom'] / .3048,
        'minbound': '*' if k['min_bound'] else '',
        'peakgeo': k['summitgeo'],
        'saddlegeo': k['saddlegeo'],
    }

def to_kml(k):
    content = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
<Document>
	<name>promviz</name>
	<StyleMap id="msn_ylw-pushpin">
		<Pair>
			<key>normal</key>
			<styleUrl>#sn_ylw-pushpin</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#sh_ylw-pushpin</styleUrl>
		</Pair>
	</StyleMap>
	<Style id="sh_blue-pushpin">
		<IconStyle>
			<scale>1.3</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/blue-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
		<ListStyle>
		</ListStyle>
	</Style>
	<Style id="sn_ylw-pushpin">
		<IconStyle>
			<scale>1.1</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<Style id="sh_ylw-pushpin">
		<IconStyle>
			<scale>1.3</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/ylw-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
	</Style>
	<Style id="sn_blue-pushpin">
		<IconStyle>
			<scale>1.1</scale>
			<Icon>
				<href>http://maps.google.com/mapfiles/kml/pushpin/blue-pushpin.png</href>
			</Icon>
			<hotSpot x="20" y="2" xunits="pixels" yunits="pixels"/>
		</IconStyle>
		<ListStyle>
		</ListStyle>
	</Style>
	<StyleMap id="msn_blue-pushpin">
		<Pair>
			<key>normal</key>
			<styleUrl>#sn_blue-pushpin</styleUrl>
		</Pair>
		<Pair>
			<key>highlight</key>
			<styleUrl>#sh_blue-pushpin</styleUrl>
		</Pair>
	</StyleMap>
	<Folder>
	        <name>%.1f%s</name>
                <visibility>0</visibility>
		<open>0</open>
		<Placemark>
			<name>summit</name>
			<styleUrl>#msn_ylw-pushpin</styleUrl>
			<Point>
				<coordinates>%f,%f,0</coordinates>
			</Point>
		</Placemark>
		<Placemark>
			<name>saddle</name>
			<styleUrl>#msn_blue-pushpin</styleUrl>
			<Point>
				<coordinates>%f,%f,0</coordinates>
			</Point>
		</Placemark>
<Placemark>
  <name>divide</name>
  <LineString>
    <extrude>1</extrude>
    <gx:altitudeMode>clampToGround</gx:altitudeMode>
    <coordinates>
%s
    </coordinates>
  </LineString>
</Placemark>
	</Folder>
</Document>
</kml>
""" % (k['prom'] / .3048, '*' if k['min_bound'] else '', k['summit'][1],  k['summit'][0], k['saddle'][1],  k['saddle'][0],
       '\n'.join('%f,%f,0' % (p[1], p[0]) for p in k['path']))

    path = '/tmp/pviz-%s-%s.kml' % (k['summitgeo'], k['saddlegeo'])
#    fd, path = tempfile.mkstemp(suffix='.kml', prefix='promviz')
#    with os.fdopen(fd, 'w') as f:
    with open(path, 'w') as f:
        f.write(content)
    
    print '<a target="_blank" href="https://maps.google.com/maps?t=p&q=http://mrgris.com:8053/%(path)s"><span style="font-family: monospace;">%(peakgeo)s %(saddlegeostem)s</span> %(prom).1f%(minbound)s (%(pathlen)d)</a><br>' % {
        'path': path[5:],
        'prom': k['prom'] / .3048,
        'minbound': '*' if k['min_bound'] else '',
        'pathlen': len(k['path']),
        'peakgeo': k['summitgeo'][:11],
        'saddlegeostem': common_prefix(k['saddlegeo'], k['summitgeo'])[:11],
    }

def common_prefix(sub, sup, pad='-'):
    x = list(sub)
    for i in range(len(sub)):
        if sub[i] == sup[i]:
            x[i] = pad
        else:
            break
    return ''.join(x)

for k in data:
    to_kml(k)


