import os
import json
import sys
import tempfile

raw = os.popen('/usr/lib/jvm/java-6-openjdk/bin/java -Xms2400m -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar promviz.DEMManager %s' % ' '.join(sys.argv[1:])).readlines()
#raw = open('/tmp/debug.output').readlines()

data = json.loads('[%s]' % ', '.join(raw))

data.sort(key=lambda e: e['prom'], reverse=True)

def to_link(k):
    print '<a target="_blank" href="http://mapper.acme.com/?ll=%f,%f&z=12&marker0=%f,%f,%.1f&marker1=%f,%f,%.1f">%.1f%s</a><br>' % (k['summit'][0], k['summit'][1], k['summit'][0], k['summit'][1], k['elev'] / .3048, k['saddle'][0], k['saddle'][1], (k['elev'] - k['prom']) / .3048, k['prom'] / .3048, '*' if k['min_bound'] else '')

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

    fd, path = tempfile.mkstemp(suffix='.kml', prefix='promviz')
    with os.fdopen(fd, 'w') as f:
        f.write(content)

    print '<a target="_blank" href="https://maps.google.com/maps?t=p&q=http://mrgris.com:8053/%s">%.1f%s (%d)</a><br>' % (path[5:], k['prom'] / .3048, '*' if k['min_bound'] else '', len(k['path']))

for k in data:
    to_kml(k)


