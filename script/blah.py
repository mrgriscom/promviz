import os
import json

#raw = os.popen('/usr/lib/jvm/java-6-openjdk/bin/java -Xms2400m -Dfile.encoding=UTF-8 -classpath /home/drew/dev/promviz/promviz/bin:/home/drew/dev/promviz/promviz/lib/guava-14.0.1.jar LoadDEM').readlines()
raw = open('/tmp/debug.output').readlines()

data = json.loads('[%s]' % ', '.join(raw))

data.sort(key=lambda e: e['prom'], reverse=True)

print """<?xml version="1.0" encoding="UTF-8"?>
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
		<name>findings</name>
		<open>1</open>
"""

for k in data:

    print """
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
		</Folder>
""" % (k['prom'] / .3048, '*' if k['min_bound'] else '', k['summit'][1],  k['summit'][0], k['saddle'][1],  k['saddle'][0])

print """
	</Folder>
</Document>
</kml>
"""
