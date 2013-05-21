package promviz;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import com.google.common.io.LittleEndianDataInputStream;


public class DEMFile {

	int width;
	int height;
	double lat0;
	double lon0;
	double dx;
	double dy;
	boolean sample_mode; // true == lat0/lon0 correspond to center of pixel; false == correspond to corner of pixel
	String path;
	
	public DEMFile(String path, int width, int height, double lat0, double lon0, double dx, double dy, boolean sample_mode) {
		this.path = path;
		this.width = width;
		this.height = height;
		this.lat0 = lat0;
		this.lon0 = lon0;
		this.dx = dx;
		this.dy = dy;
		this.sample_mode = sample_mode;
	}
	
	class CoordsIterator implements Iterator<Long> {
		int r;
		int c;
		
		public CoordsIterator() {
			r = 0;
			c = 0;
		}
		
		@Override
		public boolean hasNext() {
			return r < height;
		}

		@Override
		public Long next() {
			double lon = lon0 + dx * (c + (sample_mode ? 0 : 0.5));
			double lat = lat0 + dy * (height - (r + (sample_mode ? 1 : 0.5)));
			c++;
			if (c == width) {
				c = 0;
				r++;
			}
			return GeoCode.fromCoord(lat, lon);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
		
	public Iterable<Long> coords() {
		return new Iterable<Long>() {
			@Override
			public Iterator<Long> iterator() {
				return new CoordsIterator();
			}
		};
	}
	
	class PointsIterator implements Iterator<Point> {
		LittleEndianDataInputStream f;
		Iterator<Long> coords;
		Random r;
		DEMManager.Prefix prefix;
		
		long _nextIx;
		double _nextElev;
		
		public PointsIterator(DEMManager.Prefix prefix) {
			this.prefix = prefix;
			try {
				f = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
			} catch (FileNotFoundException fnfe) {
				throw new RuntimeException(String.format("[%s] not found", path));
			}
			coords = new CoordsIterator();
			r = new Random(path.hashCode());
		}
			
		@Override
		public boolean hasNext() {
			while (coords.hasNext()) {
				long geocode = coords.next();
				double elev;
				try {
					elev = f.readShort();
				} catch (IOException ioe) {
					throw new RuntimeException("error reading DEM");
				}

				if (prefix.isParent(geocode)) {
					_nextIx = geocode;
					_nextElev = elev;
					return true;
				}
			}
			return false;
		}

		@Override
		public Point next() {
			long geocode = _nextIx;
			double elev = _nextElev;
			if (elev == -32768) {
				elev = 0; // cgiar has voids filled so nodata is actually ocean
			}
			Point p = new Point(geocode, elev);
			p._adjacent = DEMManager.adjacency(p.geocode);
			p.elev += r.nextDouble() - 0.5;
			return p;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}

	public Iterable<Point> samples(final DEMManager.Prefix prefix) {
		return new Iterable<Point>() {
			@Override
			public Iterator<Point> iterator() {
				return new PointsIterator(prefix);
			}			
		};
	}
			
	public static void main(String[] args) {
		for (Long l : new DEMFile("", 20, 10, 40, -75, .01, .01, true).coords()) {
			System.out.println(GeoCode.print(GeoCode.prefix(l, 26)));
		}
	}
}
