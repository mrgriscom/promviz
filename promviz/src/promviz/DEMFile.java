package promviz;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public abstract class DEMFile {

	Projection proj;
	int width;
	int height;
	int x0; //left
	int y0; //bottom
	String path;
	
	public DEMFile(String path, Projection proj, int width, int height, int x0, int y0) {
		this.path = path;
		this.proj = proj;
		this.width = width;
		this.height = height;
		this.x0 = x0;
		this.y0 = y0;
	}
	
	public int xmax() {
		return x0 + width - 1;
	}
	
	public int ymax() {
		return y0 + height - 1;
	}
	
	class CoordsIterator implements Iterator<Long> {
		int r0;
		int c0;
		int r;
		int c;
		
		public CoordsIterator() {
			r0 = y0;
			c0 = x0;
			r = 0;
			c = 0;
		}
		
		@Override
		public boolean hasNext() {
			return r < height;
		}

		@Override
		public Long next() {
			double[] coord = proj.fromGrid(c0 + c, r0 + height - 1 - r);
			
			c++;
			if (c == width) {
				c = 0;
				r++;
			}
			
			return GeoCode.fromCoord(coord[0], coord[1]);
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
		Object f;
		
		Iterator<Long> coords;
		DEMManager.Prefix prefix;
		
		long _nextIx;
		double _nextElev;

		public PointsIterator(DEMManager.Prefix prefix) {
			this.prefix = prefix;
			try {
				f = getReader(path);
			} catch (FileNotFoundException fnfe) {
				throw new RuntimeException(String.format("[%s] not found", path));
			}
			coords = new CoordsIterator();
		}
			
		@Override
		public boolean hasNext() {
			while (coords.hasNext()) {
				long geocode = coords.next();
				double elev;
				try {
					elev = getNextSample(f);
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
			Point p = new Point(geocode, elev);
			p._adjacent = DEMManager.adjacency(p.geocode);
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
			
	public abstract Object getReader(String path) throws FileNotFoundException;
	
	public abstract double getNextSample(Object reader) throws IOException;

	public static void main(String[] args) {
//		for (Long l : new DEMFile("", 20, 10, 40, -75, .01, .01, true).coords()) {
//			System.out.println(GeoCode.print(GeoCode.prefix(l, 26)));
//		}
	}
}
