package old.promviz;
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
			long ix = PointIndex.make(proj.refID, c0 + c, r0 + height - 1 - r);
			
			c++;
			if (c == width) {
				c = 0;
				r++;
			}
			
			return ix;
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
	
	static class Sample {
		public Sample(long ix, float elev) {
			this.ix = ix;
			this.elev = elev;
		}
			
		long ix;
		float elev;
	}
	
	class SamplesIterator implements Iterator<Sample> {
		Object f;
		
		Iterator<Long> coords;
		Prefix prefix;
		
		long _nextIx;
		float _nextElev;

		public SamplesIterator(Prefix prefix) {
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
				long ix = coords.next();
				float elev;
				try {
					elev = getNextSample(f);
				} catch (IOException ioe) {
					throw new RuntimeException("error reading DEM");
				}

				if (prefix == null || prefix.isParent(ix)) {
					_nextIx = ix;
					_nextElev = elev;
					return true;
				}
			}
			try {
				closeReader(f);
			} catch (IOException ioe) { }
			return false;
		}

		@Override
		public Sample next() {
			return new Sample(_nextIx, _nextElev);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}

	public Iterable<Sample> samples(final Prefix prefix) {
		return new Iterable<Sample>() {
			@Override
			public Iterator<Sample> iterator() {
				return new SamplesIterator(prefix);
			}
		};
	}
	
	public String toString() {
		return path;
	}
	
	public abstract Object getReader(String path) throws FileNotFoundException;
	
	public abstract void closeReader(Object f) throws IOException;
	
	public abstract float getNextSample(Object reader) throws IOException;

	public static void main(String[] args) {
//		for (Long l : new DEMFile("", 20, 10, 40, -75, .01, .01, true).coords()) {
//			System.out.println(GeoCode.print(GeoCode.prefix(l, 26)));
//		}
	}
}
