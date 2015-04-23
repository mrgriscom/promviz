package promviz.dem;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import promviz.PointIndex;

public abstract class DEMFile {

	static final boolean DEBUG_NODATA_IS_OCEAN = true;
	
	Projection proj;
	int width;
	int height;
	int x0; //left
	int y0; //bottom
	public String path;
	
	public DEMFile(String path, Projection proj, int width, int height, int x0, int y0) {
		this.path = path;
		this.proj = proj;
		this.width = width;
		this.height = height;
		this.x0 = x0;
		this.y0 = y0;
	}
	
	public DEMFile(String path, Projection proj, int width, int height, int[] xy0) {
		this(path, proj, width, height, xy0[0], xy0[1]);
	}

	public DEMFile(String path, Projection proj, int width, int height, double lat0, double lon0) {
		this(path, proj, width, height, proj.toGrid(lat0, lon0));
	}
	
	public int xmax() {
		return x0 + width - 1;
	}
	
	public int ymax() {
		return y0 + height - 1;
	}
	
	public long genIx(int x, int y) {
		return genAbsIx(x0 + x, y0 + y);
	}
	
	public long genAbsIx(int x, int y) {
		return PointIndex.make(proj.refID, x, y);		
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
			long ix = genIx(c, height - 1 - r);
			
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
	
	public static class Sample {
		public long ix;
		public float elev;
		
		public Sample(long ix, float elev) {
			this.ix = ix;
			this.elev = elev;
		}
	}
	
	class SamplesIterator implements Iterator<Sample> {
		Object f;
		
		Iterator<Long> coords;
		
		long _nextIx;
		float _nextElev;

		public SamplesIterator() {
			try {
				f = getReader(path);
			} catch (FileNotFoundException fnfe) {
				throw new RuntimeException(String.format("[%s] not found", path));
			}
			coords = new CoordsIterator();
		}
			
		@Override
		public boolean hasNext() {
			boolean has = coords.hasNext();
			if (!has) {
				try {
					closeReader(f);
				} catch (IOException ioe) { }				
			}
			return has;
		}
		
		@Override
		public Sample next() {
			long ix = coords.next();
			double elev;
			try {
				elev = getNextSample(f);
			} catch (IOException ioe) {
				throw new RuntimeException("error reading DEM");
			}

			if (elev == noDataVal()) {
				elev = Double.NaN;
			}
			if (Double.isNaN(elev) && DEBUG_NODATA_IS_OCEAN) {
				elev = 0;
			}
			
			return new Sample(ix, (float)elev);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}

	public Iterable<Sample> samples() {
		return new Iterable<Sample>() {
			@Override
			public Iterator<Sample> iterator() {
				return new SamplesIterator();
			}
		};
	}
	
	public String toString() {
		return path;
	}
	
	public abstract Object getReader(String path) throws FileNotFoundException;
	
	public void closeReader(Object f) throws IOException {
		((InputStream)f).close();
	}
	
	public abstract double getNextSample(Object reader) throws IOException;

	public double noDataVal() {
		return Double.NaN;
	}
	
}
