package com.mrgris.prominence.dem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.mrgris.prominence.dem.DEMFile.Sample;

public abstract class DEMFileFormat {
	static final boolean DEBUG_NODATA_IS_OCEAN = true;
	
	public abstract Object getReader(String path) throws FileNotFoundException;
	
	public void closeReader(Object f) throws IOException {
		((InputStream)f).close();
	}
	
	public abstract double getNextSample(Object reader) throws IOException;

	public double noDataVal() {
		return Double.NaN;
	}
	
	class SamplesIterator implements Iterator<Sample> {
		Object f;
		
		Iterator<Long> coords;
		
		long _nextIx;
		float _nextElev;

		public SamplesIterator(DEMFile dem) {
			try {
				f = getReader(dem.path);
			} catch (FileNotFoundException fnfe) {
				throw new RuntimeException(String.format("[%s] not found", dem.path));
			}
			coords = dem.new CoordsIterator();
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

}
