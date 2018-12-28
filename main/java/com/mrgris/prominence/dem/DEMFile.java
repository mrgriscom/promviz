package com.mrgris.prominence.dem;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.mrgris.prominence.PointIndex;

/**
 *  from gdal
 *  
 *  path
 *  CRS
 *  pixel-dx/dy
 *  subpx-offset-x/y
 *  width/height
 *  origin
 *  z-unit
 *  nodata

 *  
 *  (series, path)
 *  grid def:
 *  - CRS
 *  - pixel spacing (x and y)
 *  - sub-pixel offset (i.e., area-or-point)
 *  width/height
 *  origin (integer xy pixel coordinates)
 *  z-unit (m / ft / other?)
 *  nodata value
 *
 *  loadElevData(rect)
 */

@DefaultCoder(AvroCoder.class)
public class DEMFile {

	public final static int GRID_GEO_3AS = 0;
	public final static int GRID_GEO_1AS = 1;
	public final static int GRID_GEO_1_3AS = 2;
	
	public final static int FORMAT_SRTM_HGT = 0;
	public final static int FORMAT_GRID_FLOAT = 1;
	
	static Map<Integer, Projection> gridRefs = new HashMap<>();
	static Map<Integer, DEMFileFormat> formats = new HashMap<>();
	static {
		gridRefs.put(GRID_GEO_3AS, GeoProjection.fromArcseconds(3.));
		gridRefs.put(GRID_GEO_1AS, GeoProjection.fromArcseconds(1.));
		gridRefs.put(GRID_GEO_1_3AS, GeoProjection.fromArcseconds(1/3.));
		
		formats.put(FORMAT_SRTM_HGT, new SrtmHgtFormat());
		formats.put(FORMAT_GRID_FLOAT, new GridFloatFormat());
	}
	
	public String path;
	int projId;
	int formatId;
	int width;
	int height;
	int x0; //left
	int y0; //bottom
	
	// for deserialization
	public DEMFile() {}
	
	public DEMFile(String path, int projId, int formatId, int width, int height, int x0, int y0) {
		this.path = path;
		this.projId = projId;
		this.formatId = formatId;
		this.width = width;
		this.height = height;
		this.x0 = x0;
		this.y0 = y0;
	}
	
	public DEMFile(String path, int projId, int formatId, int width, int height, int[] xy0) {
		this(path, projId, formatId, width, height, xy0[0], xy0[1]);
	}

	public DEMFile(String path, int projId, int formatId, int width, int height, double lat0, double lon0) {
		this(path, projId, formatId, width, height, _projection(projId).toGrid(lat0, lon0));
	}
	
	public static Projection _projection(int projId) {
		return gridRefs.get(projId);
	}

	public Projection projection() {
		return _projection(projId);
	}
	
	public DEMFileFormat format() {
		return formats.get(formatId);
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
		return PointIndex.make(projId, x, y);		
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
			
	public static class Sample {
		public long ix;
		public float elev;
		
		public Sample(long ix, float elev) {
			this.ix = ix;
			this.elev = elev;
		}
	}
		
	public Iterable<Sample> samples() {
		return new Iterable<Sample>() {
			@Override
			public Iterator<Sample> iterator() {
				return format().new SamplesIterator(DEMFile.this);
			}
		};
	}

	public String toString() {
		return path;
	}
	
}
