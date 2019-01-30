package com.mrgris.prominence.dem;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;

import com.mrgris.prominence.PointIndex;
import com.mrgris.prominence.TopologyNetworkPipeline;

@DefaultCoder(AvroCoder.class)
public class DEMFile {
	
	static final boolean DEBUG_NODATA_IS_OCEAN = true;
	
	public String path;
	public int grid_id;
	protected int width;
	protected int height;
	public int[] origin;
	public boolean flip_xy = false;
	public boolean inv_x = false;
	public boolean inv_y = false;
	public String bound;
	public double nodata = Double.NaN;
	public double z_unit = 1.;
	
	// for deserialization
	public DEMFile() {}
				
	public int xdim() {
		return (flip_xy ? height : width);
	}
	public int ydim() {
		return (flip_xy ? width : height);
	}
	
	public int xend() {
		return origin[0] + (xdim() - 1) * (inv_x ? -1 : 1);
	}
	public int yend() {
		return origin[1] + (ydim() - 1) * (inv_y ? -1 : 1);
	}
	
	public int xmin() {
		return Math.min(origin[0], xend());
	}
	public int xmax() {
		return Math.max(origin[0], xend());
	}
	public int ymin() {
		return Math.min(origin[1], yend());
	}
	public int ymax() {
		return Math.max(origin[1], yend());
	}		
	
	public long genRCIx(int r, int c) {
		int px = c;
		int py = height - 1 - r;
		return genAbsIx(origin[0] + (flip_xy ? py : px) * (inv_x ? -1 : 1),
				 	    origin[1] + (flip_xy ? px : py) * (inv_y ? -1 : 1));
	}

	public long genAbsIx(int x, int y) {
		return PointIndex.make(grid_id, x, y);		
	}
	
	public static class Sample {
		public long ix;
		public float elev;
		public int isodist;

		// only safe for pagedelevgrid, which reconstructs the sample later
		public Sample(long ix, float elev) {
			this(ix, elev, 0);
		}

		public Sample(long ix, float elev, int isodist) {
			this.ix = ix;
			this.elev = elev;
			this.isodist = isodist;
		}
	}
		
	public Iterable<Sample> samples(String cacheDir) {
		return new Iterable<Sample>() {
			@Override
			public Iterator<Sample> iterator() {
				return new SamplesIterator(cacheDir);
			}
		};
	}

	class SamplesIterator implements Iterator<Sample> {
		Dataset ds;
		float[] data;
		
		int r;
		int c;
		
		public SamplesIterator(String cacheDir) {
			if (!GDALUtil.initialized) {
				throw new RuntimeException("GDAL has not been initialized");
			}
			
			r = 0;
			c = 0;
			
			String cachedFile = path;
			if (cachedFile.endsWith(".gz")) {
				cachedFile = cachedFile.substring(0, cachedFile.length() - ".gz".length());
			}
			File localPath = Paths.get(cacheDir, cachedFile).toFile();
			if (!localPath.exists()) {
				localPath.getParentFile().mkdirs();
		    	try {
		    		ReadableByteChannel readableByteChannel = Channels.newChannel(
		    				new URL(TopologyNetworkPipeline.DEM_ROOT + path).openStream());
		    		FileOutputStream fileOutputStream = new FileOutputStream(localPath);
		    		fileOutputStream.getChannel()
		    		.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
		    		fileOutputStream.close();
		    	} catch (IOException e) {
		    		throw new RuntimeException(e);
		    	}
			}
			
			Dataset ds = gdal.Open(localPath.getPath(), gdalconstConstants.GA_ReadOnly);
			Band band = ds.GetRasterBand(1);
			data = new float[width * height];
			band.ReadRaster(0, 0, width, height, data);
			ds.delete();
		}
		
		@Override
		public boolean hasNext() {
			boolean has = r < height;
			if (!has) {
				// cleanup?
			}
			return has;
		}

		@Override
		public Sample next() {
			long ix = genRCIx(r, c);
			double elev = data[r * width + c];

			c++;
			if (c == width) {
				c = 0;
				r++;
			}

			if (elev == nodata) {
				elev = Double.NaN;
			}
			elev *= z_unit;
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


	
	public String toString() {
		return path;
	}
	
}
