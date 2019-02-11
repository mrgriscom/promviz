package com.mrgris.prominence.dem;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.Prefix;
import com.mrgris.prominence.TopologyNetworkPipeline;
import com.mrgris.prominence.util.WorkerUtils;

@DefaultCoder(AvroCoder.class)
public class DEMFile {
	
	  private static final Logger LOG = LoggerFactory.getLogger(DEMFile.class);
	
	static final boolean DEBUG_NODATA_IS_OCEAN = true;
	
	public String path;
	public String[] sidecars = new String[0];
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
	
	transient private DEMIndex.Grid _grid;
	
	// for deserialization
	public DEMFile() {}
				
	protected DEMIndex.Grid grid() {
		if (_grid == null) {
			_grid = DEMIndex.instance().grids[grid_id];
		}
		return _grid;
	}
	
	protected int xdim() {
		return (flip_xy ? height : width);
	}
	protected int ydim() {
		return (flip_xy ? width : height);
	}
	
	protected int xend() {
		return origin[0] + (xdim() - 1) * (inv_x ? -1 : 1);
	}
	protected int yend() {
		return origin[1] + (ydim() - 1) * (inv_y ? -1 : 1);
	}
	
	protected int xmin() {
		return Math.min(origin[0], xend());
	}
	protected int xmax() {
		return Math.max(origin[0], xend());
	}
	protected int ymin() {
		return Math.min(origin[1], yend());
	}
	protected int ymax() {
		return Math.max(origin[1], yend());
	}		
	
	interface PageTiler {
		void add(int xmin, int xmax);
	}
	public List<Prefix> overlappingPages(int pageSizeExp) {
		List<Prefix> pages = new ArrayList<>();
		PageTiler pt = (xmin, xmax) ->
			pages.addAll(Arrays.asList(Prefix.tileInclusive(grid_id, xmin, ymin(), xmax, ymax(), pageSizeExp)));			
		if (grid().isCylindrical() && (xmin() < grid().xmin() || xmax() > grid().xmax())) {
			pt.add(grid().normX(xmin()), grid().xmax());
			pt.add(grid().xmin(), grid().normX(xmax()));
		} else {
			pt.add(xmin(), xmax());
		}
		return pages;
	}
	
	public long genRasterIx(int r, int c) {
		int px = c;
		int py = height - 1 - r;
		return grid().genIx(
				origin[0] + (flip_xy ? py : px) * (inv_x ? -1 : 1),
				origin[1] + (flip_xy ? px : py) * (inv_y ? -1 : 1)
			);
	}
	
	public int[] gridXYtoCR(int x, int y) {
		int dx = (x - origin[0]) / (inv_x ? -1 : 1);
		int dy = (y - origin[1]) / (inv_y ? -1 : 1);
		int px = (flip_xy ? dy : dx);
		int py = (flip_xy ? dx : dy);
		return new int[] {px, height - 1 - py};
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
	
	public Iterable<Sample> samples(String cacheDir, Iterable<Prefix> pages) {
		return new Iterable<Sample>() {
			@Override
			public Iterator<Sample> iterator() {
				return new SamplesIterator(cacheDir, pages);
			}
		};
	}
	
	static String cacheFileFromGCS(String cacheDir, String path) {
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
		return localPath.getPath();
	}
	
	class SamplesIterator implements Iterator<Sample> {
		Dataset ds;
		float[] data;
		
		int r;
		int c;
		
		int r0;
		int c0;
		int subwidth;
		int subheight;
		
		public SamplesIterator(String cacheDir, Iterable<Prefix> pages) {
			WorkerUtils.checkGDAL();
			
			int xmin = Integer.MAX_VALUE, ymin = Integer.MAX_VALUE,
					xmax = Integer.MIN_VALUE, ymax = Integer.MIN_VALUE;
			for (Prefix p : pages) {
				int[] bounds = p.bounds();
				int bxmin = bounds[0];
				int bymin = bounds[1];
				int bxmax = bounds[2];
				int bymax = bounds[3];
				if (grid().isCylindrical()) {
					if (bxmax < DEMFile.this.xmin()) {
						bxmin += grid().xwidth();
						bxmax += grid().xwidth();
					} else if (bxmin > DEMFile.this.xmax()) {
						bxmin -= grid().xwidth();
						bxmax -= grid().xwidth();					
					}
				}
				xmin = Math.min(xmin, bxmin);
				ymin = Math.min(ymin, bymin);
				xmax = Math.max(xmax, bxmax);
				ymax = Math.max(ymax, bymax);
			}
			
			int[] cr0 = gridXYtoCR(xmin, ymin);
			int[] cr1 = gridXYtoCR(xmax - 1, ymax - 1);
			int cmin = Math.max(Math.min(cr0[0], cr1[0]), 0);
			int cmax = Math.min(Math.max(cr0[0], cr1[0]) + 1, width);
			int rmin = Math.max(Math.min(cr0[1], cr1[1]), 0);
			int rmax = Math.min(Math.max(cr0[1], cr1[1]) + 1, height);
			r0 = rmin;
			c0 = cmin;
			subwidth = cmax - cmin;
			subheight = rmax - rmin;
			
			r = 0;
			c = 0;
			
			String localPath = cacheFileFromGCS(cacheDir, path);
			if (sidecars != null) {
				for (String sidecar : sidecars) {
					cacheFileFromGCS(cacheDir, sidecar);
				}
			}
			
			LOG.info(String.format("DEM loading %dx%d-%dx%d -> %dx%d-%dx%d",
					xmin, ymin, xmax, ymax, rmin, cmin, rmax, cmax));
			
			Dataset ds = gdal.Open(localPath, gdalconstConstants.GA_ReadOnly);
			Band band = ds.GetRasterBand(1);
			data = new float[subwidth * subheight];
			band.ReadRaster(c0, r0, subwidth, subheight, data);
			ds.delete();
		}
		
		@Override
		public boolean hasNext() {
			boolean has = r < subheight;
			if (!has) {
				// cleanup?
			}
			return has;
		}

		@Override
		public Sample next() {
			long ix = genRasterIx(r0 + r, c0 + c);
			double elev = data[r * subwidth + c];

			c++;
			if (c == subwidth) {
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
