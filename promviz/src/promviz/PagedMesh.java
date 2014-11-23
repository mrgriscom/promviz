package promviz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.DEMFile.Sample;
import promviz.DEMManager.Prefix;
import promviz.util.Logging;

public class PagedMesh implements IMesh {

	Map<Prefix, Set<DEMFile>> coverage;
	long maxPoints;
	Map<Prefix, Segment> segments;
	
	long ctr = 0;
	
	public PagedMesh(Map<Prefix, Set<DEMFile>> coverage, long maxPoints) {
		this.coverage = coverage;
		this.maxPoints = maxPoints;
		segments = new HashMap<Prefix, Segment>();
	}

	static class Segment {
		Prefix p;
		float[] data;
		int[] pbase;
		long ctr;
		
		public Segment(Prefix p) {
			this.p = p;
			this.pbase = PointIndex.split(p.prefix);
			this.data = new float[1 << (2 * DEMManager.GRID_TILE_SIZE)];
			for (int i = 0; i < this.data.length; i++) {
				this.data[i] = Float.NaN;
			}
		}
		
		public float get(long ix) {
			int[] _ix = PointIndex.split(ix);
			int xo = _ix[1] - this.pbase[1];
			int yo = _ix[2] - this.pbase[2];
			return this.data[(1<<DEMManager.GRID_TILE_SIZE) * yo + xo];
		}
		
		public void set(long ix, float elev) {
			int[] _ix = PointIndex.split(ix);
			int xo = _ix[1] - this.pbase[1];
			int yo = _ix[2] - this.pbase[2];
			this.data[(1<<DEMManager.GRID_TILE_SIZE) * yo + xo] = elev;
		}
				
		public Iterable<DEMFile.Sample> samples() {
			List<DEMFile.Sample> samples = new ArrayList<DEMFile.Sample>();
			for (int x = 0; x < (1<<DEMManager.GRID_TILE_SIZE); x++) {
				for (int y = 0; y < (1<<DEMManager.GRID_TILE_SIZE); y++) {
					long ix = PointIndex.make(pbase[0], pbase[1] + x, pbase[2] + y);
					float elev = get(ix);
					if (!Float.isNaN(elev)) {
						samples.add(new DEMFile.Sample(ix, elev));
					}
				}
			}
			return samples;
		}
	}
	
	public long curSize() {
		return (long)(1 << (2 * DEMManager.GRID_TILE_SIZE)) * segments.size();
	}
	
	public Point get(long ix) {
		Segment seg = segments.get(new DEMManager.Prefix(ix, DEMManager.GRID_TILE_SIZE));
		if (seg != null) {
			seg.ctr = ctr++;
			float elev = seg.get(ix);
			if (!Float.isNaN(elev)) {
				return new GridPoint(ix, elev);
			}
		}
		return null;
	}
	
	public boolean isLoaded(Prefix prefix) {
		return segments.containsKey(prefix);
	}
	
	public Iterable<DEMFile.Sample> loadPrefixData(Prefix prefix) {
		Logging.log(String.format("loading segment %s...", prefix));
		Segment seg = new Segment(prefix);
		segments.put(prefix, seg);

		// protect against conflicting data in overlapping regions of DEM?
		List<DEMFile> DEMs = new ArrayList<DEMFile>(coverage.get(prefix));
		Collections.sort(DEMs, new Comparator<DEMFile>() {
			public int compare(DEMFile a, DEMFile b) {
				return a.path.compareTo(b.path);
			}			
		});
		for (DEMFile dem : DEMs) {
			for (DEMFile.Sample s : dem.samples(prefix)) {
				seg.set(s.ix, s.elev);
			}
			Logging.log(String.format("  scanned DEM %s", dem.path));
		}
		Logging.log(String.format("loading complete"));
		return seg.samples();
	}
	
	public Iterable<DEMFile.Sample> loadPage(Prefix prefix) {
		if (isLoaded(prefix)) {
			throw new RuntimeException("already loaded");
		}
		
		while (curSize() > maxPoints) {
			removeOldestPage();
		}
		Iterable<DEMFile.Sample> newData = loadPrefixData(prefix);
		Logging.log(String.format("%d total points in mesh", curSize()));
		return newData;
	}
	
	public void removeOldestPage() {
		Map.Entry<Prefix, Segment> toEject = null;
		for (Map.Entry<Prefix, Segment> e : segments.entrySet()) {
			if (toEject == null || e.getValue().ctr < toEject.getValue().ctr) {
				toEject = e;
			}
		}

		segments.remove(toEject.getKey());
		Logging.log("booting " + toEject.getKey());
	}
	
}
