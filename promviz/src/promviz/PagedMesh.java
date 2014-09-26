package promviz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.Logging;

public class PagedMesh implements IMesh {

	Map<Prefix, Set<DEMFile>> coverage;
	int maxPoints;
	Map<Long, Segment> segments;
	Queue<Prefix> loadedSegments;
	
	public PagedMesh(Map<Prefix, Set<DEMFile>> coverage, int maxPoints) {
		this.coverage = coverage;
		this.maxPoints = maxPoints;
		segments = new HashMap<Long, Segment>();
		loadedSegments = new LinkedList<Prefix>();
	}

	static class Segment {
		Prefix p;
		float[] data;
		int[] pbase;
		
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
	}
	
	public int curSize() {
		return (1 << (2 * DEMManager.GRID_TILE_SIZE)) * loadedSegments.size();
	}
	
	public Point get(long ix) {
		long ixbase = new DEMManager.Prefix(ix, DEMManager.GRID_TILE_SIZE).prefix;
		Segment seg = segments.get(ixbase);
		if (seg != null) {
			float elev = seg.get(ix);
			if (!Float.isNaN(elev)) {
				return new GridPoint(ix, elev);
			}
		}
		return null;
	}
	
	public boolean isLoaded(Prefix prefix) {
		return loadedSegments.contains(prefix);
	}
	
	public List<DEMFile.Sample> loadPrefixData(Prefix prefix) {
		Logging.log(String.format("loading segment %s...", prefix));
		Segment seg = new Segment(prefix);
		segments.put(prefix.prefix, seg);
		List<DEMFile.Sample> newData = new ArrayList<DEMFile.Sample>();
		for (DEMFile dem : coverage.get(prefix)) {
			for (DEMFile.Sample s : dem.samples(prefix)) {
				seg.set(s.ix, s.elev);
				newData.add(s);
			}
			Logging.log(String.format("  scanned DEM %s", dem.path));
		}
		Logging.log(String.format("loading complete"));
		return newData;
	}
	
	public List<DEMFile.Sample> loadPage(Prefix prefix) {
		if (isLoaded(prefix)) {
			throw new RuntimeException("already loaded");
		}
		
		while (curSize() > maxPoints) {
			removeOldestPage();
		}
		loadedSegments.add(prefix);
		List<DEMFile.Sample> newData = loadPrefixData(prefix);
		Logging.log(String.format("%d total points in mesh", curSize()));
		return newData;
	}
	
	public void removeOldestPage() {
		Prefix oldest = loadedSegments.remove();
		Logging.log("booting " + oldest);
	}
	
}
