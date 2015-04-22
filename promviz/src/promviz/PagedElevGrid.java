package promviz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;

import old.promviz.util.Logging;
import promviz.dem.DEMFile;
import promviz.util.DefaultMap;

public class PagedElevGrid implements IMesh {

	final static int PAGE_SIZE_EXP = 9;
	
	Map<Prefix, Set<DEMFile>> coverage;
	int maxPages;
	Map<Prefix, Segment> segments;
	
	long ctr = 0;
	
	public PagedElevGrid(Map<Prefix, Set<DEMFile>> coverage, long maxPoints) {
		this.coverage = coverage;
		this.maxPages = (int)Math.ceil(maxPoints / (double)pageArea());
		segments = new HashMap<Prefix, Segment>();
	}

	static int pageDim() { return 1 << PAGE_SIZE_EXP; }
	static int pageArea() { return 1 << (2 * PAGE_SIZE_EXP); }
	static Prefix segmentPrefix(long ix) { return new Prefix(ix, PAGE_SIZE_EXP); }
	
	static class Segment {
		Prefix p;
		float[] data;
		int[] pbase;
		long ctr;
		
		public Segment(Prefix p) {
			this.p = p;
			this.pbase = PointIndex.split(p.prefix);
			this.data = new float[pageArea()];
			for (int i = 0; i < this.data.length; i++) {
				this.data[i] = Float.NaN;
			}
		}
		
		int _ix(long ix) {
			int[] _ix = PointIndex.split(ix);
			int xo = _ix[1] - this.pbase[1];
			int yo = _ix[2] - this.pbase[2];
			return pageDim() * yo + xo;
		}
		
		public float get(long ix) {
			return this.data[_ix(ix)];
		}
		
		public void set(long ix, float elev) {
			this.data[_ix(ix)] = elev;
		}
				
		public Iterable<DEMFile.Sample> samples() {
			// somewhat memory inefficient; if we only do this for max one page at a time, should be ok
			List<DEMFile.Sample> samples = new ArrayList<DEMFile.Sample>();
			for (int x = 0; x < pageDim(); x++) {
				for (int y = 0; y < pageDim(); y++) {
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
	
	public static Map<Prefix, Set<DEMFile>> partitionDEM(List<DEMFile> DEMs) {
		class PartitionMap extends DefaultMap<Prefix, Set<DEMFile>> {
			@Override
			public Set<DEMFile> defaultValue(Prefix _) {
				return new HashSet<DEMFile>();
			}
		};
		
		// TODO this could be made much faster
		PartitionMap partitions = new PartitionMap();
		for (DEMFile dem : DEMs) {
			for (long ix : dem.coords()) {
				partitions.get(segmentPrefix(ix)).add(dem);
			}
		}

		return partitions;
	}
	
	public MeshPoint get(long ix) {
		Segment seg = segments.get(segmentPrefix(ix));
		if (seg == null) {
			throw new IndexOutOfBoundsException();
		}
		seg.ctr = ctr++;
		float elev = seg.get(ix);
		if (Float.isNaN(elev)) {
			return null;
		}
		return new GridPoint(ix, elev);
	}
	
	public boolean isLoaded(Prefix prefix) {
		return segments.containsKey(prefix);
	}
	
	public Iterable<DEMFile.Sample> bulkLoadPrefixData(Set<Prefix> prefixes) {
		for (Prefix prefix : prefixes) {
			Logging.log(String.format("loading segment %s...", prefix));
			Segment seg = new Segment(prefix);
			segments.put(prefix, seg);
		}
			
		class DEMtoPrefixMap extends DefaultMap<DEMFile, Set<Prefix>> {
			@Override
			public Set<Prefix> defaultValue(DEMFile _) {
				return new HashSet<Prefix>();
			}
		};
		DEMtoPrefixMap map = new DEMtoPrefixMap();
		for (Prefix prefix : prefixes) {
			for (DEMFile dem : coverage.get(prefix)) {
				map.get(dem).add(prefix);
			}
		}
		
		// DEMs may overlap and have conflicting data in the overlapping region
		// always process DEMs in a deterministic order to be safe against this
		List<DEMFile> DEMs = new ArrayList<DEMFile>(map.keySet());
		Collections.sort(DEMs, new Comparator<DEMFile>() {
			public int compare(DEMFile a, DEMFile b) {
				return a.path.compareTo(b.path);
			}
		});
		
		for (DEMFile dem : DEMs) {
			for (DEMFile.Sample s : dem.samples()) {
				Prefix p = segmentPrefix(s.ix);
				if (map.get(dem).contains(p)) {
					segments.get(p).set(s.ix, s.elev);
				}
			}
			Logging.log(String.format("  scanned DEM %s", dem.path));
		}
		Logging.log(String.format("loading complete, %d pages", segments.size()));
		
		List<Iterable<DEMFile.Sample>> newData = new ArrayList<Iterable<DEMFile.Sample>>();
		for (Prefix prefix : prefixes) {
			newData.add(segments.get(prefix).samples());
		}
		return Iterables.concat(newData);
	}
	
	public Iterable<DEMFile.Sample> loadPage(Prefix prefix) {
		Set<Prefix> pp = new HashSet<Prefix>();
		pp.add(prefix);
		return bulkLoadPage(pp);
	}
	
	public Iterable<DEMFile.Sample> bulkLoadPage(Set<Prefix> prefixes) {
		for (Prefix p : prefixes) {
			if (isLoaded(p)) {
				throw new RuntimeException("already loaded");
			}
		}
		trimPages(prefixes.size());
		return bulkLoadPrefixData(prefixes);
	}
	
	public void trimPages(int headroom) {
		if (headroom > this.maxPages) {
			throw new IllegalArgumentException("cannot purge enough pages");
		}
		removeOldestPages(segments.size() - (this.maxPages - headroom));
	}
	
	public void removeOldestPages(int n) {
		if (n <= 0) {
			return;
		}
		
		// could be more efficient with a heap
		for (int i = 0; i < n; i++) {
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
	
//	public Meta getMeta(BasePoint p, String type) {
//		throw new RuntimeException("not supported");
//	}
}