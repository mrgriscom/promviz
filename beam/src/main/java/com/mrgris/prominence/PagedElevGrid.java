package com.mrgris.prominence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.DefaultMap;
import com.mrgris.prominence.util.SaneIterable;
import com.mrgris.prominence.util.Util;

public class PagedElevGrid implements IMesh {

	final static int PAGE_SIZE_EXP = 9;
	
	Map<Prefix, Iterable<DEMFile>> coverage;
	int maxPages;
	Map<Prefix, Segment> segments;
	String demCacheDir;
	boolean destroyed = false;
	
	long ctr = 0;
	
	public PagedElevGrid(Map<Prefix, Iterable<DEMFile>> coverage, long maxPoints) {
		this.coverage = coverage;
		this.maxPages = (int)Math.ceil(maxPoints / (double)pageArea());
		segments = new HashMap<Prefix, Segment>();
		demCacheDir = com.google.common.io.Files.createTempDir().getPath();
	}

	public void destroy() {
		try {
			Files.walk(Paths.get(demCacheDir))
	        .map(Path::toFile)
	        .sorted((o1, o2) -> -o1.compareTo(o2))
	        .forEach(File::delete);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		destroyed = true;
	}
	
	@Override
	public void finalize() {
		if (!destroyed) {
			throw new RuntimeException("destroy() never called");
		}
	}
	
	static int pageDim() { return Util.pow2(PAGE_SIZE_EXP); }
	static int pageArea() { return Util.pow2(2 * PAGE_SIZE_EXP); }
	static Prefix segmentPrefix(long ix) { return new Prefix(ix, PAGE_SIZE_EXP); }
	
	static class Segment {
		Prefix p;
		float[] data;
		int[] pbase;
		long ctr;
		
		public Segment(Prefix p) {
			this(p, false);
		}
		
		public Segment(Prefix p, boolean virtual) {
			this.p = p;
			this.pbase = PointIndex.split(p.prefix);
			if (!virtual) {
				this.data = new float[pageArea()];
				for (int i = 0; i < this.data.length; i++) {
					this.data[i] = Float.NaN;
				}
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
			return new SaneIterable<DEMFile.Sample>() {
				int x = pageDim() - 1;
				int y = -1;

				public DEMFile.Sample genNext() {
					while (true) {
						x++;
						if (x == pageDim()) {
							x = 0;
							y++;
							if (y == pageDim()) {
								throw new NoSuchElementException();
							}
						}

						long ix = PointIndex.make(pbase[0], pbase[1] + x, pbase[2] + y);
						float elev = get(ix);
						if (!Float.isNaN(elev)) {
							return new DEMFile.Sample(ix, elev, fakeIsodistForIx(ix));
						}
					}
				}
			};
		}
	}
	
	// a segment that we know has no data -- we still want to 'load' it so that it may
	// be queried, but don't want to use any memory
	static class EmptySegment extends Segment {
		public EmptySegment(Prefix p) {
			super(p, true);
		}
		
		public float get(long ix) {
			return Float.NaN;
		}
		
		public void set(long ix, float elev) {
			throw new UnsupportedOperationException();
		}
	}

	static int fakeIsodistForIx(long ix) {
		// rely on direct index tiebreaking for random walk behavior (expensive)
		//return 0;
		
		int[] c = PointIndex.split(ix);
		return c[1] - c[2];
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
		return new GridPoint(ix, elev, fakeIsodistForIx(ix));
	}
	
	public boolean isLoaded(Prefix prefix) {
		return segments.containsKey(prefix);
	}
	
	public Iterable<DEMFile.Sample> bulkLoadPrefixData(Set<Prefix> prefixes) {
		for (Prefix prefix : prefixes) {
			//Logging.log(String.format("loading segment %s...", prefix));
			Segment seg = (coverage.containsKey(prefix) ? new Segment(prefix) : new EmptySegment(prefix));
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
			for (DEMFile dem : coverage.getOrDefault(prefix, new ArrayList<DEMFile>())) {
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
			for (DEMFile.Sample s : dem.samples(demCacheDir, map.get(dem))) {
				Prefix p = segmentPrefix(s.ix);
				if (map.get(dem).contains(p)) {
					segments.get(p).set(s.ix, s.elev);
				}
			}
			//Logging.log(String.format("  scanned DEM %s", dem.path));
		}
		//Logging.log(String.format("loading complete, %d pages", segments.size()));
		
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
	
	public Iterable<DEMFile.Sample> loadForPrefix(Prefix p, int fringe) {
		Prefix[] prefixes = p.children(p.res - PAGE_SIZE_EXP, fringe);
		return bulkLoadPage(new HashSet<Prefix>(Arrays.asList(prefixes)));
	}
	
	public void trimPages(int headroom) {
		if (headroom > this.maxPages) {
			throw new IllegalArgumentException(String.format("cannot purge enough pages %d %d %d", this.maxPages, headroom, segments.size()));
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
			//Logging.log("booting " + toEject.getKey());
		}
	}
	
//	public Meta getMeta(BasePoint p, String type) {
//		throw new RuntimeException("not supported");
//	}
}
