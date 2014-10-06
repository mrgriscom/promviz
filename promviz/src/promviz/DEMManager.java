package promviz;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.Util;

import com.google.gson.Gson;


public class DEMManager {	
	static final int GRID_TILE_SIZE = 11;
	static final int MESH_MAX_POINTS = (1 << 26);
	
	List<DEMFile> DEMs;
	List<Projection> projs;
	static Projection PROJ;
	
	public DEMManager() {
		DEMs = new ArrayList<DEMFile>();
		projs = new ArrayList<Projection>();
	}
	
	DualTopologyNetwork buildAll() {
		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
		Set<Prefix> allPrefixes = coverage.keySet();
		Logging.log("partitioning complete");
		Set<Prefix> yetToProcess = new HashSet<Prefix>(coverage.keySet()); //mutable!

		PagedMesh m = new PagedMesh(coverage, MESH_MAX_POINTS);
		DualTopologyNetwork tn = new DualTopologyNetwork(this);
		while (!tn.complete(allPrefixes, yetToProcess)) {
			Prefix nextPrefix = getNextPrefix(allPrefixes, yetToProcess, tn, m);
			if (m.isLoaded(nextPrefix)) {
				Logging.log("prefix already loaded!");
				continue;
			}
			
			List<DEMFile.Sample> newData = m.loadPage(nextPrefix);
			tn.buildPartial(m, yetToProcess.contains(nextPrefix) ? newData : null);
			yetToProcess.remove(nextPrefix);
			Logging.log(yetToProcess.size() + " ytp");
		}
		return tn;
	}
		
	Prefix getNextPrefix(Set<Prefix> allPrefixes, Set<Prefix> yetToProcess, TopologyNetwork tn, PagedMesh m) {
		Map<Set<Prefix>, Integer> frontierTotals = tn.tallyPending(allPrefixes);
		
		Map<Prefix, Set<Set<Prefix>>> pendingPrefixes = new DefaultMap<Prefix, Set<Set<Prefix>>>() {
			@Override
			public Set<Set<Prefix>> defaultValue() {
				return new HashSet<Set<Prefix>>();
			}			
		};
		for (Set<Prefix> prefixGroup : frontierTotals.keySet()) {
			for (Prefix p : prefixGroup) {
				pendingPrefixes.get(p).add(prefixGroup);
			}
		}
		
		Prefix mostInDemand = null;
		int bestScore = 0;
		for (Entry<Prefix, Set<Set<Prefix>>> e : pendingPrefixes.entrySet()) {
			Prefix p = e.getKey();
			Set<Set<Prefix>> cohorts = e.getValue();

			Logging.log(String.format("pending> %s...", e.getKey())); // more?

			if (m.isLoaded(p)) {
				continue;
			}
			
			int score = 0;
			for (Set<Prefix> cohort : cohorts) {
				int numLoaded = 0;
				for (Prefix coprefix : cohort) {
					if (coprefix != p && m.isLoaded(coprefix)) {
						numLoaded++;
					}
				}
				int cohortScore = 1000000 * numLoaded + frontierTotals.get(cohort);
				score = Math.max(score, cohortScore);
			}
			
			if (score > bestScore) {
				bestScore = score;
				mostInDemand = p;
			}
		}
		
		if (mostInDemand == null) {
			mostInDemand = yetToProcess.iterator().next();
		}
		return mostInDemand;
	}
	
	public static long[] adjacency(Long ix) {
		int[] _ix = PointIndex.split(ix);
		int[] rc = {_ix[1], _ix[2]};
		
		int[][] offsets = {
				{0, 1},
				{1, 1},
				{1, 0},
				{1, -1},
				{0, -1},
				{-1, -1},
				{-1, 0},
				{-1, 1},
			};
		List<Long> adj = new ArrayList<Long>();
		boolean fully_connected = (Util.mod(rc[0] + rc[1], 2) == 0);
		for (int[] offset : offsets) {
			boolean diagonal_connection = (Util.mod(offset[0] + offset[1], 2) == 0);
			if (fully_connected || !diagonal_connection) {
				adj.add(PointIndex.make(_ix[0], rc[0] + offset[0], rc[1] + offset[1]));
			}
		}
		long[] adjix = new long[adj.size()];
		for (int i = 0; i < adjix.length; i++) {
			adjix[i] = adj.get(i);
		}
		return adjix;
	}
	
	static class Prefix implements Comparable<Prefix> {
		long prefix;
		int res;

		public Prefix(long ix, int res) {
			this.res = res;
			this.prefix = ix & this.mask();
		}

		private long mask() {
			int _mask = (~0 << this.res);
			return PointIndex.make(~0, _mask, _mask);
		}
		
		public boolean isParent(long ix) {
			return (ix & this.mask()) == this.prefix;
		}
		
		public boolean equals(Object o) {
			if (o instanceof Prefix) {
				Prefix p = (Prefix)o;
				return (this.prefix == p.prefix && this.res == p.res);
			} else {
				return false;
			}
		}
		
		public int hashCode() {
			return Long.valueOf(this.prefix | this.res).hashCode();
		}
		
		public int compareTo(Prefix p) {
			int result = Integer.valueOf(p.res).compareTo(this.res);
			if (result == 0) {
				result = Long.valueOf(this.prefix).compareTo(p.prefix);
			}
			return result;
		}
		
		public String toString() {
			int[] c = PointIndex.split(this.prefix);
			return String.format("%d,%d,%d/%d", c[0], c[1], c[2], this.res);
		}
	}
	
	class PartitionCounter {
		int count;
		Set<DEMFile> coverage;
		
		public PartitionCounter() {
			count = 0;
			coverage = new HashSet<DEMFile>();
		}
		
		public void addSample(DEMFile dem) {
			count++;
			coverage.add(dem);
		}
		
		public void combine(PartitionCounter pc) {
			count += pc.count;
			coverage.addAll(pc.coverage);
		}
	}
	
	public Map<Prefix, Set<DEMFile>> partitionDEM() {
		class PartitionMap extends DefaultMap<Prefix, Set<DEMFile>> {
			@Override
			public Set<DEMFile> defaultValue() {
				return new HashSet<DEMFile>();
			}
		};
		
		PartitionMap partitions = new PartitionMap();
		for (DEMFile dem : DEMs) {
			for (long ix : dem.coords()) {
				Prefix p = new Prefix(ix, GRID_TILE_SIZE);
				partitions.get(p).add(dem);
			}
		}
			
		return partitions;
	}
	
	// HACKY
	boolean inScope(long ix) {
		// FIXME bug lurking here: nodata areas within loaded DEM extents
		int[] _ix = PointIndex.split(ix);
		int[] xy = {_ix[1], _ix[2]};
		for (DEMFile dem : DEMs) {
			if (xy[0] >= dem.x0 && xy[1] >= dem.y0 &&
					xy[0] <= (dem.x0 + dem.height - 1) &&
					xy[1] <= (dem.y0 + dem.width - 1)) {
				return true;
			}
		}
		return false;
	}
	
	public static void main(String[] args) {
		
		Logging.init();
		
		DEMManager dm = new DEMManager();
		//PROJ = SRTMDEM.SRTMProjection(1.);
		PROJ = GridFloatDEM.NEDProjection();
		dm.projs.add(PROJ);
		
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W078.hgt", 1201, 1201, 40, -78, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W078.hgt", 1201, 1201, 41, -78, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W078.hgt", 1201, 1201, 42, -78, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W078.hgt", 1201, 1201, 43, -78, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W078.hgt", 1201, 1201, 44, -78, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N39W077.hgt", 1201, 1201, 39, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W077.hgt", 1201, 1201, 40, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W077.hgt", 1201, 1201, 41, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W077.hgt", 1201, 1201, 42, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W077.hgt", 1201, 1201, 43, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W077.hgt", 1201, 1201, 44, -77, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N38W076.hgt", 1201, 1201, 38, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N39W076.hgt", 1201, 1201, 39, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W076.hgt", 1201, 1201, 40, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W076.hgt", 1201, 1201, 41, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W076.hgt", 1201, 1201, 42, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W076.hgt", 1201, 1201, 43, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W076.hgt", 1201, 1201, 44, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W076.hgt", 1201, 1201, 45, -76, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N38W075.hgt", 1201, 1201, 38, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N39W075.hgt", 1201, 1201, 39, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W075.hgt", 1201, 1201, 40, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W075.hgt", 1201, 1201, 41, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W075.hgt", 1201, 1201, 42, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W075.hgt", 1201, 1201, 43, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W075.hgt", 1201, 1201, 44, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W075.hgt", 1201, 1201, 45, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W075.hgt", 1201, 1201, 46, -75, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W074.hgt", 1201, 1201, 40, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W074.hgt", 1201, 1201, 41, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W074.hgt", 1201, 1201, 42, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W074.hgt", 1201, 1201, 43, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W074.hgt", 1201, 1201, 44, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W074.hgt", 1201, 1201, 45, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W074.hgt", 1201, 1201, 46, -74, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N40W073.hgt", 1201, 1201, 40, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W073.hgt", 1201, 1201, 41, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W073.hgt", 1201, 1201, 42, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W073.hgt", 1201, 1201, 43, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W073.hgt", 1201, 1201, 44, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W073.hgt", 1201, 1201, 45, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W073.hgt", 1201, 1201, 46, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W073.hgt", 1201, 1201, 47, -73, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W072.hgt", 1201, 1201, 41, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W072.hgt", 1201, 1201, 42, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W072.hgt", 1201, 1201, 43, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W072.hgt", 1201, 1201, 44, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W072.hgt", 1201, 1201, 45, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W072.hgt", 1201, 1201, 46, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W072.hgt", 1201, 1201, 47, -72, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W071.hgt", 1201, 1201, 41, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N42W071.hgt", 1201, 1201, 42, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W071.hgt", 1201, 1201, 43, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W071.hgt", 1201, 1201, 44, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W071.hgt", 1201, 1201, 45, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W071.hgt", 1201, 1201, 46, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W071.hgt", 1201, 1201, 47, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W071.hgt", 1201, 1201, 48, -71, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N41W070.hgt", 1201, 1201, 41, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W070.hgt", 1201, 1201, 43, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W070.hgt", 1201, 1201, 44, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W070.hgt", 1201, 1201, 45, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W070.hgt", 1201, 1201, 46, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W070.hgt", 1201, 1201, 47, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W070.hgt", 1201, 1201, 48, -70, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W069.hgt", 1201, 1201, 43, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W069.hgt", 1201, 1201, 44, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W069.hgt", 1201, 1201, 45, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W069.hgt", 1201, 1201, 46, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W069.hgt", 1201, 1201, 47, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W069.hgt", 1201, 1201, 48, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W069.hgt", 1201, 1201, 49, -69, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W068.hgt", 1201, 1201, 44, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W068.hgt", 1201, 1201, 45, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W068.hgt", 1201, 1201, 46, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W068.hgt", 1201, 1201, 47, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W068.hgt", 1201, 1201, 48, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W068.hgt", 1201, 1201, 49, -68, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W067.hgt", 1201, 1201, 43, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W067.hgt", 1201, 1201, 44, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W067.hgt", 1201, 1201, 45, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W067.hgt", 1201, 1201, 46, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W067.hgt", 1201, 1201, 47, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W067.hgt", 1201, 1201, 48, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W067.hgt", 1201, 1201, 49, -67, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W066.hgt", 1201, 1201, 43, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W066.hgt", 1201, 1201, 44, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W066.hgt", 1201, 1201, 45, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W066.hgt", 1201, 1201, 46, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W066.hgt", 1201, 1201, 47, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W066.hgt", 1201, 1201, 48, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W066.hgt", 1201, 1201, 49, -66, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N43W065.hgt", 1201, 1201, 43, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W065.hgt", 1201, 1201, 44, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W065.hgt", 1201, 1201, 45, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W065.hgt", 1201, 1201, 46, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W065.hgt", 1201, 1201, 47, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N48W065.hgt", 1201, 1201, 48, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W065.hgt", 1201, 1201, 49, -65, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W064.hgt", 1201, 1201, 44, -64, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W064.hgt", 1201, 1201, 45, -64, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W064.hgt", 1201, 1201, 46, -64, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W064.hgt", 1201, 1201, 47, -64, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N49W064.hgt", 1201, 1201, 49, -64, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W063.hgt", 1201, 1201, 44, -63, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W063.hgt", 1201, 1201, 45, -63, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W063.hgt", 1201, 1201, 46, -63, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W063.hgt", 1201, 1201, 47, -63, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N44W062.hgt", 1201, 1201, 44, -62, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W062.hgt", 1201, 1201, 45, -62, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W062.hgt", 1201, 1201, 46, -62, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W062.hgt", 1201, 1201, 47, -62, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W061.hgt", 1201, 1201, 45, -61, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W061.hgt", 1201, 1201, 46, -61, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W061.hgt", 1201, 1201, 47, -61, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N45W060.hgt", 1201, 1201, 45, -60, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N46W060.hgt", 1201, 1201, 46, -60, 1));
//		dm.DEMs.add(new SRTMDEM("/mnt/ext/gis/ferranti/hgts/N47W060.hgt", 1201, 1201, 47, -60, 1));

		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n42w073/floatn42w073_13.flt",
				10812, 10812, 40.9994444, -73.0005555, 9.259259e-5, 9.259259e-5, true));
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n45w072/floatn45w072_13.flt",
//				10812, 10812, 43.9994444, -72.0005555, 9.259259e-5, 9.259259e-5, true));
		
		boolean up = true;
		//boolean up = false;
		DualTopologyNetwork dtn = dm.buildAll();
		System.err.println(dtn.up.points.size() + " nodes in network (up)");
		System.err.println(dtn.down.points.size() + " nodes in network (down)");

		double PROM_CUTOFF = 5.;
		double ANTI_PROM_CUTOFF = PROM_CUTOFF;
		
		TopologyNetwork tn = (up ? dtn.up : dtn.down);
		TopologyNetwork anti_tn = (!up ? dtn.up : dtn.down);
		
		Map<Point, PromNetwork.PromInfo> prominentPoints = new HashMap<Point, PromNetwork.PromInfo>();
		
		for (Point p : tn.points.values()) {
			if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
				continue;
			}
			
			PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
			if (pi != null && pi.prominence() > PROM_CUTOFF) {
				prominentPoints.put(p, pi);
			}
		}

		Map<Point, PromNetwork.PromInfo> saddleIndex = new HashMap<Point, PromNetwork.PromInfo>();
		for (Entry<Point, PromNetwork.PromInfo> e : prominentPoints.entrySet()) {
			// TODO i think we need to refine the tiebreaker logic here
			saddleIndex.put(e.getValue().saddle, e.getValue());
		}
		
		Gson ser = new Gson();
		System.out.println("[");
		boolean first = true;
		for (Entry<Point, PromNetwork.PromInfo> e : prominentPoints.entrySet()) {
			Point p = e.getKey();
			PromNetwork.PromInfo pi = e.getValue();
			
			PromNetwork.PromInfo parentage = PromNetwork.parent(tn, p, up, prominentPoints);
			
			List<Point> domainSaddles = null; //PromNetwork.domainSaddles(tn, p, saddleIndex, (float)pi.prominence());
//				List<String> domainLimits = new ArrayList<String>();
//				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
//					domainLimits.add(pathToStr(ro));					
//				}
			//domainSaddles.remove(pi.saddle);
			
			System.out.println((first ? "" : ",") + ser.toJson(new PromData(
					up, p, pi, parentage, domainSaddles, prominentPoints
				)));
			first = false;
		}
		System.out.println("]");
	}
	
	static class PromPoint {
		double coords[];
		String geo;
		double elev;
		double prom;
		
		public PromPoint(Point p, PromNetwork.PromInfo pi) {
			this.coords = PointIndex.toLatLon(p.ix);
			this.geo = GeoCode.print(GeoCode.fromCoord(this.coords[0], this.coords[1]));
			this.elev = p.elev;
			if (pi != null) {
				prom = pi.prominence();
			}
		}
	}
	
	static class PromData {
		boolean up;
		PromPoint summit;
		PromPoint saddle;
		boolean min_bound;
		PromPoint higher;
		PromPoint parent;
		List<double[]> higher_path;
		List<double[]> parent_path;
		//List<PromPoint> secondary_saddles;
		
		public PromData(boolean up, Point p, PromNetwork.PromInfo pi, PromNetwork.PromInfo parentage,
				List<Point> domainSaddles, Map<Point, PromNetwork.PromInfo> prominentPoints) {
			this.up = up;
			this.summit = new PromPoint(p, pi);
			this.saddle = new PromPoint(pi.saddle, null);
			this.min_bound = pi.min_bound_only;
		
			/*
			this.secondary_saddles = new ArrayList<PromPoint>();
			for (Point ss : domainSaddles) {
				this.secondary_saddles.add(new PromPoint(ss, null));
			}
			*/
			
			this.higher_path = new ArrayList<double[]>();
			for (Point k : pi.path) {
				this.higher_path.add(PointIndex.toLatLon(k.ix));
			}
			this.parent_path = new ArrayList<double[]>();
			for (Point k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k.ix));
			}
			
			if (!pi.min_bound_only && !pi.path.isEmpty()) {
				this.higher = new PromPoint(pi.path.get(0), null);
			}
			if (!parentage.min_bound_only && !parentage.path.isEmpty()) {
				Point parent = parentage.path.get(0);
				this.parent = new PromPoint(parent, prominentPoints.get(parent));
			}
		}
	}
	
}
