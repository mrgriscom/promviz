package promviz;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.Util;

import com.google.gson.Gson;


public class DEMManager {	

	static final int GRID_TILE_SIZE = 11;
	static long MESH_MAX_POINTS;
	
	List<DEMFile> DEMs;
	List<Projection> projs;
	static Projection PROJ;
	
	static Properties props;
	
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
		DualTopologyNetwork tn = new DualTopologyNetwork(this, true);
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
					xy[0] < (dem.x0 + dem.height) &&
					xy[1] < (dem.y0 + dem.width)) {
				return true;
			}
		}
		return false;
	}

	public static void loadDEMs(DEMManager dm, String region) {
		try {
	        Process proc = new ProcessBuilder(new String[] {"python", "demregion.py", region}).start();
	        BufferedReader stdin = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	        
	        String s = null;
	        while ((s = stdin.readLine()) != null) {
	        	String[] parts = s.split(",");
	        	String type = parts[0];
	        	String path = parts[1];
	        	int w = Integer.parseInt(parts[2]);
	        	int h = Integer.parseInt(parts[3]);
	        	double lat = Double.parseDouble(parts[4]);
	        	double lon = Double.parseDouble(parts[5]);
	        	int res = Integer.parseInt(parts[6]);
	        	
	        	DEMFile dem = new SRTMDEM(path, w, h, lat, lon, res);
	        	dm.DEMs.add(dem);
	        }
	    } catch (IOException e) {
	        throw new RuntimeException();
	    }		
		Logging.log("DEMs inventoried");
	}
	
	public static void main(String[] args) {
		
		Logging.init();
		
		props = new Properties();
		try {
			props.load(ClassLoader.getSystemResourceAsStream("config.properties"));
		} catch (IOException e) {
			throw new RuntimeException();
		}
		
		DEMManager dm = new DEMManager();
		PROJ = SRTMDEM.SRTMProjection(1.);
		//PROJ = GridFloatDEM.NEDProjection();
		dm.projs.add(PROJ);
		
		boolean buildtn;
		if (args[0].equals("--build")) {
			buildtn = true;
		} else if (args[0].equals("--search")) {
			buildtn = false;
		} else {
			throw new RuntimeException("operation not specified");
		}
		String region = args[1];
		loadDEMs(dm, region);
		
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n42w073/floatn42w073_13.flt",
//				10812, 10812, 40.9994444, -73.0005555, 9.259259e-5, 9.259259e-5, true));
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n45w072/floatn45w072_13.flt",
//				10812, 10812, 43.9994444, -72.0005555, 9.259259e-5, 9.259259e-5, true));
		
		final boolean up = true;
		//boolean up = false;

		DualTopologyNetwork dtn;
		if (buildtn) {
			MESH_MAX_POINTS = Long.parseLong(props.getProperty("memory")) / 8;
			dtn = dm.buildAll();
			dtn.up.cleanup();
			dtn.down.cleanup();
		} else {
			MESH_MAX_POINTS = (1 << 26);
			dtn = DualTopologyNetwork.load(dm);
		}
		
		System.err.println(dtn.up.points.size() + " nodes in network (up), " + dtn.up.numEdges + " edges");
		System.err.println(dtn.down.points.size() + " nodes in network (down), " + dtn.down.numEdges + " edges");

		if (buildtn) {
			return;
		}
		
		double PROM_CUTOFF = 15.;
		double ANTI_PROM_CUTOFF = PROM_CUTOFF;
		
		TopologyNetwork tn = (up ? dtn.up : dtn.down);
		TopologyNetwork anti_tn = (!up ? dtn.up : dtn.down);
		
//		Point highest = null;
//		Comparator<Point> cmp = new Comparator<Point>() {
//			@Override
//			public int compare(Point p0, Point p1) {
//				return up ? ElevComparator.cmp(p0, p1) : ElevComparator.cmp(p1, p0);
//			}
//		};
//		for (Point p : tn.allPoints()) {
//			if (highest == null || cmp.compare(p, highest) > 0) {
//				highest = p;
//			}
//		}
//		Logging.log("highest: " + highest.elev);
//		prominentPoints.put(highest, PromNetwork.prominence(tn, highest, up));
//		PromNetwork.bigOlPromSearch(highest, tn, prominentPoints, PROM_CUTOFF);
		
		for (Point p : tn.allPoints()) {
			if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
				continue;
			}
			
			PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
			if (pi != null && pi.prominence() >= PROM_CUTOFF) {
				outputPromPoint(pi, up);
			}
		}

//		Map<Point, PromNetwork.PromInfo> saddleIndex = new HashMap<Point, PromNetwork.PromInfo>();
//		for (Entry<Point, PromNetwork.PromInfo> e : prominentPoints.entrySet()) {
//			// TODO i think we need to refine the tiebreaker logic here
//			saddleIndex.put(e.getValue().saddle, e.getValue());
//		}
		
//		Gson ser = new Gson();
//		System.out.println("[");
//		boolean first = true;
//		for (Entry<Point, PromNetwork.PromInfo> e : prominentPoints.entrySet()) {
//			Point p = e.getKey();
//			PromNetwork.PromInfo pi = e.getValue();
//			
//			PromNetwork.PromInfo parentfill = new PromNetwork.PromInfo(pi.p, pi.c);
//			parentfill.saddle = pi.saddle;
//			parentfill.path = new ArrayList<Long>();
//			parentfill.path.add(pi.p.ix);
//			parentfill.path.add(pi.saddle.ix);
//			parentfill.min_bound_only = true;
//			PromNetwork.PromInfo parentage = parentfill; //PromNetwork.parent(tn, p, up, prominentPoints);
//			
//			List<Point> domainSaddles = null; //PromNetwork.domainSaddles(tn, p, saddleIndex, (float)pi.prominence());
////				List<String> domainLimits = new ArrayList<String>();
////				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
////					domainLimits.add(pathToStr(ro));					
////				}
//			//domainSaddles.remove(pi.saddle);
//			
//			System.out.println((first ? "" : ",") + ser.toJson(new PromData(
//					up, p, pi, parentage, domainSaddles
//				)));
//			first = false;
//		}
//		System.out.println("]");
		
		Logging.log("java out");
	}
	
	static void outputPromPoint(PromNetwork.PromInfo pi, boolean up) {
		Gson ser = new Gson();
		
		PromNetwork.PromInfo parentfill = new PromNetwork.PromInfo(pi.p, pi.c);
		parentfill.saddle = pi.saddle;
		parentfill.path = new ArrayList<Long>();
		parentfill.path.add(pi.p.ix);
		parentfill.path.add(pi.saddle.ix);
		parentfill.min_bound_only = true;
		PromNetwork.PromInfo parentage = parentfill; //PromNetwork.parent(tn, p, up, prominentPoints);
		
		List<Point> domainSaddles = null; //PromNetwork.domainSaddles(tn, p, saddleIndex, (float)pi.prominence());
//				List<String> domainLimits = new ArrayList<String>();
//				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
//					domainLimits.add(pathToStr(ro));					
//				}
		//domainSaddles.remove(pi.saddle);
		
		System.out.println(ser.toJson(new PromData(
				up, pi.p, pi, parentage, domainSaddles
			)));
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
		
		public PromPoint(long ix) {
			this.coords = PointIndex.toLatLon(ix);
			this.geo = GeoCode.print(GeoCode.fromCoord(this.coords[0], this.coords[1]));			
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
				List<Point> domainSaddles) {
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
			for (long k : pi.path) {
				this.higher_path.add(PointIndex.toLatLon(k));
			}
			this.parent_path = new ArrayList<double[]>();
			for (long k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k));
			}
			
			if (!pi.min_bound_only && !pi.path.isEmpty()) {
				this.higher = new PromPoint(pi.path.get(0));
			}
			if (!parentage.min_bound_only && !parentage.path.isEmpty()) {
				this.parent = new PromPoint(parentage.path.get(0));
			}
		}
	}
	
}
