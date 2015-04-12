package old.promviz;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import old.promviz.PreprocessNetwork.EdgeIterator;
import old.promviz.PreprocessNetwork.Meta;
import old.promviz.PreprocessNetwork.PromMeta;
import old.promviz.PreprocessNetwork.ThresholdMeta;
import old.promviz.PromNetwork.DomainSaddleInfo;
import old.promviz.PromNetwork.ParentInfo;
import old.promviz.PromNetwork.PromInfo;
import old.promviz.util.DefaultMap;
import old.promviz.util.Logging;
import old.promviz.util.Util;

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
			
			Iterable<DEMFile.Sample> newData = m.loadPage(nextPrefix);
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
			public Set<Set<Prefix>> defaultValue(Prefix _) {
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
			public Set<DEMFile> defaultValue(Prefix _) {
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
	
	public static void initProps() {
		props = new Properties();
		try {
			props.load(ClassLoader.getSystemResourceAsStream("config.properties"));
		} catch (IOException e) {
			throw new RuntimeException();
		}		
	}
	
	static void buildTopologyNetwork(DEMManager dm, String region) {
		loadDEMs(dm, region);
		
		DualTopologyNetwork dtn;
		MESH_MAX_POINTS = Long.parseLong(props.getProperty("memory")) / 8;
		dtn = dm.buildAll();
		dtn.up.cleanup();
		dtn.down.cleanup();
		
		System.err.println("edges in network (up), " + dtn.up.numEdges);
		System.err.println("edges in network (down), " + dtn.down.numEdges);
	}
	
	static void preprocessNetwork(DEMManager dm, String region) {
		if (region != null) {
			loadDEMs(dm, region);
		}

		File folder = new File(DEMManager.props.getProperty("dir_net"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/net not empty!");
		}
		
		MESH_MAX_POINTS = (1 << 26);
		PreprocessNetwork.preprocess(dm, true);
		PreprocessNetwork.preprocess(dm, false);
	}
	
	static void verifyNetwork() {
		DualTopologyNetwork dtn;
		MESH_MAX_POINTS = (1 << 26);
		dtn = DualTopologyNetwork.load(null);

		_verifyNetwork(dtn.up);
		_verifyNetwork(dtn.down);
	}
	
	static void _verifyNetwork(TopologyNetwork tn) {
		int peakClass = (tn.up ? Point.CLASS_SUMMIT : Point.CLASS_PIT);
		int saddleClass = (tn.up ? Point.CLASS_PIT : Point.CLASS_SADDLE);
		
		for (Point p : tn.allPoints()) {
			boolean refersToSelf = false;
			for (long adj : p.adjIx()) {
				if (adj == p.ix) {
					refersToSelf = true;
					break;
				}
			}
			if (refersToSelf) {
				Logging.log("verify: " + p + " refers to self");
				continue;
			}
			
			int pClass = p.classify(tn);
			if (pClass != Point.CLASS_SUMMIT && pClass != Point.CLASS_PIT) {
				if (pClass == Point.CLASS_OTHER && tn.pendingSaddles.contains(p)) {
					// fringe saddle whose leads are all pending -- not connected to rest of network
					// note: these are now filtered out by pagedtoponetwork.load()
					Logging.log("verify: fringe saddle (ok, but should have been pruned)");
					continue;
				}
				
				Logging.log("verify: " + p + " unexpected class " + pClass);
				continue;
			}
			boolean topologyViolation = false;
			for (Point adj : p.adjacent(tn)) {
				if (p == adj) {
					continue;
				}
				
				int adjClass = adj.classify(tn);
				if (adjClass != Point.CLASS_SUMMIT && adjClass != Point.CLASS_PIT) {
					Logging.log("verify: " + p + " adj " + adj + " unexpected class " + adjClass);
					continue;
				}
				if (adjClass == pClass) {
					topologyViolation = true;
					break;
				}
			}
			if (topologyViolation) {
				Logging.log("verify: " + p + " adjacent to same type " + pClass);					
				continue;
			}

			boolean isSaddle = (pClass == saddleClass);
			boolean connected;
			if (isSaddle) {
				int numAdj = (p.adjIx().length + (tn.pendingSaddles.contains(p) ? 1 : 0));
				connected = (numAdj == 2);
			} else {
				// redundant, since if this were false, point would have been flagged above
				connected = (p.adjIx().length >= 1);
			}
			if (!connected) {
				Logging.log("verify: " + p + " [" + pClass + "] insufficiently connected (" + p.adjIx().length + ")");					
				continue;
			}

			// check pending too?
		}
	}
	
	public static interface OnProm {
		void onprom(PromNetwork.PromInfo pi);
	}
	public static interface OnPromParent {
		void onparent(PromNetwork.ParentInfo pi);
	}
	
	static Point getHighest(DEMManager dm, boolean up) {
		Point highest = null;
		Comparator<BasePoint> cmp = BasePoint.cmpElev(up);
		for (DEMFile dem : dm.DEMs) {
			Logging.log("searching " + dem);
			for (DEMFile.Sample s : dem.samples(null)) {
				Point p = new GridPoint(s);
				if (highest == null || cmp.compare(p, highest) > 0) {
					highest = p;
				}					
			}
		}
		Logging.log("highest: " + highest.elev);
		return highest;
	}
	
	//static boolean oldSchool = true;
	static boolean oldSchool = false;
	static void promSearch(final boolean up, double cutoff, DEMManager dm, String region) {
		DualTopologyNetwork dtn;
		MESH_MAX_POINTS = (1 << 26);
		dtn = DualTopologyNetwork.load(null);
		
		//double ANTI_PROM_CUTOFF = PROM_CUTOFF;
		
		TopologyNetwork tn = (up ? dtn.up : dtn.down);
		//TopologyNetwork anti_tn = (!up ? dtn.up : dtn.down);
				
		if (oldSchool) {
			
			for (Point p : tn.allPoints()) {
				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
					continue;
				}
				
				PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
				if (pi != null && pi.prominence() >= cutoff) {
					outputPromPoint(pi, up);
				}
			}
			
		} else {
			
			loadDEMs(dm, region);
			Point highest = getHighest(dm, up);
			
			final DataOutputStream promOut;
			final DataOutputStream thresholdsOut;
			try {
				promOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/prom-" + (up ? "up" : "down"))));
				thresholdsOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/thresh-" + (up ? "up" : "down"))));
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
			PromNetwork.bigOlPromSearch(up, highest, tn, new OnProm() {
				public void onprom(PromInfo pi) {
					outputPromPoint(pi, up);
					
					// note: if we ever have varying cutoffs this will take extra care
					// to make sure we always capture the parent/next highest peaks for
					// peaks right on the edge of the lower-cutoff region
					new PromMeta(pi.p.ix, pi.prominence(), pi.saddle.ix, pi.forward).write(promOut);
					new PromMeta(pi.saddle.ix, pi.prominence(), pi.p.ix, pi.forward).write(promOut);
					
					new ThresholdMeta(pi.path.get(0)).write(thresholdsOut);
				}
			}, new PromNetwork.MSTWriter(up, EdgeIterator.PHASE_MST), cutoff);
			try {
				promOut.close();
				thresholdsOut.close();
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
			
			processMST(highest, dm, up, null);
		}
	}

	static void processMST(Point highest, DEMManager dm, boolean up, String region) {
		if (region != null) {
			loadDEMs(dm, region);
		}
		if (highest == null) {
			highest = getHighest(dm, up);
		}
		
		File folder = new File(DEMManager.props.getProperty("dir_mst"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/mst not empty!");
		}
		PreprocessNetwork.processMST(dm, up, highest);
		folder = new File(DEMManager.props.getProperty("dir_rmst"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/rmst not empty!");
		}
		PreprocessNetwork.processRMST(dm, up, highest);
	}
	
	static void promPass2(Point highest, DEMManager dm, final boolean up, String region) {
		MESH_MAX_POINTS = (1 << 26);
		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_RMST, up, null, new Meta[] {new PromMeta()});
		
		if (oldSchool) {

			for (Point p : tn.allPoints()) {
				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
					continue;
				}

				// we know where the saddle is; would could start the search from there
				// also, using the MST requires less bookkeeping than this search function provides
				PromNetwork.PromInfo pi = PromNetwork.parent(tn, p, up);
				if (pi != null) {
					outputPromParentage(pi, up);
				}

				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
				outputSubsaddles(p, dsi, up);
			}
			
		} else {
			if (region != null) {
				loadDEMs(dm, region);
			}
			if (highest == null) {
				highest = getHighest(dm, up);
			}

			PromNetwork.bigOlPromParentSearch(up, highest, tn, new DEMManager.OnPromParent() {
				public void onparent(ParentInfo pi) {
					PromInfo _pi = new PromInfo(new Point(pi.pIx, 0), null);
					_pi.path = pi.path;
					outputPromParentage(_pi, up);
				}
			});
			
			// just use old-school method for subsaddle search
			for (Point p : tn.allPoints()) {
				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
					continue;
				}
				
				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
				outputSubsaddles(p, dsi, up);
			}

		}
		
		// must operate on *untrimmed* MST!
		tn = null;
		TopologyNetwork tnfull = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta(), new ThresholdMeta()});
		try {
			PrintWriter w = new PrintWriter("/tmp/thresh");
			for (Point p : tnfull.allPoints()) {
				if (p.classify(tnfull) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tnfull.getMeta(p, "thresh") == null) {
					continue;
				}
				
				PromNetwork.PromInfo pi = PromNetwork.promThresh(tnfull, p, up);
				if (pi != null) {
					outputPromThresh(w, pi, up);
				}
			}
			w.close();
		} catch (IOException ioe) { throw new RuntimeException(); }

	}
	
	public static void main(String[] args) {
		
		Logging.init();
		initProps();
		
		DEMManager dm = new DEMManager();
		PROJ = SRTMDEM.SRTMProjection(1.);
		//PROJ = GridFloatDEM.NEDProjection();
		dm.projs.add(PROJ);

		String region = args[1];
		if (args[0].equals("--build")) {
			buildTopologyNetwork(dm, region);
			preprocessNetwork(dm, null);
		} else if (args[0].equals("--prepnet")) {
			preprocessNetwork(dm, region);
		} else if (args[0].equals("--verify")) {
			verifyNetwork();
		} else if (args[0].equals("--searchup") || args[0].equals("--searchdown")) {
			boolean up = args[0].equals("--searchup");
			double cutoff = Double.parseDouble(args[2]);
			promSearch(up, cutoff, dm, region);
		} else if (args[0].equals("--mstup") || args[0].equals("--mstdown")) {
			boolean up = args[0].equals("--mstup");
			processMST(null, dm, up, region);
		} else if (args[0].equals("--searchup2") || args[0].equals("--searchdown2")) {
			boolean up = args[0].equals("--searchup2");
			promPass2(null, dm, up, region);
		} else {
			throw new RuntimeException("operation not specified");
		}
		
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n42w073/floatn42w073_13.flt",
//				10812, 10812, 40.9994444, -73.0005555, 9.259259e-5, 9.259259e-5, true));
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n45w072/floatn45w072_13.flt",
//				10812, 10812, 43.9994444, -72.0005555, 9.259259e-5, 9.259259e-5, true));
		
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
	
	static void outputPromParentage(PromNetwork.PromInfo pi, boolean up) {
		Gson ser = new Gson();
		
		if (pi.min_bound_only || pi.path.isEmpty()) {
			return;
		}
		
		System.out.println(ser.toJson(new ParentData(up, pi.p, pi)));
	}

	static void outputSubsaddles(Point p, List<DomainSaddleInfo> dsi, boolean up) {
		if (dsi.isEmpty()) {
			return;
		}
		Gson ser = new Gson();
		System.out.println(ser.toJson(new SubsaddleData(up, p, dsi)));
	}

	static void outputPromThresh(PrintWriter w, PromNetwork.PromInfo pi, boolean up) {
		if (pi.min_bound_only) {
			return;
		}
		w.write(new PromPoint(pi.p.ix).geo + " " + new PromPoint(pi.path.get(0)).geo + "\n");
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
		
		public PromPoint(double[] c) {
			this.coords = c;
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
		PromPoint _thresh;
		
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
			if (pi.thresholdFactor >= 0) {
				// this is a stop-gap; actually threshold should be determined by following chase from saddle
				double[] last = this.higher_path.get(0);
				double[] nextToLast = this.higher_path.get(1);
				for (int i = 0; i < 2; i++) {
					last[i] = last[i] * pi.thresholdFactor + nextToLast[i] * (1. - pi.thresholdFactor);
				}
			}
			this._thresh = new PromPoint(pi.path.get(0));
			
			this.parent_path = new ArrayList<double[]>();
			for (long k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k));
			}
			
			if (!pi.min_bound_only && !pi.path.isEmpty()) {
				this.higher = new PromPoint(this.higher_path.get(0));
			}
			if (!parentage.min_bound_only && !parentage.path.isEmpty()) {
				this.parent = new PromPoint(parentage.path.get(0));
			}
		}
	}
	
	static class ParentData {
		boolean up;
		PromPoint summit;
		PromPoint parent;
		List<double[]> parent_path;
		String addendum = "parent";
		
		public ParentData(boolean up, Point p, PromNetwork.PromInfo parentage) {
			if (parentage.min_bound_only) {
				throw new IllegalArgumentException();
			}
			
			this.up = up;
			this.summit = new PromPoint(p.ix);

			this.parent_path = new ArrayList<double[]>();
			for (long k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k));
			}
			this.parent = new PromPoint(parentage.path.get(0));
		}
	}
	
	static class SubsaddleData {
		boolean up;
		PromPoint summit;
		String addendum = "subsaddles";

		static class Subsaddle {
			PromPoint saddle;
			PromPoint peak;
			boolean higher;
			boolean domain;
		}
		List<Subsaddle> subsaddles;
		
		public SubsaddleData(boolean up, Point p, List<DomainSaddleInfo> dsis) {
			this.up = up;
			this.summit = new PromPoint(p.ix);

			this.subsaddles = new ArrayList<Subsaddle>();
			for (DomainSaddleInfo dsi : dsis) {
				Subsaddle SS = new Subsaddle();
				SS.saddle = new PromPoint((Point)dsi.saddle, null);
				SS.peak = new PromPoint(dsi.peakIx);
				SS.higher = dsi.isHigher;
				SS.domain = dsi.isDomain;
				this.subsaddles.add(SS);
			}
		}
	}
	
}