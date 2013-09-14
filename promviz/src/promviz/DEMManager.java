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

import com.google.common.collect.Iterables;


public class DEMManager {	
	final int MAX_BUCKET_DEPTH = 26; // ~5km square
	final int DEM_TILE_MAX_POINTS = (1 << 20);
	final int MESH_MAX_POINTS = (1 << 23);
	
	List<DEMFile> DEMs;
	
	public DEMManager() {
		DEMs = new ArrayList<DEMFile>();
	}
	
	DualTopologyNetwork buildAll() {
		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
		Set<Prefix> allPrefixes = coverage.keySet();
		Logging.log("partitioning complete");
		Set<Prefix> yetToProcess = new HashSet<Prefix>(coverage.keySet()); //mutable!

		PagedMesh m = new PagedMesh(MESH_MAX_POINTS);
		DualTopologyNetwork tn = new DualTopologyNetwork(this);
		while (!tn.complete(allPrefixes, yetToProcess)) {
			Prefix nextPrefix = getNextPrefix(allPrefixes, yetToProcess, tn, m);
			if (m.isLoaded(nextPrefix)) {
				Logging.log("prefix already loaded!");
				continue;
			}
			
			Set<Point> data = loadPrefix(nextPrefix, coverage);
			m.loadPage(nextPrefix, data);
			tn.buildPartial(m, yetToProcess.contains(nextPrefix) ? data : null);
			yetToProcess.remove(nextPrefix);
		}
		return tn;
	}
	
	Set<Point> loadPrefix(Prefix prefix, Map<Prefix, Set<DEMFile>> coverage) {
		Logging.log(String.format("loading segment %s...", prefix));
		Set<Point> points = new HashSet<Point>();
		for (DEMFile dem : coverage.get(prefix)) {
			Iterables.addAll(points, dem.samples(prefix));
			Logging.log(String.format("  scanned DEM %s", dem.path));
		}
						
		Logging.log(String.format("loading complete (%d points)", points.size()));
		return points;
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
		double[] ll = GeoCode.toCoord(ix);
		int r = (int)Math.round(ll[0] / DEMFile.STEP);
		int c = (int)Math.round(ll[1] / DEMFile.STEP);
		
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
		List<double[]> adj = new ArrayList<double[]>();
		boolean fully_connected = (Util.mod(r + c, 2) == 0);
		for (int[] offset : offsets) {
			boolean diagonal_connection = (Util.mod(offset[0] + offset[1], 2) == 0);
			if (fully_connected || !diagonal_connection) {
				adj.add(DEMFile.gridToCoord(r + offset[1], c + offset[0], true));
			}
		}
		long[] adjix = new long[adj.size()];
		for (int i = 0; i < adjix.length; i++) {
			double[] coord = adj.get(i);
			adjix[i] = GeoCode.fromCoord(coord[0], coord[1]);
		}
		return adjix;
	}
	
	class Prefix implements Comparable<Prefix> {
		long prefix;
		int depth;

		public Prefix(long ix, int depth) {
			this.prefix = GeoCode.prefix(ix, depth);
			this.depth = depth;
		}
		
		Prefix child(int quad) {
			return new Prefix(prefix | ((long)quad << (64 - depth - 2)), depth + 2);
		}
		
		public Prefix[] children() {
			return new Prefix[] {
				child(0),
				child(1),
				child(2),
				child(3),
			};
		}
		
		public boolean isParent(long ix) {
			return (GeoCode.prefix(ix, this.depth) == this.prefix);
		}
		
		public boolean equals(Object o) {
			if (o instanceof Prefix) {
				Prefix p = (Prefix)o;
				return (this.prefix == p.prefix && this.depth == p.depth);
			} else {
				return false;
			}
		}
		
		public int hashCode() {
			return Long.valueOf(this.prefix | this.depth).hashCode();
		}
		
		public int compareTo(Prefix p) {
			int result = Integer.valueOf(this.depth).compareTo(p.depth);
			if (result == 0) {
				result = Long.valueOf(this.prefix).compareTo(p.prefix);
			}
			return result;
		}
		
		public String toString() {
			return String.format("%02d:%s", depth, GeoCode.print(prefix));
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
	
	Map<Prefix, PartitionCounter> partitionBucketing() {
		class PartitionMap extends DefaultMap<Prefix, PartitionCounter> {
			@Override
			public PartitionCounter defaultValue() {
				return new PartitionCounter();
			}
		};
		
		PartitionMap partitions = new PartitionMap();
		for (DEMFile dem : DEMs) {
			for (long ix : dem.coords()) {
				Prefix p = new Prefix(ix, MAX_BUCKET_DEPTH);
				partitions.get(p).addSample(dem);
			}
		}
		
		for (int i = MAX_BUCKET_DEPTH - 2; i >= 0; i -= 2) {
			PartitionMap tmp = new PartitionMap();
			for (Entry<Prefix, PartitionCounter> e : partitions.entrySet()) {
				if (e.getKey().depth != i + 2) {
					continue;
				}

				Prefix p = new Prefix(e.getKey().prefix, i);
				tmp.get(p).combine(e.getValue());
			}
			partitions.putAll(tmp);
		}
		
		return partitions;
	}
	
	public Map<Prefix, Set<DEMFile>> partitionDEM() {
		Map<Prefix, Set<DEMFile>> partitioning = new HashMap<Prefix, Set<DEMFile>>();
		partitionDEM(new Prefix(0, 0), partitionBucketing(), partitioning);
		return partitioning;
	}
	
	void partitionDEM(Prefix prefix, Map<Prefix, PartitionCounter> buckets, Map<Prefix, Set<DEMFile>> partitioning) {
		if (!buckets.containsKey(prefix)) {
			// empty quad; do nothing
		} else if (prefix.depth == MAX_BUCKET_DEPTH || buckets.get(prefix).count <= DEM_TILE_MAX_POINTS) {
			partitioning.put(prefix, buckets.get(prefix).coverage);
		} else {
			for (Prefix child : prefix.children()) {
				partitionDEM(child, buckets, partitioning);
			}
		}
	}

	// HACKY
	boolean inScope(long ix) {
		double[] c = GeoCode.toCoord(ix);
		for (DEMFile dem : DEMs) {
			if (c[0] >= dem.lat0 && c[1] >= dem.lon0 &&
					c[0] <= (dem.lat0 + dem.dy * (dem.height - 1)) &&
					c[1] <= (dem.lon0 + dem.dx * (dem.width - 1))) {
				return true;
			}
		}
		return false;
	}
	
	public static void main(String[] args) {
		
		Logging.init();
		
		DEMManager dm = new DEMManager();
		DEMFile.STEP = .0025;
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w125ds3", 2001, 2001, 30, -125, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w120ds3", 2001, 2001, 30, -120, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w115ds3", 2001, 2001, 30, -115, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w110ds3", 2001, 2001, 30, -110, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w125ds3", 2001, 2001, 35, -125, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w120ds3", 2001, 2001, 35, -120, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w115ds3", 2001, 2001, 35, -115, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w110ds3", 2001, 2001, 35, -110, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w125ds3", 2001, 2001, 40, -125, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w120ds3", 2001, 2001, 40, -120, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w115ds3", 2001, 2001, 40, -115, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w110ds3", 2001, 2001, 40, -110, .0025, .0025, true));

//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w090ds3", 2001, 2001, 30, -90, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w090ds3", 2001, 2001, 35, -90, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w090ds3", 2001, 2001, 40, -90, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n25w085ds3", 2001, 2001, 25, -85, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w085ds3", 2001, 2001, 30, -85, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w085ds3", 2001, 2001, 35, -85, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w085ds3", 2001, 2001, 40, -85, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n30w080ds3", 2001, 2001, 30, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w080ds3", 2001, 2001, 35, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w080ds3", 2001, 2001, 40, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n35w075ds3", 2001, 2001, 35, -75, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w075ds3", 2001, 2001, 40, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w075ds3", 2001, 2001, 45, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w070ds3", 2001, 2001, 40, -70, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w070ds3", 2001, 2001, 45, -70, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w065ds3", 2001, 2001, 40, -65, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w065ds3", 2001, 2001, 45, -65, .0025, .0025, true));

//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w080ds3", 2001, 2001, 45, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n50w080ds3", 2001, 2001, 50, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n55w080ds3", 2001, 2001, 55, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w075ds3", 2001, 2001, 45, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n50w075ds3", 2001, 2001, 50, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n55w075ds3", 2001, 2001, 55, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w070ds3", 2001, 2001, 45, -70, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n50w070ds3", 2001, 2001, 50, -70, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n55w070ds3", 2001, 2001, 55, -70, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n50w065ds3", 2001, 2001, 50, -65, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n55w065ds3", 2001, 2001, 55, -65, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n50w060ds3", 2001, 2001, 50, -60, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n55w060ds3", 2001, 2001, 55, -60, .0025, .0025, true));
		
		
		boolean up = true;
		DualTopologyNetwork dtn = dm.buildAll();
		System.err.println(dtn.up.points.size() + " nodes in network (up)");
		System.err.println(dtn.down.points.size() + " nodes in network (down)");

		double PROM_CUTOFF = 50.;
		
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
		
		for (Entry<Point, PromNetwork.PromInfo> e : prominentPoints.entrySet()) {
			Point p = e.getKey();
			PromNetwork.PromInfo pi = e.getValue();
			
			PromNetwork.PromInfo parentage = PromNetwork.parent(tn, p, up, prominentPoints);
			
//				List<String> domainLimits = new ArrayList<String>();
//				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
//					domainLimits.add(pathToStr(ro));					
//				}
				
			double[] peak = p.coords();
			double[] saddle = pi.saddle.coords();
			System.out.println(String.format(
					"{\"summit\": [%.5f, %.5f], \"elev\": %.1f, \"prom\": %.1f, \"saddle\": [%.5f, %.5f], \"min_bound\": %s, \"linepath\": %s, \"summitgeo\": \"%s\", \"saddlegeo\": \"%s\", \"parentpath\": %s, \"runoff\": %s}",
					peak[0], peak[1],
					p.elev,
					pi.prominence(),
					saddle[0], saddle[1],
					pi.min_bound_only ? "true" : "false",
					pathToStr(pi.path),
					GeoCode.print(p.geocode),
					GeoCode.print(pi.saddle.geocode),
					pathToStr(parentage.path),
					"null" //String.format("[%s]", join(domainLimits, ", "))
				));
		}
	}
	
	static String join(List<String> strs, String sep) {
		StringBuilder joined = new StringBuilder();
		for (int i = 0; i < strs.size(); i++) {
			joined.append(strs.get(i) + (i < strs.size() - 1 ? sep : ""));
		}
		return joined.toString();
	}
	
	static String pathToStr(List<Point> points) {
		List<String> strs = new ArrayList<String>();
		for (int i = 0; i < points.size(); i++) {
			double[] c = points.get(i).coords();
			strs.add(String.format("[%f, %f]", c[0], c[1]));
		}
		return "[" + join(strs, ", ") + "]";
	}
}
