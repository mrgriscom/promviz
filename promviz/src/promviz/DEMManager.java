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

import com.google.common.collect.Iterables;


public class DEMManager {

	final int MAX_BUCKET_DEPTH = 26; // ~5km square
	final int DEM_TILE_MAX_POINTS = (1 << 20);
	final int MESH_MAX_POINTS = (1 << 22);
	
	List<DEMFile> DEMs;
	
	public DEMManager() {
		DEMs = new ArrayList<DEMFile>();
	}
	
	TopologyNetwork buildAll(boolean up) {
		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
		Logging.log("partitioning complete");
		Set<Prefix> allPrefixes = coverage.keySet();
		Set<Prefix> yetToProcess = coverage.keySet();

		PagedMesh m = new PagedMesh(MESH_MAX_POINTS);
		TopologyNetwork tn = new TopologyNetwork(up);
		while (!tn.complete(allPrefixes, yetToProcess)) {
			Prefix nextPrefix = getNextPrefix(allPrefixes, yetToProcess, tn);
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
	
	Prefix getNextPrefix(Set<Prefix> allPrefixes, Set<Prefix> yetToProcess, TopologyNetwork tn) {
		Map<Prefix, Integer> frontierTotals = tn.tallyPending(allPrefixes);
		Prefix mostInDemand = null;
		for (Entry<Prefix, Integer> e : frontierTotals.entrySet()) {
			if (mostInDemand == null || e.getValue() > frontierTotals.get(mostInDemand)) {
				mostInDemand = e.getKey();
			}
		}
		if (mostInDemand == null) {
			mostInDemand = yetToProcess.iterator().next();
		}
		return mostInDemand;
	}
	
	public static long[] adjacency(Long ix) {
		double[] ll = GeoCode.toCoord(ix);
		final double STEP = .0025; // violates DRY
		int r = (int)Math.round((ll[0] + 360. * 10.) / STEP);
		int c = (int)Math.round((ll[1] + 360. * 10.) / STEP);
		// must snap lat/lon to grid or geocodes will not match in least-significant bits
		ll[0] = r * STEP - 360. * 10.;
		ll[1] = c * STEP - 360. * 10.;
		
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
		boolean fully_connected = (r + c) % 2 == 0;
		for (int[] offset : offsets) {
			boolean diagonal_connection = (offset[0] + offset[1] + 2) % 2 == 0;
			if (fully_connected || !diagonal_connection) {
				adj.add(new double[] {ll[0] + STEP * offset[1], ll[1] + STEP * offset[0]});
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

	
	
	public static void main(String[] args) {
		
		Logging.init();
		
		DEMManager dm = new DEMManager();
		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w075ds3", 2001, 2001, 40, -75, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n45w075ds3", 2001, 2001, 45, -75, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/n40w080ds3", 2001, 2001, 40, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("3575", 2001, 2001, 35, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("4080", 2001, 2001, 40, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("3580", 2001, 2001, 35, -80, .0025, .0025, true));

		boolean up = true;
		TopologyNetwork tn = dm.buildAll(up);
		System.err.println(tn.points.size() + " nodes in network");
		for (Point p : tn.points.values()) {
			if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
				continue;
			}
			
			double PROM_CUTOFF = 50.;
			PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
			if (pi != null && pi.prominence() > PROM_CUTOFF) {
				StringBuilder path = new StringBuilder();
				for (int i = 0; i < pi.path.size(); i++) {
					double[] c = pi.path.get(i).coords();
					path.append(String.format("[%f, %f]", c[0], c[1]) + (i < pi.path.size() - 1 ? ", " : ""));
				}
				double[] peak = p.coords();
				double[] saddle = pi.saddle.coords();
				System.out.println(String.format(
						"{\"summit\": [%.5f, %.5f], \"elev\": %.1f, \"prom\": %.1f, \"saddle\": [%.5f, %.5f], \"min_bound\": %s, \"path\": [%s]}",
						peak[0], peak[1], p.elev, pi.prominence(), saddle[0], saddle[1], pi.min_bound_only ? "true" : "false", path.toString()));
			}
		}
	}
}
