package promviz;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Iterables;

import promviz.util.DefaultMap;


public class DEMManager {

	final int MAX_BUCKET_DEPTH = 26; // ~5km square
	final int DEM_TILE_MAX_POINTS = (1 << 20);
	
	List<DEMFile> DEMs;
	
	public DEMManager() {
		DEMs = new ArrayList<DEMFile>();
	}
	
	TopologyNetwork buildAll(boolean up) {
		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
		Set<Prefix> yetToProcess = coverage.keySet();

		Mesh m = new Mesh();
		TopologyNetwork tn = new TopologyNetwork(up);
		
		while (yetToProcess.size() > 0) {
			Prefix nextPrefix = getNextPrefix(yetToProcess, tn);
			Set<Point> data = loadPrefix(nextPrefix, coverage);
			
			System.out.println(String.format("loaded %s (%d)", nextPrefix, data.size()));
			// load data for prefix into mesh
			// if mesh total point count is exceeded, remove old (how to keep track of old?)
			// update pendings if available
			// process new chunk
			
			yetToProcess.remove(nextPrefix);
		}
		
		return null;
	}
	
	Set<Point> loadPrefix(Prefix prefix, Map<Prefix, Set<DEMFile>> coverage) {
		Set<Point> points = new HashSet<Point>();
		for (DEMFile dem : coverage.get(prefix)) {
			Iterables.addAll(points, dem.samples(prefix));
		}
		return points;
	}
	
	Prefix getNextPrefix(Set<Prefix> yetToProcess, TopologyNetwork tn) {
		Map<Prefix, Integer> frontierTotals = new DefaultMap<Prefix, Integer>() {
			@Override
			public Integer defaultValue() {
				return 0;
			}
		};
		for (Set<Long> terms : tn.pending.values()) {
			for (Long term : terms) {
				Set<Prefix> frontiers = new HashSet<Prefix>();
				for (Long adj : adjacency(term)) {
					for (Prefix potential : yetToProcess) {
						if (potential.isParent(adj)) {
							frontiers.add(potential);
							break;
						}
					}
				}
				for (Prefix p : frontiers) {
					frontierTotals.put(p, frontierTotals.get(p) + 1);
				}
			}
		}
		
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
				
		DEMManager dm = new DEMManager();
		dm.DEMs.add(new DEMFile("/mnt/ext/pvdata/data2ne", 2001, 2001, 40, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("3575", 2001, 2001, 35, -75, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("4080", 2001, 2001, 40, -80, .0025, .0025, true));
//		dm.DEMs.add(new DEMFile("3580", 2001, 2001, 35, -80, .0025, .0025, true));

		dm.buildAll(true);
	}
}
