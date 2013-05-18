import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class DEMManager {

	final int MAX_BUCKET_DEPTH = 26; // ~5km square
	final int DEM_TILE_MAX_POINTS = (1 << 18);
	
	List<DEMFile> DEMs;
	
	public DEMManager() {
		DEMs = new ArrayList<DEMFile>();
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
	
	Map<Prefix, Integer> prefixTotals() {
		Map<Prefix, Integer> totals = new HashMap<Prefix, Integer>();
		for (DEMFile dem : DEMs) {
			for (long ix : dem.coords()) {
				Prefix p = new Prefix(ix, MAX_BUCKET_DEPTH);
				if (!totals.containsKey(p)) {
					totals.put(p, 0);
				}
				totals.put(p, totals.get(p) + 1);
			}
		}
		
		for (int i = MAX_BUCKET_DEPTH - 2; i >= 0; i -= 2) {
			Map<Prefix, Integer> summed = new HashMap<Prefix, Integer>();
			for (Entry<Prefix, Integer> e : totals.entrySet()) {
				if (e.getKey().depth != i + 2) {
					continue;
				}

				Prefix p = new Prefix(e.getKey().prefix, i);
				if (!summed.containsKey(p)) {
					summed.put(p, 0);
				}
				summed.put(p, summed.get(p) + e.getValue());
			}
			totals.putAll(summed);
		}
		
		return totals;
	}
	
	public Set<Prefix> partitionDEM() {
		Set<Prefix> prefixes = new HashSet<Prefix>();
		partitionDEM(new Prefix(0, 0), prefixTotals(), prefixes);
		return prefixes;
	}
	
	void partitionDEM(Prefix p, Map<Prefix, Integer> totals, Set<Prefix> prefixes) {
		if (totals.get(p) == null) {
			// empty quad; do nothing
		} else if (p.depth == MAX_BUCKET_DEPTH || totals.get(p) <= DEM_TILE_MAX_POINTS) {
			prefixes.add(p);
		} else {
			for (Prefix child : p.children()) {
				partitionDEM(child, totals, prefixes);
			}
		}
	}

	Map<Prefix, Set<DEMFile>> DEMCoverage(Set<Prefix> prefixes) {
		int max_depth = 0;
		for (Prefix p : prefixes) {
			max_depth = Math.max(max_depth, p.depth);
		}
		
		Map<Prefix, Set<DEMFile>> coverage = new HashMap<Prefix, Set<DEMFile>>();
		for (DEMFile dem : DEMs) {
			for (long ix : dem.coords()) {
				Prefix p = null;
				for (int depth = max_depth; depth >= 0; depth -= 2) {
					p = new Prefix(ix, depth);
					if (prefixes.contains(p)) {
						break;
					}
				}
				
				if (!coverage.containsKey(p)) {
					coverage.put(p, new HashSet<DEMFile>());
				}
				coverage.get(p).add(dem);
			}
		}
		return coverage;
	}
	
	public static void main(String[] args) {
		DEMManager dm = new DEMManager();
		dm.DEMs.add(new DEMFile("4075", 2001, 2001, 40, -75, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("3575", 2001, 2001, 35, -75, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("4080", 2001, 2001, 40, -80, .0025, .0025, true));
		dm.DEMs.add(new DEMFile("3580", 2001, 2001, 35, -80, .0025, .0025, true));

		Set<Prefix> pl = dm.partitionDEM();
		Map<Prefix, Set<DEMFile>> coverage = dm.DEMCoverage(pl);
		for (Entry<Prefix, Set<DEMFile>> e : coverage.entrySet()) {
			System.out.println(e.getKey());
			for (DEMFile df : e.getValue()) {
				System.out.println("  " + df.path);
			}
		}
	}
}
