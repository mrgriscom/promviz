package promviz;
import java.util.ArrayList;
import java.util.List;



public class Point implements Comparable<Point> {

	public static final int CLASS_OTHER = 0;
	public static final int CLASS_SUMMIT = 1;
	public static final int CLASS_PIT = 2;
	public static final int CLASS_SADDLE = 3;
	public static final int CLASS_INDETERMINATE = 4;
	
	long ix;
	float elev; // meters

	/* list of point geocodes adjacent to this point, listed in a clockwise direction
	 * if any two consecutive points are NOT adjacent to each other (i.e., the
	 * three points do not form a Tri), this list will have one or more nulls
	 * inserted between them
	 */
	long[] _adjacent = new long[0];

	public Point(long ix, double elev) {
		this.ix = ix;
		this.elev = (float)elev;
	}
	
	public long[] adjIx() {
		return _adjacent;
	}
	
	public List<Point> adjacent(IMesh m) {
		List<Point> adj = new ArrayList<Point>();
		for (long geocode : adjIx()) {
			adj.add(geocode != -1 ? m.get(geocode) : null);
		}
		return adj;
	}
	
	public int classify(IMesh m) {
		boolean is_summit = true;
		boolean is_pit = true;

		List<Point> adjacent = this.adjacent(m);
		for (Point p : adjacent) {
			if (p == null)  {
				return CLASS_INDETERMINATE;
			}
			
			if (ElevComparator.cmp(p, this) == 1) {
				is_summit = false;
			} else if (ElevComparator.cmp(p, this) == -1) {
				is_pit = false;
			}
		}
		
		if (is_summit) {
			return CLASS_SUMMIT;
		} else if (is_pit) {
			return CLASS_PIT;
		}
		
		int transitions = 0;
		for (int i = 0; i < adjacent.size(); i++) {
			Point p0 = adjacent.get(i);
			Point p1 = adjacent.get((i + 1) % adjacent.size());
			
			if (ElevComparator.cmp(p0, this) != ElevComparator.cmp(p1, this)) {
				transitions++;
			}
		}
		if (transitions > 2) {
			return CLASS_SADDLE;
		}
		
		return CLASS_OTHER;
	}
	
	List<Point> leads(IMesh m, boolean up) {
		List<Point> adjacent = this.adjacent(m);
		int[] cohort = new int[adjacent.size()];
		int current_cohort = 0;
		for (int i = 0; i < adjacent.size(); i++) {
			Point p = adjacent.get(i);
			Point p_prev = (i > 0 ? adjacent.get(i - 1) : null);
			if (p_prev != null && ElevComparator.cmp(p, this) != ElevComparator.cmp(p_prev, this)) {
				current_cohort++;
			}
			cohort[i] = current_cohort;
		}
		int num_cohorts = ((current_cohort + 1) / 2) * 2; // if cur_cohort is odd it means the 'seam' fell in the middle of one
		for (int i = 0; i < adjacent.size(); i++) {
			cohort[i] = cohort[i] % num_cohorts;
		}
		
		List<Point> L = new ArrayList<Point>();
		for (int cur_cohort = 0; cur_cohort < num_cohorts; cur_cohort++) {
			Point best = null;
			for (int i = 0; i < adjacent.size(); i++) {
				if (cohort[i] != cur_cohort) {
					continue;
				}
				
				Point p = adjacent.get(i);
				if (best == null || Math.abs(p.elev - this.elev) > Math.abs(best.elev - this.elev)) {
					best = p;
				}
			}
			if (ElevComparator.cmp(best, this) == (up ? 1 : -1)) {
				L.add(best);
			}
		}
		return L;
	}
	
	public String toString() {
//		double[] coords = this.coords();
//		return String.format("<%f,%f %f %dadj>", coords[0], coords[1], this.elev, this._adjacent.length);
		return this.ix + " (" + this.elev + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Point) {
			return this.ix == ((Point)o).ix;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return Long.valueOf(this.ix).hashCode();
	}
	
	@Override
	public int compareTo(Point p) {
		return Long.valueOf(this.ix).compareTo(p.ix);
	}
}
