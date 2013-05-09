import java.util.ArrayList;
import java.util.List;



public class Point {

	public static final int CLASS_OTHER = 0;
	public static final int CLASS_SUMMIT = 1;
	public static final int CLASS_PIT = 2;
	public static final int CLASS_SADDLE = 3;
	public static final int CLASS_INDETERMINATE = 4;
	
	//todo: geopoint index (as long) instead of lat/lon
	double lat;
	double lon;
	float elev; // meters

	/* list of points adjacent to this point, listed in a clockwise direction
	 * if any two consecutive points are NOT adjacent to each other (i.e., the
	 * three points do not form a Tri), this list will have one or more nulls
	 * inserted between them
	 */
	Point[] adjacent;

	public Point(double lat, double lon, double elev) {
		this.lat = lat;
		this.lon = lon;
		this.elev = (float)elev;
	}
	
	public int classify() {
		boolean is_summit = true;
		boolean is_pit = true;
		
		for (Point p : adjacent) {
			if (p == null)  {
				return CLASS_INDETERMINATE;
			}
			
			if (p.elev > this.elev) {
				is_summit = false;
			} else if (p.elev < this.elev) {
				is_pit = false;
			}
		}
		
		if (is_summit) {
			return CLASS_SUMMIT;
		} else if (is_pit) {
			return CLASS_PIT;
		}
		
		int transitions = 0;
		for (int i = 0; i < adjacent.length; i++) {
			Point p0 = adjacent[i];
			Point p1 = adjacent[(i + 1) % adjacent.length];
			
			if ((p0.elev > this.elev) != (p1.elev > this.elev)) {
				transitions++;
			}
		}
		if (transitions > 2) {
			return CLASS_SADDLE;
		}
		
		return CLASS_OTHER;
	}
	
	List<Point> leads(boolean up) {
		int[] cohort = new int[adjacent.length];
		int current_cohort = 0;
		for (int i = 0; i < adjacent.length; i++) {
			Point p = adjacent[i];
			Point p_prev = (i > 0 ? adjacent[i - 1] : null);
			if (p_prev != null && (p.elev > this.elev) != (p_prev.elev > this.elev)) {
				current_cohort++;
			}
			cohort[i] = current_cohort;
		}
		int num_cohorts = ((current_cohort + 1) / 2) * 2; // if cur_cohort is odd it means the 'seam' fell in the middle of one
		for (int i = 0; i < adjacent.length; i++) {
			cohort[i] = cohort[i] % num_cohorts;
		}
		
		List<Point> L = new ArrayList<Point>();
		for (int cur_cohort = 0; cur_cohort < num_cohorts; cur_cohort++) {
			Point best = null;
			for (int i = 0; i < adjacent.length; i++) {
				if (cohort[i] != cur_cohort) {
					continue;
				}
				
				Point p = adjacent[i];
				if (best == null || Math.abs(p.elev - this.elev) > Math.abs(best.elev - this.elev)) {
					best = p;
				}
			}
			if ((best.elev > this.elev) == up) {
				L.add(best);
			}
		}
		return L;
	}
}
