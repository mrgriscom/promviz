package promviz;

import java.util.Comparator;

import promviz.util.ReverseComparator;
import promviz.util.Util;

/* basic representation of a point/elevation sample in isolation */
public class Point {
	
	long ix;      // a PointIndex
	float elev;   // m
	int isodist;  // metric representing distance to closest edge of flat region; more specifically:
	/* if p is the closest point on the edge of the flat area (a contiguous set of points all the same elevation),
	 * and n is the distance to p is some arbitrary unit, then:
	 * if p is on an upward-curving lip/edge (all points adjacent to p are >= elevation of p), isodist = MAX_INT - n
	 * if p is on a downward-curving lip/edge, isodist = MIN_INT + n
	 * if p is adjacent to points both higher and lower, isodist = 0
	 */

	public Point(long ix, float elev) {
		this(ix, elev, 0);

		// placeholder calc for isodist
		int[] c = PointIndex.split(ix);
		this.isodist = c[1] - c[2];
	}
	
	public Point(long ix, float elev, int isodist) {
		this.ix = ix;
		this.elev = elev;
		this.isodist = isodist;
	}
	
	public Point(Point p) {
		this(p.ix, p.elev, p.isodist);
	}
	
	public static int compareElev(Point a, Point b) {
		if (a.elev != b.elev) {
			return Float.compare(a.elev, b.elev);
		} else if (a.isodist != b.isodist) {
			return Integer.compare(a.isodist, b.isodist);
		} else {
			//return Long.compare(a.ix, b.ix); // simple method
			return PointIndex.compare(a.ix, b.ix);
		}
	}

	public int compareElev(Point p) {
		return compareElev(this, p);
	}
	
	static Comparator<Point> _cmpElev = new Comparator<Point>() {
		public int compare(Point a, Point b) {
			return compareElev(a, b);
		}
	};
	static Comparator<Point> _invCmpElev = new ReverseComparator<Point>(_cmpElev);
	public static Comparator<Point> cmpElev(boolean up) {
		return up ? _cmpElev : _invCmpElev;
	}
	
	public String toString() {
		double[] c = PointIndex.toLatLon(this.ix);
		return String.format("%s %s %s %.5f %.5f (%.1f)",
				PointIndex.geocode(this.ix), Util.print(this.ix), PointIndex.toString(this.ix),
				c[0], c[1], this.elev);
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
	

	
}
