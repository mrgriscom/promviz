package promviz;

import java.util.Comparator;

import promviz.util.ReverseComparator;

/* basic representation of a point/elevation sample in isolation */
public class BasePoint {
	
	long ix;      // a PointIndex
	float elev;   // m
	int isodist;  // metric representing distance to closest edge of flat region; more specifically:
	/* if p is the closest point on the edge of the flat area (a contiguous set of points all the same elevation),
	 * and n is the distance to p is some arbitrary unit, then:
	 * if p is on an upward-curving lip/edge (all points adjacent to p are >= elevation of p), isodist = MAX_INT - n
	 * if p is on a downward-curving lip/edge, isodist = MIN_INT + n
	 * if p is adjacent to points both higher and lower, isodist = 0
	 */

	public BasePoint(long ix, float elev) {
		this(ix, elev, 0);

		// temporary
		int[] c = PointIndex.split(ix);
		this.isodist = c[1] - c[2];
	}
	
	public BasePoint(long ix, float elev, int isodist) {
		this.ix = ix;
		this.elev = elev;
		this.isodist = isodist;
	}
	
	public static int compareElev(BasePoint a, BasePoint b) {
		if (a.elev != b.elev) {
			return Float.compare(a.elev, b.elev);
		} else if (a.isodist != b.isodist) {
			return Integer.compare(a.isodist, b.isodist);
		} else {
			//return Long.compare(a.ix, b.ix); // simple method
			return PointIndex.compare(a.ix, b.ix);
		}
	}

	public int compareElev(BasePoint p) {
		return compareElev(this, p);
	}
	
	static Comparator<BasePoint> _cmpElev = new Comparator<BasePoint>() {
		public int compare(BasePoint a, BasePoint b) {
			return compareElev(a, b);
		}
	};
	static Comparator<BasePoint> _invCmpElev = new ReverseComparator<BasePoint>(_cmpElev);
	public static Comparator<BasePoint> cmpElev(boolean up) {
		return up ? _cmpElev : _invCmpElev;
	}
	
	public String toString() {
		double[] c = PointIndex.toLatLon(this.ix);
		return String.format("%s %016x %s %.5f %.5f (%.1f)", PointIndex.geocode(this.ix), this.ix, PointIndex.print(this.ix), c[0], c[1], this.elev);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BasePoint) {
			return this.ix == ((BasePoint)o).ix;
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return Long.valueOf(this.ix).hashCode();
	}
	

	
}
