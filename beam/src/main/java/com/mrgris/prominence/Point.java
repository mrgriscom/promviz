package com.mrgris.prominence;

import java.util.Comparator;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.mrgris.prominence.util.ReverseComparator;
import com.mrgris.prominence.util.Util;

/* basic representation of a point/elevation sample in isolation */
@DefaultCoder(AvroCoder.class)
public class Point {
	
	public long ix;      // a PointIndex
	public float elev;   // m
	public int isodist;  // metric representing distance to closest edge of flat region; more specifically:
	/* if p is the closest point on the edge of the flat area (a contiguous set of points all the same elevation),
	 * and n is the distance to p is some arbitrary unit, then:
	 * if p is on an upward-curving lip/edge (all points adjacent to p are >= elevation of p), isodist = MAX_INT - n
	 * if p is on a downward-curving lip/edge, isodist = MIN_INT + n
	 * if p is adjacent to points both higher and lower, isodist = 0
	 */

	// for deserialization
	public Point() {}
	
	public Point(long ix, float elev, int isodist) {
		this.ix = ix;
		this.elev = elev;
		this.isodist = isodist;
	}
	
	public Point(Point p) {
		this(p.ix, p.elev, p.isodist);
	}
	
	public static long toIx(Point p) {
		return p != null ? p.ix : PointIndex.NULL;
	}
	
	public double relCompare(Point a, Point b) {
		double diffA = a.elev - this.elev;
		double diffB = b.elev - this.elev;
		if (diffA != 0.0 || diffB != 0.0) {
			return diffA / diffB;
		}
		diffA = (double)a.isodist - this.isodist;
		diffB = (double)b.isodist - this.isodist;
		if (diffA != 0.0 || diffB != 0.0) {
			return diffA / diffB;
		}
		// TODO handle point-ix based comparison
		return 1;
	}
	
	// a point's "height" is effectively (elev, isodist, pseudorandom id (geocode with reversed bits, seq #)
	public static int compareElev(Point a, Point b) {
		if (a.elev != b.elev) {
			return Float.compare(a.elev, b.elev);
		} else if (a.isodist != b.isodist) {
			return Integer.compare(a.isodist, b.isodist);
		} else {
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
