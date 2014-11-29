package promviz;

import java.util.Comparator;

public class ElevComparator implements Comparator<Point> {

	static ElevComparator _ = new ElevComparator();
	
	public static int cmp(Point a, Point b) {
		return _.compare(a, b);
	}
	
	public int compare(Point a, Point b) {
		if (a.elev != b.elev) {
			return Float.compare(a.elev, b.elev);
		} else {
			//return Long.compare(a.ix, b.ix); // simple method
			return PointIndex.compare(a.ix, b.ix);
		}
	}
}
