package promviz.util;

import promviz.Point;
import promviz.PointIndex;

public class Debug {

	public static boolean pointIs(Point p, String geocode) {
		boolean eq = (p != null && PointIndex.geocode(p.ix).equals(geocode));
		if (eq) {
			int[] pcs = PointIndex.split(p.ix);
			if (pcs[3] != 0) {
				Logging.log("WARNING: geocode not unique");
			}
		}
		return eq;
	}

	public static boolean pointIsIx(Point p, String ix) {
		return p != null && Util.print(p.ix).equals(ix);
	}
	
	public static boolean edgeIs(Point a, Point b, String ix1, String ix2) {
		return (pointIsIx(a, ix1) && pointIsIx(b, ix2)) || (pointIsIx(a, ix2) && pointIsIx(b, ix1));
	}

}
