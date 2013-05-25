package promviz;

import java.util.Comparator;

public class ElevComparator implements Comparator<Point> {

	static ElevComparator _ = new ElevComparator();
	
	public static int cmp(Point a, Point b) {
		return _.compare(a, b);
	}
	
	@Override
	public int compare(Point a, Point b) {
		if (a.elev == b.elev) {
			if (a.geocode == b.geocode) {
				return 0;
			}
			for (int i = 0; i < 64; i++) {
				long mask = (~0L >> (63 - i));
				long ka = a.geocode & mask;
				long kb = b.geocode & mask;
				if (ka != kb) {
					return (ka < kb ? -1 : 1);
				}
			}
			throw new RuntimeException();
		} else {
			return (a.elev < b.elev ? -1 : 1);
		}
	}
}
