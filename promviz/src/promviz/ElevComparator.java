package promviz;

import java.util.Comparator;

public class ElevComparator implements Comparator<Point> {

	static ElevComparator _ = new ElevComparator();
	
	public static int cmp(Point a, Point b) {
		return _.compare(a, b);
	}
	
	@Override
	// FIXME won't work as intended with new index format
	public int compare(Point a, Point b) {
		if (a.elev == b.elev) {
			if (a.ix == b.ix) {
				return 0;
			}
			for (int i = 0; i < 64; i++) {
				long mask = (~0L >> (63 - i));
				long ka = a.ix & mask;
				long kb = b.ix & mask;
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
