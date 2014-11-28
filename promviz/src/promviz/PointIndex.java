package promviz;

public class PointIndex {

	static final int BITS_SEQ = 3;
	static final int BITS_X = 24;
	static final int BITS_Y = 24;
	static final int BITS_PROJ = 64 - BITS_X - BITS_Y - BITS_SEQ;

	static final int OFFSET_SEQ = 0;
	static final int OFFSET_Y = OFFSET_SEQ + BITS_SEQ;
	static final int OFFSET_X = OFFSET_Y + BITS_Y;
	static final int OFFSET_PROJ = OFFSET_X + BITS_X;
	
	public static long make(int projId, int x, int y) {
		return make(projId, x, y, 0);
	}
	
	public static long make(int projId, int x, int y, int seq) {
		return ((long)(projId & ~(~0 << BITS_PROJ)) << OFFSET_PROJ) |
			   ((long)(x      & ~(~0 << BITS_X   )) << OFFSET_X   ) |
			   ((long)(y      & ~(~0 << BITS_Y   )) << OFFSET_Y   ) |
			    (long)(seq    & ~(~0 << BITS_SEQ ));
	}
	
	public static int[] split(long ix) {
		int projId = (int)(ix >>> OFFSET_PROJ);
		int x =      (int)(ix >>  OFFSET_X    & ~(~0 << BITS_X  ));
		int y =      (int)(ix >>  OFFSET_Y    & ~(~0 << BITS_Y  ));
		int seq =    (int)(ix                 & ~(~0 << BITS_SEQ));
		
		if (x >= 1 << (BITS_X - 1)) {
			x -= (1 << BITS_X);
		}
		if (y >= 1 << (BITS_Y - 1)) {
			y -= (1 << BITS_Y);
		}
		
		return new int[] {projId, x, y, seq};
	}
	
	public static double[] toLatLon(long ix) {
		int[] _ix = split(ix);
		return DEMManager.PROJ.fromGrid(_ix[1], _ix[2]);
	}
	
	public static long truncate(long ix, int depth) {
		int[] c = split(ix);
		int x = c[1] | (~0 << depth);
		int y = c[2] | (~0 << depth);
		return make(c[0], x, y);
	}
	
	public static String geocode(long ix) {
		double[] ll = PointIndex.toLatLon(ix);
		return GeoCode.print(GeoCode.fromCoord(ll[0], ll[1]));
	}
	
	static void testA(int proj, int x, int y) {
		long ix = make(proj, x, y);
		int[] reverse = split(ix);
		System.out.println(String.format("%d %d %d => %016x", proj, x, y, make(proj, x, y)));
		if (proj != reverse[0] || x != reverse[1] || y != reverse[2]) {
			throw new RuntimeException();
		}
	}
	
	public static void main(String[] args) {
		testA(0, 0, 0);
		testA(1, 1, 1);
		testA(2, -1, 1);
		testA(3, 1, -1);
		testA(4, -1, -1);
		testA(65535, 8388000, -8388000);
	}

}
