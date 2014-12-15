package promviz;

import java.util.ArrayList;
import java.util.List;

public class PointIndex {
	
	static final int BITS_SEQ = 3; // signed
	static final int BITS_X = 24;  // signed
	static final int BITS_Y = 24;  // signed
	static final int BITS_PROJ = 64 - BITS_X - BITS_Y - BITS_SEQ;

	static final int OFFSET_SEQ = 0;
	static final int OFFSET_Y = OFFSET_SEQ + BITS_SEQ;
	static final int OFFSET_X = OFFSET_Y + BITS_Y;
	static final int OFFSET_PROJ = OFFSET_X + BITS_X;
	
	static int[] CMP_BITS = null;
	static {
		List<Integer> bits = new ArrayList<Integer>();
		for (int i = 0; i < BITS_X; i++) {
			bits.add(OFFSET_Y + i);
			bits.add(OFFSET_X + i);
		}
		for (int i = 0; i < BITS_PROJ; i++) {
			bits.add(OFFSET_PROJ + i);
		}
		CMP_BITS = new int[bits.size()];
		for (int i = 0; i < bits.size(); i++) {
			CMP_BITS[i] = bits.get(i);
		}
	}
	
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
		if (seq >= 1 << (BITS_SEQ - 1)) {
			seq -= (1 << BITS_SEQ);
		}
		
		return new int[] {projId, x, y, seq};
	}

	public static long clone(long ix, int seq) {
		if (seq < (-1 << (BITS_SEQ - 1)) || seq >= (1 << (BITS_SEQ - 1))) {
			throw new IllegalArgumentException();
		}
		int[] k = split(ix);
		return make(k[0], k[1], k[2], seq);
	}
	
	public static int compare(long a, long b) {
		if (a == b) {
			return 0;
		}

		// try to mask any ordering amongst indexes; we want to appear to
		// break ties between points in a randomish manner. thus, compare
		// starting with least-significant bits
		for (int i = 0; i < CMP_BITS.length; i++) {
			int bit = CMP_BITS[i];
			int aBit = (int)((a >> bit) & 0x1);
			int bBit = (int)((b >> bit) & 0x1);
			if (aBit != bBit) {
				return Integer.compare(aBit, bBit);
			}
		}
		
		// if (proj, x, y) are the same, compare straightforwardly based on sequence number
		int[] pa = PointIndex.split(a);
		int[] pb = PointIndex.split(b);
		return Integer.compare(pa[3], pb[3]);
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
	
	public static String print(long ix) {
		if (ix == -1) {
			return "---";
		}
		
		int[] c = split(ix);
		return String.format("%d/%d,%d:%d", c[0], c[1], c[2], c[3]);
	}
	
	static void testA(int proj, int x, int y) {
		long ix = make(proj, x, y);
		int[] reverse = split(ix);
		System.out.println(String.format("%d %d %d => %016x", proj, x, y, make(proj, x, y)));
		if (proj != reverse[0] || x != reverse[1] || y != reverse[2]) {
			throw new RuntimeException();
		}
	}
	
	static void testB(int a, int b, int c, int d, int e, int f, int g, int h) {
		long ix1 = make(a, b, c, d);
		long ix2 = make(e, f, g, h);
		System.out.println(String.format("%016x %016x %d", ix1, ix2, compare(ix1, ix2)));
	}
	
	public static void main(String[] args) {
		testA(0, 0, 0);
		testA(1, 1, 1);
		testA(2, -1, 1);
		testA(3, 1, -1);
		testA(4, -1, -1);
		testA(8191, 8388000, -8388000);
		
		testB(0, 1024, 0, 0, 0, 1024, 0, 0); // 0
		testB(0, 1536, 0, 0, 0, 1025, 0, 0); // -1
		testB(0, 1537, 0, 0, 0, 1025, 0, 0); // 1
		testB(0, 1025, 1024, 0, 0, 1024, 1536, 0); // 1
		testB(0, 1024, 1025, 0, 0, 1536, 1024, 0); // 1
	}

}
