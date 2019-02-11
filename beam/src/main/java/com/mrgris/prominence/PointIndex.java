package com.mrgris.prominence;

import com.mrgris.prominence.dem.DEMIndex;
import com.mrgris.prominence.util.GeoCode;
import com.mrgris.prominence.util.Util;

public class PointIndex {
	
	static final int BITS_SEQ = 4; // signed
	public static final int BITS_X = 24;  // signed
	static final int BITS_Y = 24;  // signed
	static final int BITS_PROJ = 64 - BITS_X - BITS_Y - BITS_SEQ;

	static final int OFFSET_SEQ = 0;
	static final int OFFSET_Y = OFFSET_SEQ + BITS_SEQ;
	static final int OFFSET_X = OFFSET_Y + BITS_Y;
	static final int OFFSET_PROJ = OFFSET_X + BITS_X;
	
	public static final long NULL = -1;
	
	public static long make(int projId, int x, int y) {
		return make(projId, x, y, 0);
	}
	
	public static long make(int projId, int x, int y, int seq) {
		assert Util.inSignedRange(seq, BITS_SEQ);
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

		return new int[] {projId, 
			Util.toSignedRange(x, BITS_X),
			Util.toSignedRange(y, BITS_Y),		
			Util.toSignedRange(seq, BITS_SEQ)
		};
	}

	public static long clone(long ix, int seq) {
		int[] k = split(ix);
		return make(k[0], k[1], k[2], seq);
	}
	
	public static long pseudorandId(long ix) {
		return Long.reverse(iGeocode(ix));
	}
	
	public static int compare(long a, long b) {
		if (a == b) {
			return 0;
		}

		// if (proj, x, y) are the same, compare straightforwardly based on sequence number
		int[] pa = PointIndex.split(a);
		int[] pb = PointIndex.split(b);
		if (pa[0] == pb[0] && pa[1] == pb[1] && pa[2] == pb[2]) {
			return Integer.compare(pa[3], pb[3]);			
		}

		// compare pseudo-randomly based on geocode, least significant bits first
		return Long.compare(pseudorandId(a), pseudorandId(b));
	}
	
	public static double[] toLatLon(long ix) {
		if (ix == PointIndex.NULL) {
			return new double[] {Double.NaN, Double.NaN};
		}
		int[] _ix = split(ix);
		return DEMIndex.instance().grids[_ix[0]].toLatLon(_ix[1], _ix[2]);
	}
	
	public static long iGeocode(long ix) {		
		double[] ll = PointIndex.toLatLon(ix);
		return GeoCode.fromCoord(ll[0], ll[1]);
	}
	
	public static String geocode(long ix) {
		return Util.print(iGeocode(ix));
	}
	
	public static long truncate(long ix, int depth) {
		int[] c = split(ix);
		int x = c[1] | (~0 << depth);
		int y = c[2] | (~0 << depth);
		return make(c[0], x, y);
	}
	
	public static String toString(long ix) {
		if (ix == -1) {
			return "~";
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
