package promviz;

public class PointIndex {

	public static long make(int projId, int x, int y) {
		return ((long)projId << 48) | ((long)(x & 0x00ffffff) << 24) | (long)(y & 0x00ffffff);
	}
	
	public static int[] split(long ix) {
		int projId = (int)(ix >>> 48);
		int x = (int)(ix >> 24 & 0xffffff);
		int y = (int)(ix & 0xffffff);
		if (x >= 1 << 23) {
			x |= 0xff000000;
		}
		if (y >= 1 << 23) {
			y |= 0xff000000;
		}
		return new int[] {projId, x, y};
	}
	
	public static long truncate(long ix, int depth) {
		int[] c = split(ix);
		int x = c[1] | (~0 << depth);
		int y = c[2] | (~0 << depth);
		return make(c[0], x, y);
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
