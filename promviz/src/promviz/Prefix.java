package promviz;

import promviz.util.Util;

public class Prefix implements Comparable<Prefix> {
	long prefix;
	int res;

	public Prefix(long ix, int res) {
		this.res = res;
		this.prefix = ix & this.mask();
	}

	public Prefix(Prefix p, int res) {
		this(p.prefix, res);
		if (res < p.res) {
			throw new IllegalArgumentException();
		}
	}
	
	private long mask() {
		int _mask = (~0 << this.res);
		return PointIndex.make(~0, _mask, _mask);
	}
	
	public boolean isParent(long ix) {
		return (ix & this.mask()) == this.prefix;
	}
	
	public Prefix[] children(int level) {
		return children(level, 0);
	}
	
	public Prefix[] children(int level, int fringe) {
		if (level <= 0) {
			level = 1;
		}
		
		int chRes = res - level;
		if (chRes < 0) {
			return null;
		}
		
		int[] p = PointIndex.split(prefix);
		int _dim = Util.pow2(level) + 2 * fringe;
		return tile(p[0], p[1], p[2], _dim, _dim, -fringe, -fringe, chRes);
	}
	
	public static Prefix[] tile(int proj, int x0, int y0, int width, int height, int xo, int yo, int res) {
		Prefix[] pp = new Prefix[width * height];
		int dim = Util.pow2(res);
		int n = 0;
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				pp[n++] = new Prefix(PointIndex.make(
						proj,
						x0 + (i + xo) * dim,
						y0 + (j + yo) * dim
					), res);
			}
		}
		return pp;
	}
	
	public static Prefix[] tileInclusive(int proj, int x0, int y0, int x1, int y1, int res) {
		int width = ((x1 - x0) >> res) + 1;
		int height = ((y1 - y0) >> res) + 1;
		return tile(proj, x0, y0, width, height, 0, 0, res);
	}
	
	public int[] bounds() {
		int[] k = PointIndex.split(prefix);
		int x0 = k[1], y0 = k[2];
		int dim = Util.pow2(res);
		return new int[] {x0, y0, x0 + dim, y0 + dim};
	}
	
	public boolean equals(Object o) {
		if (o instanceof Prefix) {
			Prefix p = (Prefix)o;
			return (this.prefix == p.prefix && this.res == p.res);
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		return Long.valueOf(this.prefix | this.res).hashCode();
	}
	
	public int compareTo(Prefix p) {
		int result = Integer.valueOf(p.res).compareTo(this.res);
		if (result == 0) {
			result = Long.valueOf(this.prefix).compareTo(p.prefix);
		}
		return result;
	}
	
	public String toString() {
		int[] c = PointIndex.split(this.prefix);
		return String.format("%d,%d,%d/%d", c[0], c[1], c[2], this.res);
	}
}