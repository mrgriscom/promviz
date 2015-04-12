package old.promviz;

class Prefix implements Comparable<Prefix> {
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
		if (level <= 0) {
			level = 1;
		}
		
		int chRes = res - level;
		if (chRes < 0) {
			return null;
		}
		
		int[] p = PointIndex.split(prefix);
		Prefix[] ch = new Prefix[1 << (2*level)];
		for (int i = 0; i < (1<<level); i++) {
			for (int j = 0; j < (1<<level); j++) {
				ch[(i << level) + j] = new Prefix(PointIndex.make(
							p[0],
							p[1] + (i << chRes),
							p[2] + (j << chRes)
						), chRes);
			}
		}
		return ch;
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