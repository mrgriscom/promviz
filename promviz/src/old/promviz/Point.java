package old.promviz;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/* extends BasePoint with the concept of being part of a network or mesh, i.e., being
 * adjacent to other points
 */
public class Point extends BasePoint {

	public static final int CLASS_OTHER = 0;
	public static final int CLASS_SUMMIT = 1;
	public static final int CLASS_PIT = 2;
	public static final int CLASS_SADDLE = 3;
	public static final int CLASS_INDETERMINATE = 4;
	
	/* list of point geocodes adjacent to this point, listed in a clockwise direction
	 * if any two consecutive points are NOT adjacent to each other (i.e., the
	 * three points do not form a Tri), this list will have one or more nulls
	 * inserted between them
	 */
	long[] _adjacent = new long[0];
	int[] _tagging = null;

	public Point(long ix, float elev) {
		super(ix, elev);
	}
	
	public Point(long ix, float elev, int isodist) {
		super(ix, elev, isodist);
	}
	
	public long[] adjIx() {
		return _adjacent;
	}
	
	public List<Point> adjacent(IMesh m) {
		List<Point> adj = new ArrayList<Point>();
		for (long geocode : adjIx()) {
			adj.add(geocode != -1 ? m.get(geocode) : null);
		}
		return adj;
	}
	
	public int classify(IMesh m) {
		boolean is_summit = true;
		boolean is_pit = true;

		if (this.adjIx().length == 0) {
			return CLASS_OTHER; // or indet?
		}
		
		List<Point> adjacent = this.adjacent(m);
		for (Point p : adjacent) {
			if (p == null)  {
				return CLASS_INDETERMINATE;
			}
			
			if (this.compareElev(p) == -1) {
				is_summit = false;
			} else if (this.compareElev(p) == 1) {
				is_pit = false;
			}
		}
		
		if (is_summit) {
			return CLASS_SUMMIT;
		} else if (is_pit) {
			return CLASS_PIT;
		}
		
		int transitions = 0;
		for (int i = 0; i < adjacent.size(); i++) {
			Point p0 = adjacent.get(i);
			Point p1 = adjacent.get((i + 1) % adjacent.size());
			
			if (this.compareElev(p0) != this.compareElev(p1)) {
				transitions++;
			}
		}
		if (transitions > 2) {
			return CLASS_SADDLE;
		}
		
		return CLASS_OTHER;
	}
	
	static class Lead {
		Point p0;
		Point p;
		int i;
		
		public Lead(Point p0, Point p, int i) {
			this.p0 = p0;
			this.p = p;
			this.i = i;
		}
		
		public boolean equals(Object o) {
			if (o instanceof Lead) {
				Lead l = (Lead)o;
				return this.p0.equals(l.p0) && this.i == l.i;
			} else {
				return false;
			}
		}
		
		public int hashCode() {
			return this.p0.hashCode() | Integer.valueOf(this.i).hashCode();
		}
	}
	
	List<Lead> leads(IMesh m, boolean up) {
		List<Point> adjacent = this.adjacent(m);
		int[] cohort = new int[adjacent.size()];
		int current_cohort = 0;
		for (int i = 0; i < adjacent.size(); i++) {
			Point p = adjacent.get(i);
			Point p_prev = (i > 0 ? adjacent.get(i - 1) : null);
			if (p_prev != null && this.compareElev(p) != this.compareElev(p_prev)) {
				current_cohort++;
			}
			cohort[i] = current_cohort;
		}
		int num_cohorts = ((current_cohort + 1) / 2) * 2; // if cur_cohort is odd it means the 'seam' (array wraparound) fell in the middle of one
		boolean startsUp = (this.compareElev(adjacent.get(0)) < 0);
		for (int i = 0; i < adjacent.size(); i++) {
			cohort[i] = (cohort[i] + (startsUp ? 0 : 1)) % num_cohorts; // 'up' cohorts must be even-numbered
		}
		
		List<Lead> L = new ArrayList<Lead>();
		for (int cur_cohort = 0; cur_cohort < num_cohorts; cur_cohort++) {
			Point best = null;
			for (int i = 0; i < adjacent.size(); i++) {
				if (cohort[i] != cur_cohort) {
					continue;
				}
				
				Point p = adjacent.get(i);
				if (best == null || best.compareElev(p) == this.compareElev(best)) {
					best = p;
				}
			}
			if (this.compareElev(best) == (up ? -1 : 1)) {
				L.add(new Lead(this, best, cur_cohort));
			}
		}
		return L;
	}
	
	boolean adjAdd(long ixTo) {
		return adjAdd(ixTo, -1, false);
	}
	boolean adjAdd(long ixTo, int tag, boolean rev) {
		// FUCKING JAVA!!
		// all this does is add the new point's geocode to the adjacency array if it isn't already in there
		boolean exists = false;
		for (long l : this._adjacent) {
			if (l == ixTo) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			long[] new_ = new long[this._adjacent.length + 1];
			System.arraycopy(this._adjacent, 0, new_, 0, this._adjacent.length);
			new_[this._adjacent.length] = ixTo;
			this._adjacent = new_;

			if (_tagging != null) {
				int[] tnew_ = new int[this._tagging.length + 1];
				System.arraycopy(this._tagging, 0, tnew_, 0, this._tagging.length);
				this._tagging = tnew_;
			}
		}
		this.setTag(ixTo, tag, rev);
		return !exists;
	}
	
	static int encTag(int tag, boolean rev) {
		return (tag < 0 ? 0 : (rev ? -1 : 1) * (1 + tag));
	}
	
	void setTag(long dst, int tag, boolean rev) {
		if (tag == -1) {
			return;
		}
		if (_tagging == null) {
			_tagging = new int[_adjacent.length];
		}
		for (int i = 0; i < _adjacent.length; i++) {
			if (_adjacent[i] == dst) {
				_tagging[i] = encTag(tag, rev);
				return;
			}
		}
		throw new NoSuchElementException();
	}
	
	// returns in 'encoded' format
	int getTag(long dst) {
		if (_tagging != null) {
			for (int i = 0; i < _adjacent.length; i++) {
				if (_adjacent[i] == dst) {
					return _tagging[i];
				}
			}
			throw new NoSuchElementException();
		}
		return 0;
	}
	
	long getByTag(int tag, boolean rev) {
		tag = encTag(tag, rev);
		if (_tagging != null) {
			for (int i = 0; i < _tagging.length; i++) {
				if (_tagging[i] == tag) {
					return _adjacent[i];
				}
			}
		}
		return -1;
	}
}
