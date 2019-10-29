package com.mrgris.prominence;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;


/* extends Point with the concept of being part of a network or mesh, i.e., being
 * adjacent to other points
 */
public class MeshPoint extends Point {

	public static final int CLASS_SLOPE = 0;
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

	public MeshPoint(long ix, float elev, int isodist) {
		super(ix, elev, isodist);
	}
	
	public MeshPoint(Point p) {
		super(p.ix, p.elev, p.isodist);
	}
	
	public long[] adjIx() {
		return _adjacent;
	}
	
	public List<MeshPoint> adjacent(IMesh m) {
		List<MeshPoint> adj = new ArrayList<MeshPoint>();
		for (long geocode : adjIx()) {
			MeshPoint p;
			try {
				p = m.get(geocode);
			} catch (IndexOutOfBoundsException e) {
				// TODO i'm not sure we should be handling this here, but grid seams will probably work pretty similarly?
				// either combine with handling of pending leads, or determine a priori which pages are non-contiguously
				// adjacent (custom adjacency for grid seams or check grid xlimits for IDL)
				if (PointIndex.split(geocode)[0] == PointIndex.split(this.ix)[0]) {
					// same grid so must be IDL wraparound
					PagedElevGrid pm = (PagedElevGrid)m;
					pm.loadPage(pm.segmentPrefix(geocode));
					p = m.get(geocode);
				} else {
					throw e;
				}
			}
			adj.add(geocode != PointIndex.NULL ? p : null);
		}
		return adj;
	}
		
	public int classify(IMesh m) {
		boolean is_summit = true;
		boolean is_pit = true;

		if (this.adjIx().length == 0) {
			return CLASS_INDETERMINATE;
		}
		
		List<MeshPoint> adjacent = this.adjacent(m);
		for (MeshPoint p : adjacent) {
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
			MeshPoint p0 = adjacent.get(i);
			MeshPoint p1 = adjacent.get((i + 1) % adjacent.size());
			
			if (this.compareElev(p0) != this.compareElev(p1)) {
				transitions++;
			}
		}
		if (transitions > 2) {
			return CLASS_SADDLE;
		}
		
		return CLASS_SLOPE;
	}
	
	static class Lead {
		MeshPoint p0;
		MeshPoint p;
		int i;
		boolean up;
		int len;
		boolean fromCheckpoint; // ie p0 is not a saddle
		List<MeshPoint> trace;
		boolean tracingEnabled = false;
		
		public Lead(boolean up, MeshPoint p0, MeshPoint p, int i) {
			this.up = up;
			this.p0 = p0;
			this.p = p;
			this.i = i;
			this.len = 0;
		}

		public void enableTracing() {
			if (tracingEnabled) {
				return;
			}
			if (len > 0) {
				throw new RuntimeException();
			}
			
			tracingEnabled = true;
			trace = new ArrayList<>();
			trace.add(p);
		}
		
		public void setNextP(MeshPoint p) {
			this.p = p;
			this.len++;
			if (tracingEnabled) {
				trace.add(p);
			}
		}
		
		public Lead follow(IMesh mesh) {
			return this.p.leads(mesh)[this.up ? 0 : 1][0];
		}
		
		public boolean equals(Object o) {
			if (o instanceof Lead) {
				Lead l = (Lead)o;
				return this.p0.equals(l.p0) && this.i == l.i && this.up == l.up;
			} else {
				return false;
			}
		}
		
		public int hashCode() {
			return Objects.hash(p0, i, up);
		}
	}
	
	double getAdjDist(Point p, int gridx, int gridy, double xstretch, double diagstretch) {
		int[] pcs = PointIndex.split(p.ix);
		int dx = pcs[1] - gridx;
		int dy = pcs[2] - gridy;
		// TODO check that point is grid-adjacent
		if (dx == 0) {
			return 1.;
		} else if (dy == 0) {
			return xstretch;
		} else {
			return diagstretch;
		}
	}
	
	Lead[][] leads(IMesh m) {
		List<MeshPoint> adjacent = this.adjacent(m);
		int[] cohort = new int[adjacent.size()];
		int current_cohort = 0;
		for (int i = 0; i < adjacent.size(); i++) {
			MeshPoint p = adjacent.get(i);
			MeshPoint p_prev = (i > 0 ? adjacent.get(i - 1) : null);
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
		
		int[] pcs = PointIndex.split(ix);
		int gridx = pcs[1];
		int gridy = pcs[2];
		// FIXME very inefficient to compute this for every point
		double lat = PointIndex.toLatLon(ix)[0];
		double xstretch = Math.cos(Math.toRadians(lat));
		double diagstretch = Math.pow(xstretch*xstretch + 1., .5);
		
		Lead[][] L = {new Lead[num_cohorts / 2], new Lead[num_cohorts / 2]};
		for (int cur_cohort = 0; cur_cohort < num_cohorts; cur_cohort++) {
			MeshPoint best = null;
			double bestRelDist = 0;
			for (int i = 0; i < adjacent.size(); i++) {
				if (cohort[i] != cur_cohort) {
					continue;
				}

				MeshPoint p = adjacent.get(i);
				double adjDist = getAdjDist(p, gridx, gridy, xstretch, diagstretch);
				boolean newBest = false;
				if (best == null) {
					newBest = true;
				} else {
					// TODO does this handle negative infinity properly
					double rcmp = this.relCompare(p, best);
					if (rcmp > adjDist/bestRelDist) {
						newBest = true;
					}
				}
				if (newBest) {
					best = p;
					bestRelDist = adjDist;
				}
			}
			boolean up = (this.compareElev(best) < 0);
			L[up ? 0 : 1][cur_cohort / 2] = new Lead(up, this, best, cur_cohort);
		}
		return L;
	}
	
}
