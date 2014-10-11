package promviz;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Map.Entry;

import promviz.util.Logging;

public class PromNetwork {

	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<Point> c;
		List<Point> path;
		
		public PromInfo(Point p, Comparator<Point> c) {
			this.p = p;
			this.c = c;
		}
		
		public double prominence() {
			return Math.abs(p.elev - saddle.elev);
		}
		
		public void add(Point cur) {
			if (saddle == null || c.compare(cur, saddle) < 0) {
				saddle = cur;
			}
		}
		
		public void finalize(Map<Point, Point> backtrace, Point horizon) {
			this.path = new ArrayList<Point>();
			Point cur = horizon; //saddle;
			while (cur != null) {
				this.path.add(cur);
				cur = backtrace.get(cur);
			}
		}
	}
	
	static class Front {
		PriorityQueue<Point> queue;
		Set<Point> set;
		Set<Point> seen;
		TopologyNetwork tree;
		int pruneThreshold = 1;
		
		public Front(final Comparator<Point> c, TopologyNetwork tree) {
			this.tree = tree;
			queue = new PriorityQueue<Point>(10, new Comparator<Point>() {
				@Override
				public int compare(Point p1, Point p2) {
					return -c.compare(p1, p2);
				}				
			});
			set = new HashSet<Point>();
			seen = new HashSet<Point>();
		}
		
		public boolean add(Point p) {
			if (seen.contains(p)) {
				return false;
			}
			
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
			}
			return newItem;
		}
		
		public Point next() {
			Point p = queue.poll();
			set.remove(p);
			seen.add(p);
			return p;
		}

		public void prune() {
			if (seen.size() > pruneThreshold) {
				seen.retainAll(this.adjacent());
				pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
				//System.err.println("pruned " + pruneThreshold);
			}
		}
		
		public Set<Point> adjacent() {
			Set<Point> frontAdj = new HashSet<Point>();
			for (Point f : queue) {
				for (Point adj : tree.adjacent(f)) {
					frontAdj.add(adj);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return set.size();
		}
	}

	static interface Criterion {
		boolean condition(Comparator<Point> cmp, Point p, Point cur);
	}
	
	public static PromInfo prominence(TopologyNetwork tree, Point p, final boolean up) {
		return _prominence(tree, p, up, new Criterion() {
			@Override
			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
				return cmp.compare(cur, p) > 0;
			}			
		});
	}

	public static PromInfo parent(TopologyNetwork tree, Point p,
				final boolean up, final Map<Point, PromNetwork.PromInfo> prominentPoints) {
		final double pProm = prominentPoints.get(p).prominence();
		return _prominence(tree, p, up, new Criterion() {
			@Override
			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
				double curProm = (prominentPoints.containsKey(cur) ? prominentPoints.get(cur).prominence() : 0);				
				return cmp.compare(cur, p) > 0 && curProm > pProm; // not sure the elev comparison is even necessary
			}
		});
	}
	
	public static List<Point> domainSaddles(TopologyNetwork tree, Point p, Map<Point, PromNetwork.PromInfo> saddleIndex, float threshold) {
		List<Point> saddles = new ArrayList<Point>();
		saddleSearch(saddles, saddleIndex, tree, threshold, p, null);
		return saddles;
	}
	
	static void saddleSearch(List<Point> saddles, Map<Point, PromNetwork.PromInfo> saddleIndex, TopologyNetwork tree, float threshold, Point p, Point parent) {
		for (Point adj : tree.adjacent(p)) {
			PromNetwork.PromInfo saddleInfo = saddleIndex.get(adj);
			if (saddleInfo != null && saddleInfo.prominence() >= threshold) {
				saddles.add(adj);
			} else if (adj != parent) {
				saddleSearch(saddles, saddleIndex, tree, threshold, adj, p);
			}
		}
		
	}
	
	public static List<List<Point>> runoff(TopologyNetwork antiTree, Point saddle, final boolean up) {
		List<List<Point>> runoffs = new ArrayList<List<Point>>();
		for (Point lead : antiTree.adjacent(saddle)) {
			// necessary?
			if (lead == null) {
				continue;
			}
			
			PromNetwork.PromInfo saddle_pi = PromNetwork._prominence(antiTree, lead, !up, new Criterion() {
				@Override
				public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
					return false;
				}				
			});
			saddle_pi.path.add(saddle);
			runoffs.add(saddle_pi.path);
		}
		return runoffs;
	}

	static Comparator<Point> _cmp(final boolean up) {
		return new Comparator<Point>() {
			@Override
			public int compare(Point p0, Point p1) {
				return up ? ElevComparator.cmp(p0, p1) : ElevComparator.cmp(p1, p0);
			}
		};
	}

	public static PromInfo _prominence(TopologyNetwork tree, Point p, boolean up, Criterion crit) {
		if (up != tree.up) {
			throw new IllegalArgumentException("incompatible topology tree");
		}
		
		Comparator<Point> c = _cmp(up);
		
		PromInfo pi = new PromInfo(p, c);
		Front front = new Front(c, tree);
		front.add(p);
		Map<Point, Point> backtrace = new HashMap<Point, Point>();

		// point is not part of network (ie too close to edge to have connecting saddle)
		if (tree.adjacent(p) == null) {
			return null;
		}

		Point cur = null;
		outer:
		while (true) {
			cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				pi.global_max = true;
				break;
			}
			pi.add(cur);
			if (crit.condition(c, p, cur)) {
				break;
			}

			if (tree.pending.containsKey(cur)) {
				// reached an edge
				pi.min_bound_only = true;
				break outer;				
			}
			for (Point adj : tree.adjacent(cur)) {
				if (adj == cur) { // FIXME
					//System.err.println("neighbor with self [" + cur.ix + "]... wtf???");
					continue;
				}
				
				if (front.add(adj)) {
					backtrace.put(adj, cur);
				}
			}
			
			front.prune();
		}
		
		pi.finalize(backtrace, cur);
		return pi;
	}

	static class PromInfo2 {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<Point> c;
		List<Point> path;
		
		public PromInfo2(Point p, Comparator<Point> c) {
			this.p = p;
			this.c = c;
		}
		
		public double prominence() {
			return Math.abs(p.elev - saddle.elev);
		}
		
		public void add(Point cur) {
			if (saddle == null || c.compare(cur, saddle) < 0) {
				saddle = cur;
			}
		}
		
		public void finalize(Map<Point, Point> backtrace, Point horizon) {
			this.path = new ArrayList<Point>();
			Point cur = horizon; //saddle;
			while (cur != null) {
				this.path.add(cur);
				cur = backtrace.get(cur);
			}
		}
		
		public PromInfo toNormal() {
			PromInfo pi = new PromInfo(this.p, this.c);
			pi.saddle = this.saddle;
			pi.global_max = this.global_max;
			pi.min_bound_only = this.min_bound_only;
			pi.path = new ArrayList<Point>();
			pi.path.add(this.p);
			pi.path.add(this.saddle);
			return pi;
		}
	}
	
	public static void bigOlPromSearch(Point p, TopologyNetwork tree, Map<Point, PromInfo> results) {
		boolean up = true;
		Comparator<Point> c = _cmp(up);
		Criterion crit = new Criterion() {
			@Override
			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
				return cmp.compare(cur, p) > 0;
			}			
		};
		
		PromInfo2 pi = new PromInfo2(p, c);
		Front front = new Front(c, tree);
		front.add(p);
		Map<Point, Point> backtrace = new HashMap<Point, Point>();
	
		// point is not part of network (ie too close to edge to have connecting saddle)
		if (tree.adjacent(p) == null) {
			return;
		}
	
		Point cur = null;
		outer:
		while (true) {
			cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				pi.global_max = true;
				break;
			}
			pi.add(cur);
			if (crit.condition(c, p, cur)) {
				break;
			}
	
			if (tree.pending.containsKey(cur)) {
				// reached an edge
				pi.min_bound_only = true;
				break outer;				
			}
			for (Point adj : tree.adjacent(cur)) {
				if (adj == cur) { // FIXME
					//System.err.println("neighbor with self [" + cur.ix + "]... wtf???");
					continue;
				}
				
				if (front.add(adj)) {
					backtrace.put(adj, cur);
				}
			}
			
			front.prune();
		}
		
		pi.finalize(backtrace, cur);
		results.put(p,  pi.toNormal());
		
		
	}
}
