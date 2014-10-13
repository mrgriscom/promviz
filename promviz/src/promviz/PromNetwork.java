package promviz;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.collect.Lists;

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
		
		public void finalize(Front f, Point horizon) {
			this.path = Lists.newArrayList(f.trace(horizon));
			
//			try {
//				PrintWriter w = new PrintWriter("/tmp/backtrace");
//				for (Map.Entry<Point, Point> e : f.backtrace.entrySet()) {
//					double[] ll0 = PointIndex.toLatLon(e.getKey().ix);
//					double[] ll1 = PointIndex.toLatLon(e.getValue().ix);
//					
//					w.println(ll0[0] + " " + ll0[1]);
//					w.println(ll1[0] + " " + ll1[1]);
//					w.println();
//				}
//				w.close();
//			} catch (IOException ioe) {
//				throw new RuntimeException();
//			}
		}
	}
	
	static class Front {
		PriorityQueue<Point> queue;
		Set<Point> set;
		Set<Point> seen;
		TopologyNetwork tree;
		Map<Point, Point> backtrace;
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
			backtrace = new HashMap<Point, Point>();
		}
		
		public boolean add(Point p, Point parent) {
			if (seen.contains(p)) {
				return false;
			}
			
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
				if (parent != null) {
					backtrace.put(p, parent);
				}
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
				
				Set<Point> backtraceKeep = new HashSet<Point>();
				for (Point p : set) {
					for (Point t : trace(p)) {
						boolean newItem = backtraceKeep.add(t);
						if (!newItem) {
							break;
						}
					}
				}
				Iterator<Point> iter = backtrace.keySet().iterator();
				while (iter.hasNext()) {
					Point p = iter.next();
					if (!backtraceKeep.contains(p)) {
				        iter.remove();
				    }
				}
				
				//System.err.println("pruned " + pruneThreshold);
			}
		}
		
		class TraceIterator implements Iterator<Point> {
			Point cur;
			
			public TraceIterator(Point start) {
				this.cur = start;
			}
			
			public boolean hasNext() {
				return this.cur != null;
			}

			public Point next() {
				Point toRet = this.cur;
				this.cur = backtrace.get(this.cur);
				return toRet;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
		
		public Iterable<Point> trace(final Point start) {
			return new Iterable<Point>() {
				public Iterator<Point> iterator() {
					return new TraceIterator(start);
				}				
			};
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
		front.add(p, null);

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
				
				front.add(adj, cur);
			}
			
			front.prune();
			
//			if (Math.random() < .0001) {
//				System.err.println(front.size() + " " + front.seen.size() + " " + front.backtrace.size());
//			}
		}
		
		pi.finalize(front, cur);
		return pi;
	}

	static class PromInfo2 {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<Point> c;
		List<Point> path;
		
		public PromInfo2(Point peak, Point saddle) {
			this.p = peak;
			this.saddle = saddle;
		}
		
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
		
		public void finalizeForward(Map<Point, Point> backtrace, Point horizon) {
			this.path = new ArrayList<Point>();
			Point cur = horizon;
			while (true) {
				this.path.add(cur);
				if (cur.equals(this.saddle)) {
					break;
				}
				cur = backtrace.get(cur);
			}
		}

		public void finalizeBackward(Map<Point, Point> backtrace, Point horizon) {
			this.path = new ArrayList<Point>();
			Point cur = this.p;
			while (true) {
				this.path.add(cur);
				if (cur.equals(this.saddle)) {
					break;
				}
				cur = backtrace.get(cur);
			}
			Collections.reverse(this.path);
		}
		
		public PromInfo toNormal() {
			PromInfo pi = new PromInfo(this.p, this.c);
			pi.saddle = this.saddle;
			pi.global_max = this.global_max;
			pi.min_bound_only = this.min_bound_only;
			if (this.path != null) {
				pi.path = this.path;
			} else {
				pi.path = new ArrayList<Point>();
				pi.path.add(this.p);
				pi.path.add(this.saddle);
			}
			return pi;
		}
	}
	
	public static void bigOlPromSearch(Point root, TopologyNetwork tree, Map<Point, PromInfo> results, double cutoff) {
		boolean up = true;
		Comparator<Point> c = _cmp(up);
		Criterion crit = new Criterion() {
			@Override
			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
				return cmp.compare(cur, p) > 0;
			}			
		};
		
		Deque<Point> peaks = new ArrayDeque<Point>(); 
		Deque<Point> saddles = new ArrayDeque<Point>(); 
		Front front = new Front(c, tree);
		front.add(root, null);
	
		// point is not part of network (ie too close to edge to have connecting saddle)
		if (tree.adjacent(root) == null) {
			return;
		}
	
		Point cur = null;
		while (true) {
			cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				//pi.global_max = true;
				//break;
				return;
			}
			
			boolean deadEnd = true;
			for (Point adj : tree.adjacent(cur)) {
				if (adj == cur) { // FIXME
					//System.err.println("neighbor with self [" + cur.ix + "]... wtf???");
					continue;
				}
				// weird temporary workaround for saddles pointing to saddles
				if ((cur.classify(tree) == Point.CLASS_SUMMIT) == (adj.classify(tree) == Point.CLASS_SUMMIT)) {
					// note this also handles the self-referential case above
//					System.err.println("weird topology");
					continue;
				}
				
				if (front.add(adj, cur)) {
					deadEnd = false;
				}
			}
			front.prune();
						
			if (cur.classify(tree) != Point.CLASS_SUMMIT) {
				if (deadEnd) {
					// basin saddle
					continue;
				}
				while (saddles.peekFirst() != null && c.compare(cur, saddles.peekFirst()) < 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					// path backtracks
					PromInfo2 pi = new PromInfo2(peak, saddle);
					if (pi.prominence() >= cutoff) {
						pi.finalizeBackward(front.backtrace, null);
						results.put(peak, pi.toNormal());
					}
				}
				saddles.addFirst(cur);
			} else {
				if (peaks.size() == saddles.size() + 1) {
//					System.err.println("ignoring forked saddle");
					continue;
				}
				
				while (peaks.peekFirst() != null && c.compare(cur, peaks.peekFirst()) > 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					// path goes forward
					PromInfo2 pi = new PromInfo2(peak, saddle);
					if (pi.prominence() >= cutoff) {
						pi.finalizeForward(front.backtrace, cur);
						results.put(peak, pi.toNormal());
					}
				}
				peaks.addFirst(cur);
			}

			if (tree.pending.containsKey(cur)) {
				while (!saddles.isEmpty()) {
					Point peak = peaks.removeLast();
					Point saddle = saddles.removeLast();
					PromInfo pi = new PromInfo2(peak, saddle).toNormal();
					pi.min_bound_only = true;
					pi.finalize(front, cur);
					if (pi.prominence() >= cutoff) {
						results.put(peak, pi);
					}
				}
				// reached an edge
				//pi.min_bound_only = true;
				//break outer;				
				return;
			}
			
		}		
		
	}
}
