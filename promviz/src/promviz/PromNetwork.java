package promviz;
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

import promviz.util.Logging;
import promviz.util.ReverseComparator;

import com.google.common.collect.Lists;

public class PromNetwork {

	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<BasePoint> c;
		List<Long> path;
		
		public PromInfo(Point p, Comparator<BasePoint> c) {
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
			if (horizon != null) {
				this.path = Lists.newArrayList(f.trace(horizon));
			} else {
				this.path = new ArrayList<Long>();
				this.path.add(this.p.ix);
				this.path.add(this.saddle.ix);
			}
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
		PriorityQueue<BasePoint> queue;
		Set<Point> set;
		Set<Long> seen;
		Map<Long, Long> backtrace;
		int pruneThreshold = 1;

		Map<Point, Point> forwardSaddles;
		Map<Point, Point> backwardSaddles;
		
		TopologyNetwork tree;
		Comparator<BasePoint> c;

		public Front(final Comparator<BasePoint> c, TopologyNetwork tree) {
			this.tree = tree;
			this.c = c;
			queue = new PriorityQueue<BasePoint>(10, new ReverseComparator<BasePoint>(c));
			set = new HashSet<Point>();
			seen = new HashSet<Long>();
			backtrace = new HashMap<Long, Long>();
			
			forwardSaddles = new HashMap<Point, Point>();
			backwardSaddles = new HashMap<Point, Point>();
		}
		
		public boolean add(Point p, Point parent) {
			if (seen.contains(p.ix)) {
				return false;
			}
			
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
				if (parent != null) {
					backtrace.put(p.ix, parent.ix);
				}
			}
			return newItem;
		}
		
		public Point next() {
			Point p = (Point)queue.poll();
			if (p != null) {
				set.remove(p);
				seen.add(p.ix);
			}
			return p;
		}

		void treeDFS(Map<Long, Set<Long>> tree, Long cur, List<Long> flat) {
			flat.add(cur);
			Set<Long> children = tree.get(cur);
			if (children != null) {
				for (Long child : children) {
					treeDFS(tree, child, flat);
				}
			}
		}
		
		public void prune() {
			// TODO optimize this to work generationally
			
			if (seen.size() <= pruneThreshold) {
				return;
			}

			seen.retainAll(this.adjacent());
			pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
			
//			Set<Long> backtraceKeep = new HashSet<Long>();
//			for (Point p : set) {
//				for (Long t : trace(p)) {
//					boolean newItem = backtraceKeep.add(t);
//					if (!newItem) {
//						break;
//					}
//				}
//			}
//			// this will include all pending saddles
//
//			Map<Long, Set<Long>> tree = new HashMap<Long, Set<Long>>();
//			Long root = -1L;
//			for (long child : backtraceKeep) {
//				long parent;
//				if (backtrace.containsKey(child)) {
//					parent = backtrace.get(child);
//				} else {
//					root = child;
//					continue;
//				}
//				if (!tree.containsKey(parent)) {
//					tree.put(parent, new TreeSet<Long>());
//				}
//				tree.get(parent).add(child);
//			}			
//			List<Long> backtraceFlat = new ArrayList<Long>();
//			treeDFS(tree, root, backtraceFlat);
//			// should this include only saddles?
//			
//			/*
//			 * iterate over backtraceflat in reverse order
//			 * 
//			 * start at saddle, set as lower bound. upper bound is global max
//			 * go to peak, adjust lower bound, if has routing table entry, merge with current
//			 * go to next saddle and store. if already, merge
//			 * if saddle lower than any pending, search those branches and add to bt//
//
//			 * 
//			 * 
//			 * 
//			 * 
//			 */
//			
//			// determine next-highest points still reachable
//			// add them to backtracekeep
//			
//			// this will include all pending peaks
//			
//			Iterator<Long> iter = backtrace.keySet().iterator();
//			while (iter.hasNext()) {
//				long p = iter.next();
//				if (!backtraceKeep.contains(p)) {
//			        //iter.remove();
//			    }
//			}
			
			// prune routing to match backtrace
			// trim threshold entries
			
			//Logging.log("pruned " + set.size() + " " + backtrace.size());
		}
		
		class TraceIterator implements Iterator<Long> {
			long cur;
			
			public TraceIterator(long start) {
				this.cur = start;
			}
			
			public boolean hasNext() {
				return this.cur != -1;
			}

			public Long next() {
				long toRet = this.cur;
				Long next = backtrace.get(this.cur);
				this.cur = (next != null ? next : -1);
				return toRet;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
		
		public Iterable<Long> trace(final Point start) {
			return new Iterable<Long>() {
				public Iterator<Long> iterator() {
					return new TraceIterator(start.ix);
				}				
			};
		}
		
		public Set<Long> adjacent() {
			Set<Long> frontAdj = new HashSet<Long>();
			for (BasePoint f : queue) {
				for (Point adj : tree.adjacent((Point)f)) {
					frontAdj.add(adj.ix);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return set.size();
		}
		
		
		Point _next(Point cur) {
			return this.tree.get(this.backtrace.get(cur.ix));
		}
		
		public Point searchThreshold(Point p, Point saddle) {

			/*
			 * we have mapping saddle->peak: forwardSaddles, backwardSaddles
			 * forwardSaddles is saddles fixed via finalizeForward, etc.
			 * backwardSaddles includes all pending saddle/peak pairs
			 * 
			 * strict definition:
			 * forwardSaddles means: given the saddle, the peak is located in the direction of the backtrace
			 * backwardSaddles means: peak is located in opposite direction to the backtrace
             */

			Point start = _next(saddle);
			Point target = null;
			for (int i = 0; i < 100; i++) { //while (true) {
				Iterable<Long> path = (target == null ? this.trace(start) : getAtoB(start, target));
				
				boolean isPeak = true;
				long prevIx = -1;
				Point lockout = null;
				for (long ix : path) {
					Point cur = this.tree.get(ix);

					if (lockout != null && this.c.compare(cur, lockout) < 0) {
						lockout = null;
					}
					
					if (lockout == null) {
						if (isPeak) {
//							if (this.c.compare(cur, start) > 0) { // is this legit?
//								start = cur;
//							}
							if (this.c.compare(cur, p) > 0) {
								return cur;
							}
						} else {
							Point pf = forwardSaddles.get(cur);
							Point pb = backwardSaddles.get(cur);
							boolean dirForward = (prevIx == this.backtrace.get(ix));
							Point peakAway = (dirForward ? pf : pb);
							Point peakToward = (dirForward ? pb : pf);
							if (peakToward != null) { // really only matters if peakToward is higher than p, but it works just the same regardless (in theory)
								lockout = cur;
							} else if (peakAway != null && this.c.compare(peakAway, p) > 0) {
								target = peakAway;
								break;
							}
						}
					}
						
					isPeak = !isPeak;
					prevIx = ix;
				}
			}
			
			double[] coords = PointIndex.toLatLon(p.ix);
			System.err.println("multiply-connected saddle??? " + GeoCode.print(GeoCode.fromCoord(coords[0], coords[1])));
			return saddle;
		}

		public List<Long> getAtoB(Point pA, Point pB) {
			Map<Long, Long> tree = this.backtrace;
			long a = pA.ix;
			long b = pB.ix;
			
			List<Long> fromA = new ArrayList<Long>();
			List<Long> fromB = new ArrayList<Long>();
			Set<Long> inFromA = new HashSet<Long>();
			Set<Long> inFromB = new HashSet<Long>();

			long intersection;
			long curA = a;
			long curB = b;
			while (true) {
				if (curA == curB) {
					intersection = curA;
					break;
				}
				
				if (curA != -1) {
					fromA.add(curA);
					inFromA.add(curA);
					try {
						curA = tree.get(curA);
					} catch (NullPointerException npe) {
						curA = -1;
					}
				}
				if (curB != -1) {
					fromB.add(curB);
					inFromB.add(curB);
					try {
						curB = tree.get(curB);
					} catch (NullPointerException npe) {
						curB = -1;
					}
				}
					
				if (inFromA.contains(curB)) {
					intersection = curB;
					break;
				} else if (inFromB.contains(curA)) {
					intersection = curA;
					break;
				}
			}

			List<Long> path = new ArrayList<Long>();
			int i = fromA.indexOf(intersection);
			path.addAll(i != -1 ? fromA.subList(0, i) : fromA);
			path.add(intersection);
			List<Long> path2 = new ArrayList<Long>();
			i = fromB.indexOf(intersection);
			path2 = (i != -1 ? fromB.subList(0, i) : fromB);
			Collections.reverse(path2);
			path.addAll(path2);
			return path;
		}
		
	}
	

	static interface Criterion {
		boolean condition(Comparator<BasePoint> cmp, BasePoint p, BasePoint cur);
	}
	
	public static PromInfo prominence(TopologyNetwork tree, Point p, final boolean up) {
		return _prominence(tree, p, up, new Criterion() {
			@Override
			public boolean condition(Comparator<BasePoint> cmp, BasePoint p, BasePoint cur) {
				return cmp.compare(cur, p) > 0;
			}			
		});
	}

//	public static PromInfo parent(TopologyNetwork tree, Point p,
//				final boolean up, final Map<Point, PromNetwork.PromInfo> prominentPoints) {
//		final double pProm = prominentPoints.get(p).prominence();
//		return _prominence(tree, p, up, new Criterion() {
//			@Override
//			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//				double curProm = (prominentPoints.containsKey(cur) ? prominentPoints.get(cur).prominence() : 0);				
//				return cmp.compare(cur, p) > 0 && curProm > pProm; // not sure the elev comparison is even necessary
//			}
//		});
//	}
	
//	public static List<Point> domainSaddles(TopologyNetwork tree, Point p, Map<Point, PromNetwork.PromInfo> saddleIndex, float threshold) {
//		List<Point> saddles = new ArrayList<Point>();
//		saddleSearch(saddles, saddleIndex, tree, threshold, p, null);
//		return saddles;
//	}
//	
//	static void saddleSearch(List<Point> saddles, Map<Point, PromNetwork.PromInfo> saddleIndex, TopologyNetwork tree, float threshold, Point p, Point parent) {
//		for (Point adj : tree.adjacent(p)) {
//			PromNetwork.PromInfo saddleInfo = saddleIndex.get(adj);
//			if (saddleInfo != null && saddleInfo.prominence() >= threshold) {
//				saddles.add(adj);
//			} else if (adj != parent) {
//				saddleSearch(saddles, saddleIndex, tree, threshold, adj, p);
//			}
//		}
//		
//	}
//	
//	public static List<List<Point>> runoff(TopologyNetwork antiTree, Point saddle, final boolean up) {
//		List<List<Point>> runoffs = new ArrayList<List<Point>>();
//		for (Point lead : antiTree.adjacent(saddle)) {
//			// necessary?
//			if (lead == null) {
//				continue;
//			}
//			
//			PromNetwork.PromInfo saddle_pi = PromNetwork._prominence(antiTree, lead, !up, new Criterion() {
//				@Override
//				public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//					return false;
//				}
//			});
////			saddle_pi.path.add(saddle);
////			runoffs.add(saddle_pi.path);
//		}
//		return runoffs;
//	}
		
	public static PromInfo _prominence(TopologyNetwork tree, Point p, boolean up, Criterion crit) {
		if (up != tree.up) {
			throw new IllegalArgumentException("incompatible topology tree");
		}
		
		Comparator<BasePoint> c = BasePoint.cmpElev(up);
		
		PromInfo pi = new PromInfo(p, c);
		Front front = new Front(c, tree);
		front.add(p, null);

		// point is not part of network (ie too close to edge to have connecting saddle)
//		if (tree.adjacent(p) == null) {
//			return null;
//		}

		Point cur = null;
		while (true) {
			cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				pi.global_max = true;
				Logging.log("global max? " + p);
				break;
			}
			pi.add(cur);
			if (crit.condition(c, p, cur)) {
				break;
			}

			if (tree.pending.containsKey(cur)) {
				// reached an edge
				pi.min_bound_only = true;
			}
			for (Point adj : tree.adjacent(cur)) {
				front.add(adj, cur);
			}
			
			front.prune();
		}
		
		pi.finalize(front, cur);
		return pi;
	}

	static class PromInfo2 {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<BasePoint> c;
		List<Long> path;
		
		public PromInfo2(Point peak, Point saddle) {
			this.p = peak;
			this.saddle = saddle;
		}
		
		public PromInfo2(Point p, Comparator<BasePoint> c) {
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
		
		public void finalizeForward(Front front, Point horizon) {
			this.path = front.getAtoB(horizon, this.p);
		}

		public void finalizeBackward(Front front) {
			Point thresh = front.searchThreshold(this.p, this.saddle);			
			this.path = front.getAtoB(thresh, this.p);
		}
		
		public PromInfo toNormal() {
			PromInfo pi = new PromInfo(this.p, this.c);
			pi.saddle = this.saddle;
			pi.global_max = this.global_max;
			pi.min_bound_only = this.min_bound_only;
			if (this.path != null) {
				pi.path = this.path;
			} else {
				pi.path = new ArrayList<Long>();
				pi.path.add(this.p.ix);
				pi.path.add(this.saddle.ix);
			}
			return pi;
		}
	}
		
	public static void bigOlPromSearch(Point root, TopologyNetwork tree, DEMManager.OnProm onprom, double cutoff) {
		boolean up = true;
		Comparator<BasePoint> c = BasePoint.cmpElev(up);
//		Criterion crit = new Criterion() {
//			@Override
//			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//				return cmp.compare(cur, p) > 0;
//			}			
//		};
		
		Deque<Point> peaks = new ArrayDeque<Point>(); 
		Deque<Point> saddles = new ArrayDeque<Point>(); 
		Front front = new Front(c, tree);
		front.add(root, null);
		
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
				if (front.add(adj, cur)) {
					deadEnd = false;
				}
			}
						
			if (cur.classify(tree) != Point.CLASS_SUMMIT) { // need to flip for down direction
				// saddle
				if (deadEnd) {
					// basin saddle
					continue;
				}
				while (saddles.peekFirst() != null && c.compare(cur, saddles.peekFirst()) < 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					PromInfo2 pi = new PromInfo2(peak, saddle);
					front.backwardSaddles.put(saddle, peak);
					if (pi.prominence() >= cutoff) {
						pi.finalizeBackward(front);
						onprom.onprom(pi.toNormal());
					}
				}
				saddles.addFirst(cur);
			} else {
				// peak
				if (peaks.size() == saddles.size() + 1) {
					//System.err.println("ignoring forked saddle");
					continue;
				}
				
				while (peaks.peekFirst() != null && c.compare(cur, peaks.peekFirst()) > 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					PromInfo2 pi = new PromInfo2(peak, saddle);
					front.forwardSaddles.put(saddle, peak);
					front.backwardSaddles.remove(saddle); // remove pending entry, if any
					if (pi.prominence() >= cutoff) {
						pi.finalizeForward(front, cur);
						onprom.onprom(pi.toNormal());
					}
				}
				peaks.addFirst(cur);
				front.backwardSaddles.put(saddles.peekFirst(), cur); // pending
				
				// front contains only saddles
				front.prune();
			}

			if (Math.random() < 1e-3) {
				StringBuilder sb = new StringBuilder();
				Point[] _p = peaks.toArray(new Point[0]);
				Point[] _s = saddles.toArray(new Point[0]);
				for (int i = 0; i < _p.length; i++) {
					sb.append(_p[_p.length - i - 1].elev + " ");
					int six = _s.length - i - 1;
					if (six >= 0) {
						sb.append(_s[six].elev + " ");
					}
				}
				System.err.println(sb.toString());
			}
			
			if (tree.pending.containsKey(cur)) {
				while (!saddles.isEmpty()) {
					Point peak = peaks.removeLast();
					Point saddle = saddles.removeLast();
					PromInfo2 pi = new PromInfo2(peak, saddle);
					pi.min_bound_only = true;
					if (pi.prominence() >= cutoff) {
						pi.finalizeForward(front, cur);
						onprom.onprom(pi.toNormal());
					}
				}
				return; // TODO: restart search from smaller islands and agglomerate?
			}
			
		}		
		
	}
}
