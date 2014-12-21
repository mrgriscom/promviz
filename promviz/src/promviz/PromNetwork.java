package promviz;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
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

import com.google.common.collect.Iterables;
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
		PriorityQueue<BasePoint> queue; // the search front, akin to an expanding contour
		Set<Point> set; // set of all points in 'queue'
		Set<Long> seen;
		Map<Long, Long> backtrace;
		int pruneThreshold = 1; // this could start out much larger (memory-dependent) to avoid
		                        // unnecessary pruning in the early stages

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

//		void treeDFS(Map<Long, Set<Long>> tree, Long cur, List<Long> flat) {
//			flat.add(cur);
//			Set<Long> children = tree.get(cur);
//			if (children != null) {
//				for (Long child : children) {
//					treeDFS(tree, child, flat);
//				}
//			}
//		}
		
		static interface OnMarkPoint {
			void markPoint(Point p);
		}
		
		public void prune(Collection<Point> pendingPeaks, Collection<Point> pendingSaddles) {
			// when called, front must contain only saddles

			// TODO: could this be made to work generationally (i.e., only deal with the
			// portion of the front that has changed since the last prune)
			
			long startAt = System.currentTimeMillis();
			if (seen.size() <= pruneThreshold) {
				return;
			}
			
			seen.retainAll(this.adjacent());
			pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
			
			final Set<Long> backtraceKeep = new HashSet<Long>();
			OnMarkPoint marker = new OnMarkPoint() {
				public void markPoint(Point p) {
					for (Long t : trace(p)) {
						boolean newItem = backtraceKeep.add(t);
						if (!newItem) {
							break;
						}
					}
				}
			};

			for (Point p : Iterables.concat(set, pendingPeaks)) {
				marker.markPoint(p);
			}
			Set<Point> bookkeeping = new HashSet<Point>();
			Set<Point> significantSaddles = new HashSet<Point>(pendingSaddles);
			for (Point p : Iterables.concat(set, pendingSaddles)) {
				bulkSearchThresholdStart(p, marker, bookkeeping, significantSaddles);
			}
			
			Iterator<Long> iterBT = backtrace.keySet().iterator();
			while (iterBT.hasNext()) {
				long p = iterBT.next();
				if (!backtraceKeep.contains(p)) {
			        iterBT.remove();
			    }
			}
			Iterator<Point> iterFS = forwardSaddles.keySet().iterator();
			while (iterFS.hasNext()) {
				Point p = iterFS.next();
				if (!significantSaddles.contains(p)) {
			        iterFS.remove();
			    }
			}
			Iterator<Point> iterBS = backwardSaddles.keySet().iterator();
			while (iterBS.hasNext()) {
				Point p = iterBS.next();
				if (!significantSaddles.contains(p)) {
			        iterBS.remove();
			    }
			}
						
			double runTime = (System.currentTimeMillis() - startAt) / 1000.;
			Logging.log(String.format("pruned [%.2fs] %d %d %d %d", runTime, set.size(), backtrace.size(), forwardSaddles.size(), backwardSaddles.size()));
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
			 * backwardSaddles also includes all pending saddle/peak pairs
			 * 
			 * strict definition:
			 * forwardSaddles means: given the saddle, the peak is located in the direction of the backtrace
			 * backwardSaddles means: peak is located in opposite direction to the backtrace
             */

			Point start = _next(saddle);
			Point target = null;
			for (int i = 0; i < 1000; i++) {
				Iterable<Long> path = (target == null ? this.trace(start) : getAtoB(start, target));
				start = null;
				
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
							if (start == null || this.c.compare(cur, start) > 0) {
								start = cur;
								if (this.c.compare(start, p) > 0) {
									return start;
								}
							}
						} else {
							Point pf = forwardSaddles.get(cur);
							Point pb = backwardSaddles.get(cur);
							boolean dirForward = (prevIx == this.backtrace.get(ix));
							Point peakAway = (dirForward ? pf : pb);
							Point peakToward = (dirForward ? pb : pf);
							if (peakToward != null && this.c.compare(peakToward, start) > 0) {
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
			throw new RuntimeException("infinite loop failsafe exceeded");
//			System.err.println("infinite loop failsafe exceeded " + p);
//			return saddle;
		}

		public void bulkSearchThresholdStart(Point saddle, OnMarkPoint markPoint, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			bulkSearchThreshold(_next(saddle), null, null, markPoint, bookkeeping, significantSaddles);
		}
		
		public void bulkSearchThreshold(Point start, Point target, Point minThresh, OnMarkPoint markPoint, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			// minThresh is the equivalent of 'p' in non-bulk mode
			
			boolean withBailout = (bookkeeping != null);
			
			Iterable<Long> path;
			if (target == null) {
				path = this.trace(start);
			} else {
				path = getAtoB(start, target);
				markPoint.markPoint(target);
			}
			start = null;
						
			boolean isPeak = true;
			long prevIx = -1;
			Point lockout = null;
			for (long ix : path) {
				Point cur = this.tree.get(ix);
				boolean bailoutCandidate = false;
				
				if (lockout != null && this.c.compare(cur, lockout) < 0) {
					lockout = null;
				}
				if (lockout == null) {
					if (isPeak) {
						if (start == null || this.c.compare(cur, start) > 0) {
							start = cur;
							if (minThresh == null || this.c.compare(start, minThresh) > 0) {
								minThresh = start;
								bailoutCandidate = true;
							}
						}
					} else {
						Point pf = forwardSaddles.get(cur);
						Point pb = backwardSaddles.get(cur);
						boolean dirForward = (prevIx == this.backtrace.get(ix));
						Point peakAway = (dirForward ? pf : pb);
						Point peakToward = (dirForward ? pb : pf);
						if (peakToward != null) {
							// i don't think we can filter based on 'start' like in non-bulk mode because
							// different paths might have differing 'start's at any given time even though
							// they ultimately find the same peaks. if the processing order changed things
							// would break? unfortunately that means every 'toward' saddle is significant
							significantSaddles.add(cur);
							lockout = cur;
						} else if (peakAway != null && this.c.compare(peakAway, minThresh) > 0) {
							significantSaddles.add(cur);
							Point newTarget = peakAway;
							bulkSearchThreshold(start, newTarget, minThresh, markPoint, bookkeeping, significantSaddles);
							minThresh = newTarget;
							bailoutCandidate = true;
						}
					}
				}
				if (bailoutCandidate && withBailout) {
					if (bookkeeping.contains(cur)) {
						return;
					} else {
						bookkeeping.add(cur);
					}
				}
				
				isPeak = !isPeak;
				prevIx = ix;
			}
			// reached target
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

			if (tree.pendingSaddles.contains(cur)) {
				// reached an edge
				pi.min_bound_only = true;
				break;
			}
			for (Point adj : tree.adjacent(cur)) {
				front.add(adj, cur);
			}
			
			front.prune(null, null);
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
		
	public static void bigOlPromSearch(boolean up, Point root, TopologyNetwork tree, DEMManager.OnProm onprom, double cutoff) {
		root = tree.getPoint(root);
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
				break;
			}
			
			boolean reachedEdge = tree.pendingSaddles.contains(cur);
			boolean deadEnd = !reachedEdge;
			for (Point adj : tree.adjacent(cur)) {
				if (front.add(adj, cur)) {
					deadEnd = false;
				}
			}
						
			if (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
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
				front.prune(peaks, saddles);
			}

			if (reachedEdge) {
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
				break; // TODO: restart search from smaller islands and agglomerate?
			}
			
			// debug output
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
			
		}		
		
	}
}
