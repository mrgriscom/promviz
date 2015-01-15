package promviz;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

import promviz.PreprocessNetwork.EdgeIterator;
import promviz.PreprocessNetwork.Meta;
import promviz.PreprocessNetwork.PromMeta;
import promviz.PromNetwork.Backtrace.BacktracePruner;
import promviz.util.Logging;
import promviz.util.ReverseComparator;

import com.google.common.collect.Iterables;

public class PromNetwork {

	static class Path {
		List<Long> path;
		double thresholdFactor = -1;
		
		public Path(Iterable<Point> path, Point ref) {
			this.path = new ArrayList<Long>();
			Point[] endSeg = new Point[2];
			int i = 0;
			for (Point p : path) {
				this.path.add(p.ix);
				
				if (i < 2) {
					endSeg[i] = p;
				}
				i++;
			}

			if (ref != null && this.path.size() >= 2) {
				Point last = endSeg[0];
				Point nextToLast = endSeg[1];
				thresholdFactor = (ref.elev - nextToLast.elev) / (last.elev - nextToLast.elev);
			}
		}
	}
	
	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<BasePoint> c;
		List<Long> path;
		boolean forward;
		double thresholdFactor = -1;
		
		public PromInfo(Point p, Comparator<BasePoint> c) {
			this.p = p;
			this.c = c;
		}
		
		public float prominence() {
			return Math.abs(p.elev - saddle.elev);
		}
		
		public void add(Point cur) {
			if (saddle == null || c.compare(cur, saddle) < 0) {
				saddle = cur;
			}
		}
		
		public void finalize(Front f, Point horizon) {
			if (horizon != null) {
				Path _ = new Path(f.bt.trace(horizon), this.p);
				this.path = _.path;
				this.thresholdFactor = _.thresholdFactor;
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
	
	static class Backtrace {
		Map<Point, Point> backtrace;
		Point root;
		
		public Backtrace() {
			this.backtrace = new HashMap<Point, Point>();
		}
		
		public void add(Point p, Point parent) {
			if (parent == null) {
				root = p;
			} else {
				backtrace.put(p, parent);
			}
		}
		
		public Point get(Point p) {
			Point parent = backtrace.get(p);
			if (parent == null && !p.equals(root)) {
				throw new RuntimeException("point not loaded");
			}
			return parent;
		}
		
		public boolean isLoaded(Point p) {
			return backtrace.containsKey(p) || p.equals(root);
		}
		
		public int size() {
			return backtrace.size();
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
				this.cur = get(this.cur);
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
		
		static interface BacktracePruner {
			void markPoint(Point p);
			void prune();
		}
		
		public BacktracePruner pruner() {
			final Set<Point> backtraceKeep = new HashSet<Point>();

			return new BacktracePruner() {
				public void markPoint(Point p) {
					for (Point t : trace(p)) {
						boolean newItem = backtraceKeep.add(t);
						if (!newItem) {
							break;
						}
					}
				}

				public void prune() {
					Iterator<Point> iterBT = backtrace.keySet().iterator();
					while (iterBT.hasNext()) {
						Point p = iterBT.next();
						if (!backtraceKeep.contains(p)) {
					        iterBT.remove();
					    }
					}
				}
			};
		}
		
		public Iterable<Point> getAtoB(Point pA, Point pB) {
			if (pB == null) {
				return trace(pA);
			}
			
			List<Point> fromA = new ArrayList<Point>();
			List<Point> fromB = new ArrayList<Point>();
			Set<Point> inFromA = new HashSet<Point>();
			Set<Point> inFromB = new HashSet<Point>();

			Point intersection;
			Point curA = pA;
			Point curB = pB;
			while (true) {
				if (curA != null && curB != null && curA.equals(curB)) {
					intersection = curA;
					break;
				}
				
				if (curA != null) {
					fromA.add(curA);
					inFromA.add(curA);
					curA = this.get(curA);
				}
				if (curB != null) {
					fromB.add(curB);
					inFromB.add(curB);
					curB = this.get(curB);
				}
					
				if (inFromA.contains(curB)) {
					intersection = curB;
					break;
				} else if (inFromB.contains(curA)) {
					intersection = curA;
					break;
				}
			}

			List<Point> path = new ArrayList<Point>();
			int i = fromA.indexOf(intersection);
			path.addAll(i != -1 ? fromA.subList(0, i) : fromA);
			path.add(intersection);
			List<Point> path2 = new ArrayList<Point>();
			i = fromB.indexOf(intersection);
			path2 = (i != -1 ? fromB.subList(0, i) : fromB);
			Collections.reverse(path2);
			path.addAll(path2);
			return path;
		}

		void load(Point p, TopologyNetwork tree, boolean isPeak) {
			Point next;
			while (!isLoaded(p)) {
				if (isPeak) {
					next = tree.get(p.getByTag(1, true));
				} else {
					next = tree.get(p.getByTag(0, false));					
				}
				this.add(p, next);
				p = next;
				isPeak = !isPeak;
			}
		}		
	}
	
	static class Front {
		PriorityQueue<Point> queue; // the search front, akin to an expanding contour
		Set<Point> set; // set of all points in 'queue'
		Set<Long> seen; // TODO is this redundant due to backtrace?
		Backtrace bt;
		int pruneThreshold = 1; // this could start out much larger (memory-dependent) to avoid
		                        // unnecessary pruning in the early stages

		/* some of these could store BasePoints instead of Points, since we don't need the adjacency info
		 * and it just takes up memory. would need to ensure that the same BasePoint object is used across
		 * all data structures, though
		 */
		
		Map<Point, Point> forwardSaddles;
		Map<Point, Point> backwardSaddles;
		
		TopologyNetwork tree;
		Comparator<BasePoint> c;

		public Front(final Comparator<BasePoint> c, TopologyNetwork tree) {
			this.tree = tree;
			this.c = c;
			queue = new PriorityQueue<Point>(10, new ReverseComparator<BasePoint>(c));
			set = new HashSet<Point>();
			seen = new HashSet<Long>();
			bt = new Backtrace();
			
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
				bt.add(p, parent);
			}
			return newItem;
		}
		
		public Point next() {
			Point p = queue.poll();
			if (p != null) {
				set.remove(p);
				seen.add(p.ix);
			}
			return p;
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

			BacktracePruner btp = bt.pruner();

			// concession for 'old school' mode
			if (pendingPeaks == null) {
				pendingPeaks = new ArrayList<Point>();
				pendingSaddles = new ArrayList<Point>();
			}
			
			for (Point p : Iterables.concat(queue, pendingPeaks)) {
				btp.markPoint(p);
			}
			Set<Point> bookkeeping = new HashSet<Point>();
			Set<Point> significantSaddles = new HashSet<Point>(pendingSaddles);
			for (Point p : Iterables.concat(queue, pendingSaddles)) {
				bulkSearchThresholdStart(p, btp, bookkeeping, significantSaddles);
			}
			btp.prune();

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
			Logging.log(String.format("pruned [%.2fs] %d %d %d %d", runTime, queue.size(), bt.size(), forwardSaddles.size(), backwardSaddles.size()));
		}
		
		public Set<Long> adjacent() {
			Set<Long> frontAdj = new HashSet<Long>();
			for (Point f : queue) {
				for (Point adj : tree.adjacent(f)) {
					frontAdj.add(adj.ix);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return queue.size();
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

			Point start = bt.get(saddle);
			Point target = null;
			for (int i = 0; i < 1000; i++) {
				Iterable<Point> path = bt.getAtoB(start, target);
				start = null;
				
				boolean isPeak = true;
				Point prev = null;
				Point lockout = null;
				for (Point cur : path) {
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
							boolean dirForward = (prev.equals(this.bt.get(cur)));
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
					prev = cur;
				}
			}
			throw new RuntimeException("infinite loop failsafe exceeded");
//			System.err.println("infinite loop failsafe exceeded " + p);
//			return saddle;
		}

		public void bulkSearchThresholdStart(Point saddle, BacktracePruner btp, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			bulkSearchThreshold(bt.get(saddle), null, null, btp, bookkeeping, significantSaddles);
		}
		
		public void bulkSearchThreshold(Point start, Point target, Point minThresh, BacktracePruner btp, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			// minThresh is the equivalent of 'p' in non-bulk mode
			
			boolean withBailout = (bookkeeping != null);
			
			Iterable<Point> path = bt.getAtoB(start, target);
			if (target != null) {
				btp.markPoint(target);
			}
			start = null;
						
			boolean isPeak = true;
			Point prev = null;
			Point lockout = null;
			for (Point cur : path) {
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
						boolean dirForward = (prev.equals(this.bt.get(cur)));
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
							bulkSearchThreshold(start, newTarget, minThresh, btp, bookkeeping, significantSaddles);
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
				prev = cur;
			}
			// reached target
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

	public static PromInfo parent(final TopologyNetwork tree, Point p, final boolean up) {
		final PromPair pp = PromPair.fromPeak(p, tree);
		return _prominence(tree, p, up, new Criterion() {
			@Override
			public boolean condition(Comparator<BasePoint> cmp, BasePoint p, BasePoint cur) {
				PromPair pp2 = PromPair.fromPeak(cur, tree);
				if (pp2 != null) {
					return cmpProm(cmp).compare(pp2, pp) > 0;
				} else {
					return false;
				}
			}
		});
	}
	
	public static PromInfo promThresh(final TopologyNetwork tree, Point p, final boolean up) {
		return _prominence(tree, p, up, new Criterion() {
			@Override
			public boolean condition(Comparator<BasePoint> cmp, BasePoint p, BasePoint cur) {
				return tree.getMeta(cur, "prom") != null;
			}
		});
	}

	static class MSTFront {
		Deque<Point> queue;
		Map<Point, Point> parents;
		Backtrace bt = null;
				
		public MSTFront(boolean includeBacktrace) {
			queue = new ArrayDeque<Point>();
			parents = new HashMap<Point, Point>();
			if (includeBacktrace) {
				bt = new Backtrace();
			}
		}
		
		public int size() {
			return queue.size();
		}
		
		public Point next() {
			return queue.removeFirst();
		}
		
		public void add(Point p, Point parent) {
			if (p.equals(parents.get(parent))) {
				return;
			}
			queue.addLast(p);
			parents.put(p, parent);
			if (bt != null) {
				bt.add(p, parent);
			}
		}
		
		public void doneWith(Point p) {
			parents.remove(p);
		}
	}
	
	public static Map<Point, Long> domainSaddles(boolean up, TopologyNetwork tree, Point p) {
		final PromPair pp = PromPair.fromPeak(p, tree);
		Map<Point, Long> saddles = new HashMap<Point, Long>();
		
		MSTFront front = new MSTFront(false);
		Comparator<PromPair> cmp = cmpProm(BasePoint.cmpElev(up));
		
		front.add(p, null);
		while (front.size() > 0) {
			Point cur = front.next();

			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
			if (isSaddle) {
				PromPair pp2 = PromPair.fromSaddle(cur, tree);
				if (pp2 != null && cmp.compare(pp2, pp) >= 0) {
					if (pp2.getPeakIx() != p.ix) {
						saddles.put(cur, pp2.getPeakIx());
					}
					continue;
				}
			}
				
			for (Point adj : tree.adjacent(cur)) {
				front.add(adj, cur);
			}
			front.doneWith(cur);
		}

		return saddles;
	}
			
	public static PromInfo _prominence(TopologyNetwork tree, Point p, boolean up, Criterion crit) {
		if (up != tree.up) {
			throw new IllegalArgumentException("incompatible topology tree");
		}
		
		Comparator<BasePoint> c = BasePoint.cmpElev(up);
		
		PromInfo pi = new PromInfo(p, c);
		Front front = new Front(c, tree);
		front.add(p, null);

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
		boolean forwardSaddle;
		double thresholdFactor = -1;
		
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
			Path _ = new Path(front.bt.getAtoB(horizon, this.p), this.p);
			this.path = _.path;
			this.thresholdFactor = _.thresholdFactor;
			forwardSaddle = true;
		}

		public void finalizeBackward(Front front) {
			Point thresh = front.searchThreshold(this.p, this.saddle);			
			Path _ = new Path(front.bt.getAtoB(thresh, this.p), this.p);
			this.path = _.path;
			this.thresholdFactor = _.thresholdFactor;
			forwardSaddle = false;
		}
		
		public PromInfo toNormal() {
			PromInfo pi = new PromInfo(this.p, this.c);
			pi.saddle = this.saddle;
			pi.global_max = this.global_max;
			pi.min_bound_only = this.min_bound_only;
			pi.forward = this.forwardSaddle;
			if (this.path != null) {
				pi.path = this.path;
				pi.thresholdFactor = this.thresholdFactor;
			} else {
				pi.path = new ArrayList<Long>();
				pi.path.add(this.p.ix);
				pi.path.add(this.saddle.ix);
			}
			return pi;
		}
	}
	
	static interface OnMSTEdge {
		void addEdge(Point p, Point parent, boolean isSaddle);
		void addPending(Point p);
		void finalize();
	}
	
	static class MSTWriter implements OnMSTEdge {
		DataOutputStream out;
		
		public MSTWriter(boolean up, int phase) {
			try {
				out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(phase, true) + "/" + (up ? "up" : "down"))));
			} catch (IOException ioe) {
				throw new RuntimeException();
			}		
		}
		
		public void addEdge(Point p, Point parent, boolean isSaddle) {
			PreprocessNetwork.Edge e;
			if (isSaddle) {
				e = new PreprocessNetwork.Edge(p.ix, parent.ix, 0);
			} else {
				e = new PreprocessNetwork.Edge(parent.ix, p.ix, 1);
			}
			e.write(out);
		}

		public void addPending(Point p) {
			new PreprocessNetwork.Edge(p.ix, -1, 1).write(out);
		}
		
		public void finalize() {
			try {
				out.close();
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
		}
	}
		
	public static void bigOlPromSearch(boolean up, Point root, TopologyNetwork tree, DEMManager.OnProm onprom, OnMSTEdge onedge, double cutoff) {
		root = tree.getPoint(root);
		Comparator<BasePoint> c = BasePoint.cmpElev(up);
		
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
			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
			
			boolean reachedEdge = tree.pendingSaddles.contains(cur);
			boolean deadEnd = !reachedEdge;
			for (Point adj : tree.adjacent(cur)) {
				if (front.add(adj, cur)) {
					deadEnd = false;
					onedge.addEdge(adj, cur, !isSaddle); // type of 'adj' is opposite 'cur'
				}
			}
						
			if (isSaddle) {
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
				onedge.addPending(cur);
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
				Logging.log(sb.toString());
			}
		}		
		
		onedge.finalize();
	}
	
	
	
	
	
	static class CachingTopologyNetwork implements IMesh {
		TopologyNetwork tree;
		Map<BasePoint, Meta> metaCache;
		Map<Long, Point> pointCache;

		// pruning
		Set<BasePoint> keepMeta;
		Set<Long> keepPoint;
		
		public CachingTopologyNetwork(TopologyNetwork tree) {
			this.tree = tree;
			this.metaCache = new HashMap<BasePoint, Meta>();
			this.pointCache = new HashMap<Long, Point>();
		}

		public Point get(long geocode) {
			if (keepPoint != null) {
				keepPoint.add(geocode);
			}
			if (!pointCache.containsKey(geocode)) {
				pointCache.put(geocode, tree.get(geocode));
			}
			return pointCache.get(geocode);
		}

		public Meta getMeta(BasePoint p, String type) {
			if (!type.equals("prom")) {
				return tree.getMeta(p, type);
			}
			
			if (keepMeta != null) {
				keepMeta.add(p);
			}
			if (!metaCache.containsKey(p)) {
				metaCache.put(p, tree.getMeta(p, type));
			}
			return metaCache.get(p);
		}
		
		void startPrune() {
			keepPoint = new HashSet<Long>();
			keepMeta = new HashSet<BasePoint>();
		}
		
		void prune() {
			Iterator<BasePoint> iterMeta = metaCache.keySet().iterator();
			while (iterMeta.hasNext()) {
				BasePoint p = iterMeta.next();
				if (!keepMeta.contains(p)) {
			        iterMeta.remove();
			    }
			}
			Iterator<Long> iterPt = pointCache.keySet().iterator();
			while (iterPt.hasNext()) {
				Long ix = iterPt.next();
				if (!keepPoint.contains(ix)) {
			        iterPt.remove();
			    }
			}
			
			keepPoint = null;
			keepMeta = null;
		}
	}
	
	static boolean isUpstream(TopologyNetwork mst, Point p, Point next) {
		int tag = p.getTag(next.ix);
		return (tag == Point.encTag(0, false) || tag == Point.encTag(1, true));
	}
	
	static class ParentFront {
		PriorityQueue<Point> queue; // the search front, akin to an expanding contour
		Set<Long> seen; // TODO can't we traverse via edge tags instead?
		Backtrace bt;
		int pruneThreshold = 1; // this could start out much larger (memory-dependent) to avoid
		                        // unnecessary pruning in the early stages

		/* some of these could store BasePoints instead of Points, since we don't need the adjacency info
		 * and it just takes up memory. would need to ensure that the same BasePoint object is used across
		 * all data structures, though
		 */
				
		TopologyNetwork tree;
		CachingTopologyNetwork cache;
		Comparator<BasePoint> c;
		Comparator<PromPair> cprom;

		public ParentFront(final Comparator<BasePoint> c, Comparator<PromPair> cprom, TopologyNetwork tree) {
			this.tree = tree;
			this.cache = new CachingTopologyNetwork(tree);
			this.c = c;
			this.cprom = cprom;
			queue = new PriorityQueue<Point>(10, new ReverseComparator<BasePoint>(c));
			seen = new HashSet<Long>();
			bt = new Backtrace();
		}
		
		public boolean add(Point p, Point parent) {
			if (seen.contains(p.ix)) {
				return false;
			}
			
			queue.add(p);
			bt.add(p, parent);
			return true;
		}
		
		public Point next() {
			Point p = queue.poll();
			if (p != null) {
				seen.add(p.ix);
			}
			return p;
		}

		public boolean isEmpty() {
			return queue.isEmpty();
		}
		
		public void prune() {
			// when called, front must contain only saddles

			// TODO: could this be made to work generationally (i.e., only deal with the
			// portion of the front that has changed since the last prune)
			
			long startAt = System.currentTimeMillis();
			if (seen.size() <= pruneThreshold) {
				return;
			}
			
			seen.retainAll(this.adjacent());
			pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
			
			BacktracePruner btp = bt.pruner();
			for (Point p : queue) {
				btp.markPoint(p);
			}
			Set<Point> bookkeeping = new HashSet<Point>();
			cache.startPrune();
			for (Point p : queue) {
				bulkSearchParentStart(p, btp, bookkeeping);
			}
			cache.prune();
			btp.prune();
						
			double runTime = (System.currentTimeMillis() - startAt) / 1000.;
			Logging.log(String.format("pruned [%.2fs] %d %d %d %d", runTime, queue.size(), bt.size(), cache.metaCache.size(), cache.pointCache.size()));
		}
		
		public Set<Long> adjacent() {
			Set<Long> frontAdj = new HashSet<Long>();
			for (Point f : queue) {
				for (Point adj : tree.adjacent(f)) {
					frontAdj.add(adj.ix);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return queue.size();
		}
		
		public Point searchParent(Point saddle) {
			PromPair pp = PromPair.fromSaddle(saddle, tree);
			Point start = bt.get(saddle);
			Point target = null;
			for (int i = 0; i < 1000; i++) {
				if (target != null && !bt.isLoaded(target)) {
					bt.load(target, tree, true);
				}
				Iterable<Point> path = bt.getAtoB(start, target);

				boolean isPeak = true;
				Point prev = null;
				Point lockout = null;
				for (Point cur : path) {
					if (lockout != null && this.c.compare(cur, lockout) < 0) {
						lockout = null;
					}
					if (lockout == null) {
						if (isPeak) {
							PromPair pp2 = PromPair.fromPeak(cur, cache);
							if (pp2 != null && cprom.compare(pp2, pp) > 0) {
								return cur;
							}
						} else {
							long pfIx = -1;
							long pbIx = -1;
							PromPair cand = PromPair.fromSaddle(cur, cache);
							if (cand != null) {
								if (cand.m.forward) {
									pfIx = cand.getPeakIx();
								} else {
									pbIx = cand.getPeakIx();
								}
							}
							
							boolean dirForward = (prev.equals(this.bt.get(cur)));
							long peakAwayIx = (dirForward ? pfIx : pbIx);
							long peakTowardIx = (dirForward ? pbIx : pfIx);
							if (peakTowardIx != -1) {
								lockout = cur;
							} else if (peakAwayIx != -1 && cprom.compare(cand, pp) > 0) {
								target = cache.get(peakAwayIx);
								break;
							}
						}
					}

					isPeak = !isPeak;
					prev = cur;
				}
			}
			throw new RuntimeException("infinite loop failsafe exceeded");
//			System.err.println("infinite loop failsafe exceeded " + p);
//			return saddle;
		}

		public void bulkSearchParentStart(Point saddle, BacktracePruner btp, Set<Point> bookkeeping) {
			bulkSearchParent(bt.get(saddle), null, null, btp, bookkeeping);
		}
		
		// FIXME this seems to have quadratic-like behavior, unlike bulkSearchThreshold...
		public void bulkSearchParent(Point start, Point target, PromPair minThresh, BacktracePruner btp, Set<Point> bookkeeping) {
			// minThresh is the equivalent of 'p' in non-bulk mode
			
			boolean withBailout = (bookkeeping != null);
			
			if (target != null && !bt.isLoaded(target)) {
				bt.load(target, tree, true);
			}
			Iterable<Point> path = bt.getAtoB(start, target);
			if (target != null) {
				btp.markPoint(target);
			}

			boolean isPeak = true;
			Point prev = null;
			Point lockout = null;
			for (Point cur : path) {
				boolean bailoutCandidate = false;
				
				if (lockout != null && this.c.compare(cur, lockout) < 0) {
					lockout = null;
				}
				if (lockout == null) {
					if (isPeak) {
						PromPair pp2 = PromPair.fromPeak(cur, cache);
						if (pp2 != null && (minThresh == null || cprom.compare(pp2, minThresh) > 0)) {
							minThresh = pp2;
							bailoutCandidate = true;
						}
					} else {
						long pfIx = -1;
						long pbIx = -1;
						PromPair cand = PromPair.fromSaddle(cur, cache);
						if (cand != null) {
							if (cand.m.forward) {
								pfIx = cand.getPeakIx();
							} else {
								pbIx = cand.getPeakIx();
							}
						}
						
						boolean dirForward = (prev.equals(this.bt.get(cur)));
						long peakAwayIx = (dirForward ? pfIx : pbIx);
						long peakTowardIx = (dirForward ? pbIx : pfIx);
						if (peakTowardIx != -1) {
							lockout = cur;
						} else if (peakAwayIx != -1 && (minThresh == null || cprom.compare(cand, minThresh) > 0)) {
							Point newTarget = cache.get(peakAwayIx);
							bulkSearchParent(start, newTarget, minThresh, btp, bookkeeping);
							minThresh = cand;
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
				prev = cur;
			}
			// reached target
		}
		
	}

	static class ParentInfo {
		long pIx;
		Point saddle;
		Point parent;
		List<Long> path;
		
		public ParentInfo(long pIx, Point saddle, Point parent) {
			this.pIx = pIx;
			this.saddle = saddle;
			this.parent = parent;
		}
		
		public void finalizeForward(ParentFront front) {
			this.path = new Path(front.bt.getAtoB(this.parent, this.saddle), null).path;
		}

		public void finalizeBackward(ParentFront front) {
			this.parent = front.searchParent(this.saddle);			
			this.path = new Path(front.bt.getAtoB(this.parent, this.saddle), null).path;
		}
	}
	
	static class PromPair { // TODO could load 'other' on demand only if needed
		private BasePoint peak;
		private BasePoint saddle;
		PromMeta m;
		IMesh tree;
		
		static PromPair make(BasePoint p, IMesh tree, boolean isSaddle) {
			PromPair pp = new PromPair();
			pp.tree = tree;
			pp.m = (PromMeta)tree.getMeta(p, "prom");
			if (pp.m == null) {
				return null;
			}
			if (isSaddle) {
				pp.saddle = p;
			} else {
				pp.peak = p;
			}
			return pp;
		}
		static PromPair fromPeak(BasePoint p, IMesh tree) {
			return make(p, tree, false);
		}
		static PromPair fromSaddle(BasePoint p, IMesh tree) {
			return make(p, tree, true);
		}
		
		float prominence() {
			return m.prom;
		}
		
		long getPeakIx() {
			return (peak != null ? peak.ix : m.getPeak(true));
		}
		BasePoint getPeak() {
			if (peak == null) {
				peak = tree.get(getPeakIx());
			}
			return peak;
		}
		
		long getSaddleIx() {
			return (saddle != null ? saddle.ix : m.getSaddle(false));
		}
		BasePoint getSaddle() {
			if (saddle == null) {
				saddle = tree.get(getSaddleIx());
			}
			return saddle;
		}
	}
	static Comparator<PromPair> cmpProm(final Comparator<BasePoint> cmp) {
		return new Comparator<PromPair>() {
			public int compare(PromPair ppa, PromPair ppb) {
				int c = Float.compare(ppa.prominence(), ppb.prominence());
				if (c == 0) {
					int cp = cmp.compare(ppa.getPeak(), ppb.getPeak());
					int cs = cmp.compare(ppa.getSaddle(), ppb.getSaddle());
					if (cp > 0 && cs < 0) {
						c = 1;
					} else if (cp < 0 && cs > 0) {
						c = -1;
					}
				}
				return c;
			}
		};
	}
	
	public static void bigOlPromParentSearch(boolean up, Point root, TopologyNetwork tree, DEMManager.OnPromParent onparent) {
		root = tree.getPoint(root);
		Comparator<BasePoint> c = BasePoint.cmpElev(up);
		Comparator<PromPair> cprom = cmpProm(c);
		
		ParentFront front = new ParentFront(c, cprom, tree);
		front.add(root, null);
		
		Deque<PromPair> pendingForward = new ArrayDeque<PromPair>();
		
		Point cur = null;
		while (!front.isEmpty()) {
			cur = front.next();
			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
			
			boolean reachedEdge = tree.pendingSaddles.contains(cur);
			if (reachedEdge) {
				break;
			}
			
			for (Point adj : tree.adjacent(cur)) {
				front.add(adj, cur);
			}

			if (isSaddle) {
				PromPair pp = PromPair.fromSaddle(cur, tree);
				if (pp != null) {
					if (pp.m.forward) {
						if (pp.getPeakIx() == root.ix) {
							// reached the edge of the root domain, beyond which this algorithm won't work
							break;
						}
						pendingForward.addFirst(pp);
					} else {
						ParentInfo pi = new ParentInfo(pp.getPeakIx(), cur, null);
						pi.finalizeBackward(front);
						onparent.onparent(pi);
					}
				}
			} else {
				PromPair pp = PromPair.fromPeak(cur, tree);
				if (pp != null) {
					while (!pendingForward.isEmpty()) {
						PromPair pend = pendingForward.peekFirst();
						if (cprom.compare(pp, pend) > 0) {
							pendingForward.removeFirst();
							
							ParentInfo pi = new ParentInfo(pend.getPeakIx(), (Point)pend.getSaddle(), cur);
							pi.finalizeForward(front);
							onparent.onparent(pi);
						} else {
							break;
						}
					}
				}
		
				// front contains only saddles
				front.prune();
			}
		}
	}

}
