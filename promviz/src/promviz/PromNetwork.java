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
import java.util.TreeSet;

import promviz.util.Logging;

import com.google.common.collect.Lists;

public class PromNetwork {

	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<Point> c;
		List<Long> path;
		
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
		PriorityQueue<Point> queue;
		Set<Point> set;
		Set<Long> seen;
		Map<Long, Long> backtrace;
		int pruneThreshold = 1;

		Map<Point, ThresholdTable> routing;
		
		Map<Point, Point> forwardSaddles;
		Map<Point, Point> backwardSaddles;
		
		TopologyNetwork tree;
		Comparator<Point> c;

		public Front(final Comparator<Point> c, TopologyNetwork tree) {
			this.tree = tree;
			this.c = c;
			queue = new PriorityQueue<Point>(10, new Comparator<Point>() {
				public int compare(Point p1, Point p2) {
					return -c.compare(p1, p2);
				}				
			});
			set = new HashSet<Point>();
			seen = new HashSet<Long>();
			backtrace = new HashMap<Long, Long>();
			
			routing = new HashMap<Point, ThresholdTable>();
			
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
			Point p = queue.poll();
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
			
			Logging.log("pruned " + set.size());
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
			for (Point f : queue) {
				for (Point adj : tree.adjacent(f)) {
					frontAdj.add(adj.ix);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return set.size();
		}
		
		
		Point _next(Point cur) {
			return this.tree.points.get(this.backtrace.get(cur.ix));
		}
		
//		public void updateThresholds(Point p) {
//			Point saddle = null;
//			Point cur = p;
//			Point prev = null;
//			while (true) {
//				Point s = this._next(cur);
//				if (s == null) {
//					break;
//				}
//				if (saddle == null || this.c.compare(s, saddle) < 0) {
//					saddle = s;
//				}
//				prev = cur;
//				cur = this._next(s);
//				
//				if (!this.routing.containsKey(cur)) {
//					this.routing.put(cur, new ThresholdTable(this.c));
//				}
//				if (this.c.compare(cur, p) > 0) {
//					break;
//				}
//				boolean changed = this.routing.get(cur).add(p, saddle, prev);
//				if (!changed) {
//					break;
//				}
//			}
//		}
//		
//		public Point searchThreshold(Point p, Point saddle) {
//			ThresholdEntry bestBranch = null;
//			Point cur = saddle;
//			while (true) {
//				cur = this._next(cur);
//				if (this.c.compare(cur, p) > 0) {
//					return cur;
//				}
//				
//				ThresholdEntry e = this.routing.get(cur).select(p);
//				if (e != null && (bestBranch == null || this.c.compare(e.threshold, bestBranch.threshold) > 0)) {
//					bestBranch = e;
//				}
//				
//				cur = this._next(cur);
//				if (bestBranch != null && this.c.compare(cur, bestBranch.threshold) < 0) {
//					break;
//				}
//			}
//
//			cur = bestBranch.dir;
//			while (this.c.compare(cur, p) < 0) {
//				cur = this.routing.get(cur).select(p).dir;
//			}
//			return cur;
//		}

		public Point searchThreshold2(Point p, Point saddle, Map<Point, Point> pendingSaddles) {

			double[] coords = PointIndex.toLatLon(p.ix);
			boolean DEBUG = (GeoCode.print(GeoCode.fromCoord(coords[0], coords[1])).equals("088d82bb6417a64e"));
			
			/*
			 * we have mapping saddle->peak: forwardSaddles, backwardSaddles
			 * forwardSaddles is saddles fixed via finalizeForward, etc.
			 * 
			 * we also have the list of pending peaks/saddles which we include (saddle[i], peak[i+1])
			 * in the backwardSaddles mapping
			 * 
			 * strict definition:
			 * forwardSaddles means: given the saddle, the peak is located in the direction of the backtrace
			 * backwardSaddles means: peak is located in opposite direction to the backtrace
             */

			if (DEBUG) System.err.println(">> " + p + " " + saddle);
			
			Point start = tree.get(backtrace.get(saddle.ix));
			Point target = null;
			int i = 0;
			while (true) {
				i += 1;
				if (i > 100) {
					// multiply-connected saddle???
					coords = PointIndex.toLatLon(p.ix);
					System.err.println("weirdness " + GeoCode.print(GeoCode.fromCoord(coords[0], coords[1])));
					return saddle;
				}
				
				if (DEBUG) System.err.println(start + "/" + target);
				
				List<Long> path;
				if (target == null) {
					path = new ArrayList<Long>();
					for (Long ix : this.trace(start)) {
						path.add(ix);
					}
				} else {
					path = getAtoB(start, target);
				}
				
				boolean isPeak = true;
				long prevIx = -1;
				Point lockout = null;
				for (long ix : path) {
					Point cur = this.tree.get(ix);
					if (DEBUG) System.err.println("++" + cur + " " + isPeak);

					if (lockout != null && this.c.compare(cur, lockout) < 0) {
						if (DEBUG) System.err.println("lockout cleared");
						lockout = null;
					}
					
					if (lockout == null) {
						if (isPeak) {
							if (this.c.compare(cur, p) > 0) {
								return cur;
							}
						} else {
							Point pf = forwardSaddles.get(cur);
							Point pb = backwardSaddles.get(cur);
							if (pb == null) {
								pb = pendingSaddles.get(cur);
							}
	
							Point peakF, peakB;
							boolean dirForward = (prevIx == this.backtrace.get(ix));
							if (dirForward) {
								peakF = pf;
								peakB = pb;
							} else {
								peakF = pb;
								peakB = pf;
							}
							
							if (peakB != null) {
								lockout = cur;
								if (DEBUG) System.err.println("lockout:" + peakB);
							} else if (peakF != null && this.c.compare(peakF, p) > 0) {
								if (DEBUG) System.err.println("new target " + peakF);
								target = peakF;
								for (long k : this.getAtoB(target, cur)) {
									if (DEBUG) System.err.println("   * " + tree.get(k));
								}
								
//								Set<Long> sidePath = new HashSet<Long>(this.getAtoB(target, cur));
//								for (long ix2 : path) {
//									if (sidePath.contains(ix2)) {
//										start = tree.get(ix2);
//										break;
//									}
//								}
								break;
							}
						}
					}
						
					isPeak = !isPeak;
					prevIx = ix;
				}
			}
	/*
	 * 
	 * 
	 *     new target = saddles(cur)
	 *     new start = branch point of path(old start->cur) and path(new target->cur)
	 * 
	 * 
	 */
			
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

	static class ThresholdEntry {
		Point maxima;
		Point threshold;
		Point dir;
		
		public ThresholdEntry(Point maxima, Point threshold, Point dir) {
			this.maxima = maxima;
			this.threshold = threshold;
			this.dir = dir;
		}
	}
	
	static class ThresholdTable {
		static Comparator<Point> cmp;
		List<ThresholdEntry> entries;
		
		public ThresholdTable(Comparator<Point> cmp) {
			ThresholdTable.cmp = cmp;
			entries = new ArrayList<ThresholdEntry>();
		}
		
		public boolean add(Point maxima, Point threshold, Point dir) {
			// i think, based on how we traverse the tree, that the new saddle will always be lower than everything in the table
			
			int i;
			for (i = 0; i < entries.size(); i++) {
				ThresholdEntry e = entries.get(i);
				if (cmp.compare(maxima, e.maxima) >= 0) {
					if (cmp.compare(maxima, e.maxima) == 0 && cmp.compare(threshold, e.threshold) <= 0) {
						return false;
					}
					
					if (cmp.compare(threshold, e.threshold) >= 0) {
						entries.remove(i);
						i -= 1;
					}
				} else {
					break;
				}
			}
			if (i == entries.size() || cmp.compare(threshold, entries.get(i).threshold) > 0) {
				entries.add(i, new ThresholdEntry(maxima, threshold, dir));
				return true;
			} else {
				return false;
			}
		}
		
		public ThresholdEntry select(Point thresh) {
			for (ThresholdEntry e : entries) {
				if (cmp.compare(e.maxima, thresh) > 0) {
					return e;
				}
			}
			return null;
		}
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
				Logging.log("global max? " + p.ix + " " + PointIndex.geocode(p.ix));
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
		List<Long> path;
		
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
		
		public void finalizeForward(Front front, Point horizon) {
			this.path = front.getAtoB(horizon, this.p);
		}

		public void finalizeBackward(Front front, Comparator<Point> cmp, Deque<Point> peaks, Deque<Point> saddles) {
			if (peaks.size() != saddles.size() + 1) {
				throw new RuntimeException();
			}
			Map<Point, Point> pendingSaddles = new HashMap<Point, Point>();
			List<Point> _saddles = new ArrayList<Point>(saddles);
			List<Point> _peaks = new ArrayList<Point>(peaks);
			for (int i = 0; i < saddles.size(); i++) {
				pendingSaddles.put(_saddles.get(i), _peaks.get(i));
			}
			
			Point thresh = front.searchThreshold2(this.p, this.saddle, pendingSaddles);			
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
		Comparator<Point> c = _cmp(up);
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
						
			if (cur.classify(tree) != Point.CLASS_SUMMIT) { // need to flip for down direction
				// saddle
				if (deadEnd) {
					// basin saddle
					continue;
				}
				while (saddles.peekFirst() != null && c.compare(cur, saddles.peekFirst()) < 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					// path backtracks
					PromInfo2 pi = new PromInfo2(peak, saddle);
					front.backwardSaddles.put(saddle, peak);
					if (pi.prominence() >= cutoff) {
						pi.finalizeBackward(front, c, peaks, saddles);
						onprom.onprom(pi.toNormal());
					}
				}
				saddles.addFirst(cur);
			} else {
				// peak
				if (peaks.size() == saddles.size() + 1) {
//					System.err.println("ignoring forked saddle");
					continue;
				}
				
				//front.updateThresholds(cur);
				
				while (peaks.peekFirst() != null && c.compare(cur, peaks.peekFirst()) > 0) {
					Point saddle = saddles.removeFirst();
					Point peak = peaks.removeFirst();
					// path goes forward
					PromInfo2 pi = new PromInfo2(peak, saddle);
					front.forwardSaddles.put(saddle, peak);
					if (pi.prominence() >= cutoff) {
						pi.finalizeForward(front, cur);
						onprom.onprom(pi.toNormal());
					}
				}
				peaks.addFirst(cur);
				
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
