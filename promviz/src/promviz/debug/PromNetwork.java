package promviz.debug;
import java.util.Comparator;

import promviz.Point;

public class PromNetwork {

	

	static interface Criterion {
		boolean condition(Comparator<Point> cmp, Point p, Point cur);
	}
	
//	public static PromInfo prominence(TopologyNetwork tree, Point p, final boolean up) {
//		return _prominence(tree, p, up, new Criterion() {
//			@Override
//			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//				return cmp.compare(cur, p) > 0;
//			}			
//		});
//	}
//
//	public static PromInfo parent(final TopologyNetwork tree, Point p, final boolean up) {
//		final PromPair pp = PromPair.fromPeak(p, tree);
//		return _prominence(tree, p, up, new Criterion() {
//			@Override
//			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//				PromPair pp2 = PromPair.fromPeak(cur, tree);
//				if (pp2 != null) {
//					return cmpProm(cmp).compare(pp2, pp) > 0;
//				} else {
//					return false;
//				}
//			}
//		});
//	}
	
//	public static PromInfo promThresh(final TopologyNetwork tree, Point p, final boolean up) {
//		return _prominence(tree, p, up, new Criterion() {
//			@Override
//			public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
//				return tree.getMeta(cur, "prom") != null;
//			}
//		});
//	}

//	static class MSTFront {
//		Deque<Point> queue;
//		Map<Point, Point> parents;
//		Backtrace bt = null;
//				
//		public MSTFront(boolean includeBacktrace) {
//			queue = new ArrayDeque<Point>();
//			parents = new HashMap<Point, Point>();
//			if (includeBacktrace) {
//				bt = new Backtrace();
//			}
//		}
//		
//		public int size() {
//			return queue.size();
//		}
//		
//		public Point next() {
//			return queue.removeFirst();
//		}
//		
//		public void add(Point p, Point parent) {
//			if (p.equals(parents.get(parent))) {
//				return;
//			}
//			queue.addLast(p);
//			parents.put(p, parent);
//			if (bt != null) {
//				bt.add(p, parent);
//			}
//		}
//		
//		public List<Point> downstreamAdj(Point p, TopologyNetwork tree) {
//			List<Point> adjs = new ArrayList<Point>();
//			for (Point adj : tree.adjacent(p)) {
//				if (!adj.equals(parents.get(p))) {
//					adjs.add(adj);
//				}
//			}				
//			return adjs;
//		}
//		
//		public void doneWith(Point p) {
//			parents.remove(p);
//		}
//	}
	
	
//	public static List<DomainSaddleInfo> domainSaddles(boolean up, TopologyNetwork tree, Point p) {
//		final PromPair pp = PromPair.fromPeak(p, tree);
//		List<DomainSaddleInfo> saddles = new ArrayList<DomainSaddleInfo>();
//		
//		MSTFront front = new MSTFront(false);
//		MSTFront front2 = new MSTFront(false);
//		Comparator<Point> cmpE = Point.cmpElev(up);
//		Comparator<PromPair> cmp = cmpProm(cmpE);
//		
//		front.add(p, null);
//		while (front.size() > 0) {
//			Point cur = front.next();
//
//			DomainSaddleInfo dsi = null;
//			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
//			if (isSaddle) {
//				PromPair pp2 = PromPair.fromSaddle(cur, tree);
//				if (pp2 != null && cmp.compare(pp2, pp) >= 0) {
//					dsi = new DomainSaddleInfo(pp2, true, tree, cmpE, p);
//				}
//			}
//			if (dsi == null) {
//				// no significant saddle found -- keep traversing
//				for (Point adj : tree.adjacent(cur)) {
//					front.add(adj, cur);
//				}
//			} else if (dsi != null && dsi.peakIx != p.ix) {
//				// significant saddle (and not p's key saddle)
//				if (!dsi.isHigher) {
//					// continue search for strictly-higher subsaddles
//					for (Point adj : front.downstreamAdj(cur, tree)) {
//						front2.add(adj, cur);
//					}
//				}
//				saddles.add(dsi);
//			}
//
//			front.doneWith(cur);
//		}
//
//		// search further for 'height-based' subsaddles that are not part of this point's domain
//		front = null;
//		while (front2.size() > 0) {
//			Point cur = front2.next();
//			
//			DomainSaddleInfo dsi = null;
//			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
//			if (isSaddle) {
//				PromPair pp2 = PromPair.fromSaddle(cur, tree);
//				if (pp2 != null && cmp.compare(pp2, pp) >= 0) {
//					dsi = new DomainSaddleInfo(pp2, false, tree, cmpE, p);
//				}
//			}
//
//			if (dsi != null && dsi.isHigher) {
//				saddles.add(dsi);
//			} else {
//				for (Point adj : tree.adjacent(cur)) {
//					front2.add(adj, cur);
//				}
//			}
//			front2.doneWith(cur);
//		}
//		
//		return saddles;
//	}
			
//	public static PromInfo _prominence(TopologyNetwork tree, MeshPoint p, boolean up, Criterion crit) {
//		if (up != tree.up) {
//			throw new IllegalArgumentException("incompatible topology tree");
//		}
//		
//		Comparator<Point> c = Point.cmpElev(up);
//		
//		PromInfo pi = new PromInfo(p, c);
//		Front front = new Front(c, tree);
//		front.add(p, null);
//
//		Point cur = null;
//		while (true) {
//			cur = front.next();
//			if (cur == null) {
//				// we've searched the whole world
//				pi.global_max = true;
//				Logging.log("global max? " + p);
//				break;
//			}
//			pi.add(cur);
//			if (crit.condition(c, p, cur)) {
//				break;
//			}
//
//			if (tree.pendingSaddles.contains(cur)) {
//				// reached an edge
//				pi.min_bound_only = true;
//				break;
//			}
//			for (MeshPoint adj : tree.adjacent(cur)) {
//				front.add(adj, cur);
//			}
//			
//			front.prune(null, null);
//		}
//		
//		pi.finalize(front, cur);
//		return pi;
//	}

	
	static interface OnMSTEdge {
		void addEdge(Point p, Point parent, boolean isSaddle);
		void addPending(Point p);
		void finalize();
	}
	
//	static class MSTWriter implements OnMSTEdge {
//		DataOutputStream out;
//		
//		public MSTWriter(boolean up, int phase) {
//			try {
//				out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(phase, true) + "/" + (up ? "up" : "down"))));
//			} catch (IOException ioe) {
//				throw new RuntimeException();
//			}		
//		}
//		
//		public void addEdge(Point p, Point parent, boolean isSaddle) {
//			PreprocessNetwork.Edge e;
//			if (isSaddle) {
//				e = new PreprocessNetwork.Edge(p.ix, parent.ix, 0);
//			} else {
//				e = new PreprocessNetwork.Edge(parent.ix, p.ix, 1);
//			}
//			e.write(out);
//		}
//
//		public void addPending(Point p) {
//			new PreprocessNetwork.Edge(p.ix, -1, 1).write(out);
//		}
//		
//		public void finalize() {
//			try {
//				out.close();
//			} catch (IOException ioe) {
//				throw new RuntimeException();
//			}
//		}
//	}
		


	
	
	
	
//	static class CachingTopologyNetwork implements IMesh {
//		TopologyNetwork tree;
//		Map<Point, Meta> metaCache;
//		Map<Long, Point> pointCache;
//
//		// pruning
//		Set<Point> keepMeta;
//		Set<Long> keepPoint;
//		
//		public CachingTopologyNetwork(TopologyNetwork tree) {
//			this.tree = tree;
//			this.metaCache = new HashMap<Point, Meta>();
//			this.pointCache = new HashMap<Long, Point>();
//		}
//
//		public Point get(long geocode) {
//			if (keepPoint != null) {
//				keepPoint.add(geocode);
//			}
//			if (!pointCache.containsKey(geocode)) {
//				pointCache.put(geocode, tree.get(geocode));
//			}
//			return pointCache.get(geocode);
//		}
//
//		public Meta getMeta(Point p, String type) {
//			if (!type.equals("prom")) {
//				return tree.getMeta(p, type);
//			}
//			
//			if (keepMeta != null) {
//				keepMeta.add(p);
//			}
//			if (!metaCache.containsKey(p)) {
//				metaCache.put(p, tree.getMeta(p, type));
//			}
//			return metaCache.get(p);
//		}
//		
//		void startPrune() {
//			keepPoint = new HashSet<Long>();
//			keepMeta = new HashSet<Point>();
//		}
//		
//		void prune() {
//			Iterator<Point> iterMeta = metaCache.keySet().iterator();
//			while (iterMeta.hasNext()) {
//				Point p = iterMeta.next();
//				if (!keepMeta.contains(p)) {
//			        iterMeta.remove();
//			    }
//			}
//			Iterator<Long> iterPt = pointCache.keySet().iterator();
//			while (iterPt.hasNext()) {
//				Long ix = iterPt.next();
//				if (!keepPoint.contains(ix)) {
//			        iterPt.remove();
//			    }
//			}
//			
//			keepPoint = null;
//			keepMeta = null;
//		}
//	}
	
//	static boolean isUpstream(TopologyNetwork mst, MeshPoint p, MeshPoint next) {
//		int tag = p.getTag(next.ix);
//		return (tag == MeshPoint.encTag(0, false) || tag == MeshPoint.encTag(1, true));
//	}
	
//	static class ParentFront {
//		PriorityQueue<Point> queue; // the search front, akin to an expanding contour
//		Set<Long> seen; // TODO can't we traverse via edge tags instead?
//		Backtrace bt;
//		int pruneThreshold = 1; // this could start out much larger (memory-dependent) to avoid
//		                        // unnecessary pruning in the early stages
//
//		/* some of these could store Points instead of Points, since we don't need the adjacency info
//		 * and it just takes up memory. would need to ensure that the same Point object is used across
//		 * all data structures, though
//		 */
//				
//		TopologyNetwork tree;
//		CachingTopologyNetwork cache;
//		Comparator<Point> c;
//		Comparator<PromPair> cprom;
//
//		public ParentFront(final Comparator<Point> c, Comparator<PromPair> cprom, TopologyNetwork tree) {
//			this.tree = tree;
//			this.cache = new CachingTopologyNetwork(tree);
//			this.c = c;
//			this.cprom = cprom;
//			queue = new PriorityQueue<Point>(10, new ReverseComparator<Point>(c));
//			seen = new HashSet<Long>();
//			bt = new Backtrace();
//		}
//		
//		public boolean add(Point p, Point parent) {
//			if (seen.contains(p.ix)) {
//				return false;
//			}
//			
//			queue.add(p);
//			bt.add(p, parent);
//			return true;
//		}
//		
//		public Point next() {
//			Point p = queue.poll();
//			if (p != null) {
//				seen.add(p.ix);
//			}
//			return p;
//		}
//
//		public boolean isEmpty() {
//			return queue.isEmpty();
//		}
//		
//		public void prune() {
//			// when called, front must contain only saddles
//
//			// TODO: could this be made to work generationally (i.e., only deal with the
//			// portion of the front that has changed since the last prune)
//			
//			long startAt = System.currentTimeMillis();
//			if (seen.size() <= pruneThreshold) {
//				return;
//			}
//			
//			seen.retainAll(this.adjacent());
//			pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
//			
//			BacktracePruner btp = bt.pruner();
//			for (Point p : queue) {
//				btp.markPoint(p);
//			}
//			Set<Point> bookkeeping = new HashSet<Point>();
//			cache.startPrune();
//			for (Point p : queue) {
//				bulkSearchParentStart(p, btp, bookkeeping);
//			}
//			cache.prune();
//			btp.prune();
//						
//			double runTime = (System.currentTimeMillis() - startAt) / 1000.;
//			Logging.log(String.format("pruned [%.2fs] %d %d %d %d", runTime, queue.size(), bt.size(), cache.metaCache.size(), cache.pointCache.size()));
//		}
//		
//		public Set<Long> adjacent() {
//			Set<Long> frontAdj = new HashSet<Long>();
//			for (Point f : queue) {
//				for (Point adj : tree.adjacent(f)) {
//					frontAdj.add(adj.ix);
//				}
//			}
//			return frontAdj;
//		}
//		
//		public int size() {
//			return queue.size();
//		}
//		
//		public Point searchParent(Point saddle) {
//			PromPair pp = PromPair.fromSaddle(saddle, tree);
//			Point start = bt.get(saddle);
//			Point target = null;
//			for (int i = 0; i < 1000; i++) {
//				if (target != null && !bt.isLoaded(target)) {
//					bt.load(target, tree, true);
//				}
//				Iterable<Point> path = bt.getAtoB(start, target);
//
//				boolean isPeak = true;
//				Point prev = null;
//				Point lockout = null;
//				for (Point cur : path) {
//					if (lockout != null && this.c.compare(cur, lockout) < 0) {
//						lockout = null;
//					}
//					if (lockout == null) {
//						if (isPeak) {
//							PromPair pp2 = PromPair.fromPeak(cur, cache);
//							if (pp2 != null && cprom.compare(pp2, pp) > 0) {
//								return cur;
//							}
//						} else {
//							long pfIx = -1;
//							long pbIx = -1;
//							PromPair cand = PromPair.fromSaddle(cur, cache);
//							if (cand != null) {
//								if (cand.m.forward) {
//									pfIx = cand.getPeakIx();
//								} else {
//									pbIx = cand.getPeakIx();
//								}
//							}
//							
//							boolean dirForward = (prev.equals(this.bt.get(cur)));
//							long peakAwayIx = (dirForward ? pfIx : pbIx);
//							long peakTowardIx = (dirForward ? pbIx : pfIx);
//							if (peakTowardIx != -1) {
//								lockout = cur;
//							} else if (peakAwayIx != -1 && cprom.compare(cand, pp) > 0) {
//								target = cache.get(peakAwayIx);
//								break;
//							}
//						}
//					}
//
//					isPeak = !isPeak;
//					prev = cur;
//				}
//			}
//			throw new RuntimeException("infinite loop failsafe exceeded");
////			System.err.println("infinite loop failsafe exceeded " + p);
////			return saddle;
//		}
//
//		public void bulkSearchParentStart(Point saddle, BacktracePruner btp, Set<Point> bookkeeping) {
//			bulkSearchParent(bt.get(saddle), null, null, btp, bookkeeping);
//		}
//		
//		// FIXME this seems to have quadratic-like behavior, unlike bulkSearchThreshold...
//		public void bulkSearchParent(Point start, Point target, PromPair minThresh, BacktracePruner btp, Set<Point> bookkeeping) {
//			// minThresh is the equivalent of 'p' in non-bulk mode
//			
//			boolean withBailout = (bookkeeping != null);
//			
//			if (target != null && !bt.isLoaded(target)) {
//				bt.load(target, tree, true);
//			}
//			Iterable<Point> path = bt.getAtoB(start, target);
//			if (target != null) {
//				btp.markPoint(target);
//			}
//
//			boolean isPeak = true;
//			Point prev = null;
//			Point lockout = null;
//			for (Point cur : path) {
//				boolean bailoutCandidate = false;
//				
//				if (lockout != null && this.c.compare(cur, lockout) < 0) {
//					lockout = null;
//				}
//				if (lockout == null) {
//					if (isPeak) {
//						PromPair pp2 = PromPair.fromPeak(cur, cache);
//						if (pp2 != null && (minThresh == null || cprom.compare(pp2, minThresh) > 0)) {
//							minThresh = pp2;
//							bailoutCandidate = true;
//						}
//					} else {
//						long pfIx = -1;
//						long pbIx = -1;
//						PromPair cand = PromPair.fromSaddle(cur, cache);
//						if (cand != null) {
//							if (cand.m.forward) {
//								pfIx = cand.getPeakIx();
//							} else {
//								pbIx = cand.getPeakIx();
//							}
//						}
//						
//						boolean dirForward = (prev.equals(this.bt.get(cur)));
//						long peakAwayIx = (dirForward ? pfIx : pbIx);
//						long peakTowardIx = (dirForward ? pbIx : pfIx);
//						if (peakTowardIx != -1) {
//							lockout = cur;
//						} else if (peakAwayIx != -1 && (minThresh == null || cprom.compare(cand, minThresh) > 0)) {
//							Point newTarget = cache.get(peakAwayIx);
//							bulkSearchParent(start, newTarget, minThresh, btp, bookkeeping);
//							minThresh = cand;
//							bailoutCandidate = true;
//						}
//					}
//				}
//				if (bailoutCandidate && withBailout) {
//					if (bookkeeping.contains(cur)) {
//						return;
//					} else {
//						bookkeeping.add(cur);
//					}
//				}
//				
//				isPeak = !isPeak;
//				prev = cur;
//			}
//			// reached target
//		}
//		
//	}

//	static class ParentInfo {
//		long pIx;
//		Point saddle;
//		Point parent;
//		List<Long> path;
//		
//		public ParentInfo(long pIx, Point saddle, Point parent) {
//			this.pIx = pIx;
//			this.saddle = saddle;
//			this.parent = parent;
//		}
//		
//		public void finalizeForward(ParentFront front) {
//			this.path = new Path(front.bt.getAtoB(this.parent, this.saddle), null).path;
//		}
//
//		public void finalizeBackward(ParentFront front) {
//			this.parent = front.searchParent(this.saddle);			
//			this.path = new Path(front.bt.getAtoB(this.parent, this.saddle), null).path;
//		}
//	}
//	
//	static class PromPair { // TODO could load 'other' on demand only if needed
//		private Point peak;
//		private Point saddle;
//		PromMeta m;
//		IMesh tree;
//		
//		static PromPair make(Point p, IMesh tree, boolean isSaddle) {
//			PromPair pp = new PromPair();
//			pp.tree = tree;
//			pp.m = (PromMeta)tree.getMeta(p, "prom");
//			if (pp.m == null) {
//				return null;
//			}
//			if (isSaddle) {
//				pp.saddle = p;
//			} else {
//				pp.peak = p;
//			}
//			return pp;
//		}
//		static PromPair fromPeak(Point p, IMesh tree) {
//			return make(p, tree, false);
//		}
//		static PromPair fromSaddle(Point p, IMesh tree) {
//			return make(p, tree, true);
//		}
//		
//		float prominence() {
//			return m.prom;
//		}
//		
//		long getPeakIx() {
//			return (peak != null ? peak.ix : m.getPeak(true));
//		}
//		Point getPeak() {
//			if (peak == null) {
//				peak = tree.get(getPeakIx());
//			}
//			return peak;
//		}
//		
//		long getSaddleIx() {
//			return (saddle != null ? saddle.ix : m.getSaddle(false));
//		}
//		Point getSaddle() {
//			if (saddle == null) {
//				saddle = tree.get(getSaddleIx());
//			}
//			return saddle;
//		}
//	}
//	static Comparator<PromPair> cmpProm(final Comparator<Point> cmp) {
//		return new Comparator<PromPair>() {
//			public int compare(PromPair ppa, PromPair ppb) {
//				int c = Float.compare(ppa.prominence(), ppb.prominence());
//				if (c == 0) {
//					int cp = cmp.compare(ppa.getPeak(), ppb.getPeak());
//					int cs = cmp.compare(ppa.getSaddle(), ppb.getSaddle());
//					if (cp > 0 && cs < 0) {
//						c = 1;
//					} else if (cp < 0 && cs > 0) {
//						c = -1;
//					}
//				}
//				return c;
//			}
//		};
//	}
	
//	public static void bigOlPromParentSearch(boolean up, Point root, TopologyNetwork tree, DEMManager.OnPromParent onparent) {
//		root = tree.getPoint(root);
//		Comparator<Point> c = Point.cmpElev(up);
//		Comparator<PromPair> cprom = cmpProm(c);
//		
//		ParentFront front = new ParentFront(c, cprom, tree);
//		front.add(root, null);
//		
//		Deque<PromPair> pendingForward = new ArrayDeque<PromPair>();
//		
//		Point cur = null;
//		while (!front.isEmpty()) {
//			cur = front.next();
//			boolean isSaddle = (cur.classify(tree) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
//			
//			boolean reachedEdge = tree.pendingSaddles.contains(cur);
//			if (reachedEdge) {
//				break;
//			}
//			
//			for (Point adj : tree.adjacent(cur)) {
//				front.add(adj, cur);
//			}
//
//			if (isSaddle) {
//				PromPair pp = PromPair.fromSaddle(cur, tree);
//				if (pp != null) {
//					if (pp.m.forward) {
//						if (pp.getPeakIx() == root.ix) {
//							// reached the edge of the root domain, beyond which this algorithm won't work
//							break;
//						}
//						pendingForward.addFirst(pp);
//					} else {
//						ParentInfo pi = new ParentInfo(pp.getPeakIx(), cur, null);
//						pi.finalizeBackward(front);
//						onparent.onparent(pi);
//					}
//				}
//			} else {
//				PromPair pp = PromPair.fromPeak(cur, tree);
//				if (pp != null) {
//					while (!pendingForward.isEmpty()) {
//						PromPair pend = pendingForward.peekFirst();
//						if (cprom.compare(pp, pend) > 0) {
//							pendingForward.removeFirst();
//							
//							ParentInfo pi = new ParentInfo(pend.getPeakIx(), (Point)pend.getSaddle(), cur);
//							pi.finalizeForward(front);
//							onparent.onparent(pi);
//						} else {
//							break;
//						}
//					}
//				}
//		
//				// front contains only saddles
//				front.prune();
//			}
//		}
//	}
//
}
