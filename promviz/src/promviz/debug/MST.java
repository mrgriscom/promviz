package promviz.debug;


public class MST {

	
//	public static void processMST(DEMManager dm, final boolean up, Point highest) {
//		Logging.log("processing MST");
//		_procMST(dm, up, highest, EdgeIterator.PHASE_MST, new Meta[] {new PromMeta(), new ThresholdMeta()});
//	    trimMST(highest, up);
//	}
//
//	public static void processRMST(DEMManager dm, final boolean up, Point highest) {
//		Logging.log("processing RMST");
//		_procMST(dm, up, highest, EdgeIterator.PHASE_RMST, new Meta[] {new PromMeta()});
//	}
//
//	static void _procMST(DEMManager dm, final boolean up, Point highest, int phase, Meta[] metas) {
//        Map<Prefix, Long> tally = initialTally(new EdgeIterator(up, phase).toIter());
//	    Set<Prefix> buckets = consolidateTally(tally);
//	    Logging.log(buckets.size() + " buckets");
//	    partition(phase, up, buckets);
//	    for (Meta m : metas) {
//	    	partitionMeta(phase, up, buckets, m);
//	    }
//	    cacheElevation(phase, up, buckets, dm);
//	}
//	
//	
//	public static void trimMST(Point highest, boolean up) {
//		Logging.log("trimming MST");
//		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta()});
//		highest = tn.get(highest.ix);
//
//		MSTFront front = new MSTFront(false);
//		Comparator<BasePoint> cmp = BasePoint.cmpElev(up);
//		MSTWriter w = new MSTWriter(up, EdgeIterator.PHASE_RMST);
//		
//		for (Point adj : tn.adjacent(highest)) {
//			front.add(adj, highest);
//		}
//		while (front.size() > 0) {
//			Point cur = front.next();
//			Point parent = front.parents.get(cur);
//			
//			MSTFront inner = new MSTFront(true);
//			inner.bt.add(parent, null);
//			inner.add(cur, parent);
//			
//			Set<Point> frontiers = new HashSet<Point>();
//			
//			while (inner.size() > 0) {
//				boolean noteworthyPoint = false;
//				Point cur2 = inner.next();
//				PromMeta pm = (PromMeta)tn.getMeta(cur2, "prom");
//				if (pm != null) {
//					// prominent point -- delegate back to main front
//					for (Point adj : tn.adjacent(cur2)) {
//						if (!PromNetwork.isUpstream(tn, cur2, adj)) {
//							front.add(adj, cur2);
//						}
//					}
//					noteworthyPoint = true;
//				} else {
//					for (Point adj : tn.adjacent(cur2)) {
//						inner.add(adj, cur2);
//					}				
//				}
//				if (tn.pendingSaddles.contains(cur2)) {
//					noteworthyPoint = true;
//				}
//				inner.doneWith(cur2);
//				
//				if (noteworthyPoint) {
//					frontiers.add(cur2);
//				}
//			}
//			front.doneWith(cur);
//
//			List<List<Point>> paths = new ArrayList<List<Point>>();
//			for (Point fr : frontiers) {
//				List<Point> path = Lists.newArrayList(inner.bt.trace(fr));
//				path = Lists.reverse(path);
//				if (tn.pendingSaddles.contains(fr)) {
//					path.add(null); // sentinel; treat like a peak
//				}
//				paths.add(path);
//			}
//			if (paths.isEmpty()) {
//				continue;
//			}
//			
//			List<List<Point>> splitPaths = new ArrayList<List<Point>>();
//			partitionPaths(paths, splitPaths);
//
//			List<Edge> edges = new ArrayList<Edge>();
//			for (List<Point> path : splitPaths) {
//				Point head = path.get(0);
//				Point tail = path.get(path.size() - 1);
//				boolean headIsSaddle = (head.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
//				boolean tailIsSaddle = (tail != null && (tail.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)));
//				long tailIx = (tail != null ? tail.ix : -1);
//				
//				if (headIsSaddle) {
//					if (tailIsSaddle) {
//						throw new RuntimeException("shouldn't happen");
//					} else {
//						edges.add(new Edge(head.ix, tailIx, 1));
//					}
//				} else {
//					if (tailIsSaddle) {
//						edges.add(new Edge(tailIx, head.ix, 0));						
//					} else {
//						// two peaks: one is a junction peak
//						Point junctionSaddle = null;
//						for (Point p : path) {
//							if (p == null) {
//								continue; // the sentinel
//							}
//							if (junctionSaddle == null || cmp.compare(p, junctionSaddle) < 0) {
//								junctionSaddle = p;
//							}
//						}
//						edges.add(new Edge(junctionSaddle.ix, head.ix, 0));
//						edges.add(new Edge(junctionSaddle.ix, tailIx, 1));
//					}
//				}
//			}
//			for (Edge e : edges) {
//				e.write(w.out);
//			}
//		}
//		w.finalize();
//	}
//	
//	static void partitionPaths(List<List<Point>> paths, List<List<Point>> split) {
//		if (paths.size() == 1) {
//			split.add(paths.get(0));
//			return;
//		}
//		
//		Map<Point, List<List<Point>>> p;
//		int i = 0;
//		while (true) {
//			p = new DefaultMap<Point, List<List<Point>>>() {
//				public List<List<Point>> defaultValue(Point key) {
//					return new ArrayList<List<Point>>();
//				}
//			};
//			
//			for (List<Point> path : paths) {
//				p.get(path.get(i)).add(path);
//			}
//			if (p.size() > 1) {
//				// divergence!
//				break;
//			}
//			i++;
//		}
//		
//		split.add(paths.get(0).subList(0, i));
//		for (List<List<Point>> subPaths : p.values()) {
//			List<List<Point>> _sps = new ArrayList<List<Point>>();
//			for (List<Point> sp : subPaths) {
//				_sps.add(sp.subList(i - 1, sp.size()));
//			}
//			partitionPaths(_sps, split);
//		}
//	}

	
}
