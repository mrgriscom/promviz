package com.mrgris.prominence.legacy;

/*
 * simplified, more intuitive prominence algorithms for creating reference datasets
 * for validation of more complex algorithms. a consequence of this simplicity is
 * the entire area of interest must be loaded into memory at once, so max area that
 * can be processed is limited.
 * 
 * priorities are, in order: simplicitly/understandability; memory overhead; speed
 */

public class ProminenceRef {

	/*
	
	public static void promSearch(List<DEMFile> DEMs, boolean up, double cutoff) {
		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		new PromSearch(coverage, up, cutoff).search();
	}
	
	static class PromSearch {

		Map<Prefix, Set<DEMFile>> coverage;
		boolean up;
		double cutoff;

		Map<Point, List<NetworkEdge>> network;
		Map<Point, Point> prom;
		Set<Point> mst;
		
		public PromSearch(Map<Prefix, Set<DEMFile>> coverage, boolean up, double cutoff) {
			this.coverage = coverage;
			this.up = up;
			this.cutoff = cutoff;
			this.prom = new HashMap<Point, Point>();
		}

		void search() {
			load();
			
			searchProm();
			searchPThresh();
			searchParent();
			
			trimToMST();
			searchHeightSubsaddles();
			searchPromSubsaddles();		
		}
		
		class NetworkEdge {
			Point a;
			Point b;
			Point saddle;
			
			Point getOther(Point p) {
				assert a.equals(p) || (b != null ? b.equals(p) : p == null);
				return (a.equals(p) ? b : a);
			}
		}
		
		void load() {
			Set<Prefix> chunks = TopologyBuilder.enumerateChunks(coverage);
			network = new DefaultMap<Point, List<NetworkEdge>>() {
				public List<NetworkEdge> defaultValue(Point key) {
					return new ArrayList<NetworkEdge>(2);
				}
			};
			prom = new HashMap<Point, Point>();
			mst = new HashSet<Point>();
			Map<Long, Point> points = new HashMap<Long, Point>();
			
			for (Prefix prefix : chunks) {
				PagedElevGrid mesh = TopologyBuilder._createMesh(coverage);
				List<Edge> edges = Lists.newArrayList(FileUtil.loadEdges(up, prefix, FileUtil.PHASE_RAW));
				
				// load necessary DEM pages for graph
				Set<Prefix> pages = new HashSet<Prefix>();
				for (Edge edge : edges) {
					pages.add(PagedElevGrid.segmentPrefix(edge.a));
					pages.add(PagedElevGrid.segmentPrefix(edge.saddle));
					if (!edge.pending()) {
						pages.add(PagedElevGrid.segmentPrefix(edge.b));
					}
				}
				mesh.bulkLoadPage(pages);
			
				for (Edge edge : edges) {
					NetworkEdge ne = new NetworkEdge();
					ne.a = getPoint(points, mesh, edge.a);
					ne.saddle = getPoint(points, mesh, edge.saddle);
					ne.b = (edge.pending() ? null : getPoint(points, mesh, edge.b));
					
					network.get(ne.a).add(ne);
					if (ne.b != null) {
						network.get(ne.b).add(ne);
					}
				}
				Logging.log("loaded " + prefix + " (" + edges.size() + ")");
			}

			// remove duplicate edges
			for (Entry<Point, List<NetworkEdge>> e : network.entrySet()) {
				Map<Point, NetworkEdge> uniq = new HashMap<Point, NetworkEdge>();
				for (NetworkEdge ne : e.getValue()) {
					uniq.put(ne.saddle, ne);
				}
				network.put(e.getKey(), new ArrayList<NetworkEdge>(uniq.values()));
			}
		}

		Point getPoint(Map<Long, Point> points, PagedElevGrid mesh, long ix) {
			if (!points.containsKey(ix)) {
				points.put(ix, mesh.get(ix));
			}
			return points.get(ix);
		}
		
		void searchProm() {
			Logging.log("searching base prom");
			for (Point p : network.keySet()) {
				PromFact pf = _searchProm(p, new Criterion() {
					public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
						return cmp.compare(cur, p) > 0;
					}
				}, true);
				
				Point saddle = (pf instanceof PromBaseInfo ? ((PromBaseInfo)pf).saddle : ((PromPending)pf).pendingSaddle);
				if (Prominence.prominence(p, saddle) >= cutoff) {
					List<PromFact> pfs = new ArrayList<PromFact>();
					pfs.add(pf);
					Harness.outputPromInfo(up, p.ix, pfs);
					
					prom.put(p, saddle);
					prom.put(saddle, p);
				}
			}
		}

		void searchPThresh() {
			Logging.log("searching pthresh");
			for (Point p : network.keySet()) {
				if (!prom.containsKey(p)) {
					continue;
				}
				
				PromFact pf = _searchProm(p, new Criterion() {
					public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
						return cmp.compare(cur, p) > 0 && prom.containsKey(cur);
					}
				}, false);
				
				if (pf instanceof PromBaseInfo) {
					PromThresh pt = new PromThresh();
					pt.pthresh = ((PromBaseInfo)pf).thresh;
					
					List<PromFact> pfs = new ArrayList<PromFact>();
					pfs.add(pt);
					Harness.outputPromInfo(up, p.ix, pfs);
				}
			}
		}

		void searchParent() {
			Logging.log("searching parent");
			for (Point p : network.keySet()) {
				if (!prom.containsKey(p)) {
					continue;
				}

				final PromPair ppair = new PromPair(p, prom.get(p));
				PromFact pf = _searchProm(p, new Criterion() {
					public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
						return prom.containsKey(cur) && new PromPair(cur, prom.get(cur)).compareTo(ppair) > 0;
					}
				}, false);
				
				if (pf instanceof PromBaseInfo) {
					PromParent pp = new PromParent();
					pp.parent = ((PromBaseInfo)pf).thresh;
					pp.path = ((PromBaseInfo)pf).path;
					
					List<PromFact> pfs = new ArrayList<PromFact>();
					pfs.add(pp);
					Harness.outputPromInfo(up, p.ix, pfs);
				}
			}
		}
		
		static interface Criterion {
			boolean condition(Comparator<Point> cmp, Point p, Point cur);
		}
		
		PromFact _searchProm(Point p, Criterion crit, boolean trimPath) {
			final Comparator<Point> c = Point.cmpElev(up);
			PriorityQueue<NetworkEdge> front = new PriorityQueue<NetworkEdge>(10, new ReverseComparator<NetworkEdge>(
					new Comparator<NetworkEdge> () {
						public int compare(NetworkEdge a, NetworkEdge b) {
							return c.compare(a.saddle, b.saddle);
						}
					}));
			Backtrace bt = new Backtrace();
			
			front.addAll(network.get(p));
			bt.add(p, null);
			
			Point saddle = null;
			Point thresh = null;
			Point curPeak = null;
			Point curSaddle = null;
			NetworkEdge curEdge = null;
			while (true) {
				curEdge = front.poll();
				if (curEdge == null) {
					// global max
					PromBaseInfo pbi = new PromBaseInfo();
					pbi.p = p;
					return pbi;
				}
				
				if (bt.contains(curEdge.a) && curEdge.b != null && bt.contains(curEdge.b)) {
					continue;
				}
				
				curPeak = (bt.contains(curEdge.a) ? curEdge.b : curEdge.a);
				curSaddle = curEdge.saddle;
				Point prevPeak = curEdge.getOther(curPeak);

				if (saddle == null || c.compare(curSaddle, saddle) < 0) {
					saddle = curSaddle;
				}
				
				mst.add(curSaddle);
				bt.add(curSaddle, prevPeak);
				if (curPeak == null) {
					// reached edge of the world
					break;
				}
				bt.add(curPeak, curSaddle);
				
				if (crit.condition(c, p, curPeak)) {
					thresh = curPeak;
					break;
				}

				for (NetworkEdge adj : network.get(curPeak)) {
					Point newPeak = adj.getOther(curPeak);
					if (newPeak == null || !newPeak.equals(prevPeak)) {
						front.add(adj);
					}
				}
			}
			
			if (thresh != null) {
				PromBaseInfo pbi = new PromBaseInfo();
				pbi.p = p;
				pbi.saddle = saddle;
				pbi.thresh = thresh;
				pbi.path = new Path(bt.trace(thresh), trimPath ? p : null);
				return pbi;
			} else {
				PromPending pp = new PromPending();
				pp.p = p;
				pp.pendingSaddle = saddle;
				pp.path = new Path(bt.trace(curSaddle), null);
				return pp;
			}
		}
		
		void trimToMST() {
			for (Entry<Point, List<NetworkEdge>> e : network.entrySet()) {
				List<NetworkEdge> edges = new ArrayList<NetworkEdge>();
				for (NetworkEdge ne : e.getValue()) {
					if (mst.contains(ne.saddle)) {
						edges.add(ne);
					}
				}
				network.put(e.getKey(), edges);
			}
		}
		
		void searchHeightSubsaddles() {
			Logging.log("searching height subsaddles");
			for (Point p : network.keySet()) {
				if (!prom.containsKey(p)) {
					continue;
				}

				List<PromPair> ss = _subsaddles(p, new Criterion() {
					public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
						return prom.containsKey(cur) && cmp.compare(prom.get(cur), p) >= 0;
					}
				});

				List<PromFact> pfs = new ArrayList<PromFact>();
				for (PromPair s : ss) {
					PromSubsaddle ps = new PromSubsaddle();
					ps.subsaddle = s;
					ps.type = PromSubsaddle.TYPE_ELEV;
					pfs.add(ps);
				}
				Harness.outputPromInfo(up, p.ix, pfs);
			}
		}
		
		void searchPromSubsaddles() {
			Logging.log("searching prom subsaddles");
			for (Point p : network.keySet()) {
				if (!prom.containsKey(p)) {
					continue;
				}

				final PromPair ppair = new PromPair(p, prom.get(p));
				List<PromPair> ss = _subsaddles(p, new Criterion() {
					public boolean condition(Comparator<Point> cmp, Point p, Point cur) {
						// note gte
						return prom.containsKey(cur) && new PromPair(prom.get(cur), cur).compareTo(ppair) >= 0;
					}
				});

				List<PromFact> pfs = new ArrayList<PromFact>();
				for (PromPair s : ss) {
					PromSubsaddle ps = new PromSubsaddle();
					ps.subsaddle = s;
					ps.type = PromSubsaddle.TYPE_PROM;
					pfs.add(ps);
				}
				Harness.outputPromInfo(up, p.ix, pfs);
			}
		}
		
		List<PromPair> _subsaddles(Point p, Criterion crit) {
			Point saddle = prom.get(p);
			List<PromPair> ss = new ArrayList<PromPair>();
			Deque<NetworkEdge> front = new ArrayDeque<NetworkEdge>();
			Backtrace bt = new Backtrace();
			Comparator<Point> cmp = Point.cmpElev(up);
			
			front.addAll(network.get(p));
			bt.add(p, null);
			
			while (!front.isEmpty()) {
				NetworkEdge curEdge = front.removeFirst();

				Point curPeak = (bt.contains(curEdge.a) ? curEdge.b : curEdge.a);
				Point prevPeak = curEdge.getOther(curPeak);
				assert curPeak == null || !bt.contains(curPeak);

				if (saddle.equals(curEdge.saddle)) { 
					// terminate
				} else if (crit.condition(cmp, p, curEdge.saddle)) {
					ss.add(new PromPair(prom.get(curEdge.saddle), curEdge.saddle));
				} else if (curPeak != null) {
					bt.add(curPeak, prevPeak);
					for (NetworkEdge adj : network.get(curPeak)) {
						Point newPeak = adj.getOther(curPeak);
						if (newPeak == null || !newPeak.equals(prevPeak)) {
							front.addFirst(adj);
						}
					}
				}				
			}
			return ss;
		}
	}
	
	*/
}
