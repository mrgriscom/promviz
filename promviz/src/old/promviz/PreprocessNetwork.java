package old.promviz;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import old.promviz.PromNetwork.MSTFront;
import old.promviz.PromNetwork.MSTWriter;
import old.promviz.util.Util;
import promviz.PagedElevGrid;
import promviz.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;

import com.google.common.collect.Lists;

public class PreprocessNetwork {
	
	static final int BASE_RES = 5;
	static final int BUCKET_MAX_POINTS = 2048;
		
	static class EdgeIterator implements Iterator<Edge> {
		DataInputStream in;
		int count = 0;
		Edge nextEdge;
		
		public EdgeIterator(String path) {
			try {
				in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
				readNext();
			} catch (FileNotFoundException e) {
				throw new RuntimeException();
				//nextEdge = null;
			}
		}
				
		static final int PHASE_RAW = 0;
		static final int PHASE_MST = 1;
		static final int PHASE_RMST = 2;
		
		static String dir(int phase, boolean dump) {
			String _d = null;
			if (phase == PHASE_RAW) {
				_d = "dir_net";
			} else if (phase == PHASE_MST) {
				_d = "dir_mst";
			} else if (phase == PHASE_RMST) {
				_d = "dir_rmst";
			}
			if (dump) {
				_d += "dump";
			}
			return DEMManager.props.getProperty(_d);
		}
		
		public EdgeIterator(boolean up, int phase) {
			this(dir(phase, true) + "/" + (up ? "up" : "down"));
		}

		public EdgeIterator(boolean up, Prefix p, int phase) {
			this(segmentPath(up, p, phase));
		}
		
		void readNext() {
			nextEdge = Edge.read(in);
			if (nextEdge == null) {
				try {
					in.close();
				} catch (IOException e) { }
			}
			count += 1;
		}
		
		public boolean hasNext() {
			return nextEdge != null;
		}

		public Edge next() {
			Edge e = nextEdge;
			if (count % 1000000 == 0) {
				Logging.log(count + " read");
			}
			readNext();
			return e;
		}

		public void remove() {
			throw new UnsupportedOperationException();			
		}
		
		public Iterable<Edge> toIter() {
			return new Iterable<Edge>() {
				public Iterator<Edge> iterator() {
					return EdgeIterator.this;
				}
			};
		}
	}

	static Map<Prefix, Long> initialTally(Iterable<Edge> edges) {
		Map<Prefix, Long> buckets = new DefaultMap<Prefix, Long>() {
			public Long defaultValue(Prefix _) {
				return 0L;
			}
		};
		for (Edge e : edges) {
			Prefix pf1 = new Prefix(e.a, BASE_RES);
			buckets.put(pf1, buckets.get(pf1) + 1);
			
			if (!e.pending()) {
				Prefix pf2 = new Prefix(e.b, BASE_RES);
				if (!pf2.equals(pf1)) {
					buckets.put(pf2, buckets.get(pf2) + 1);
				}
			}
		}
		return buckets;
	}
	
	static Set<Prefix> consolidateTally(Map<Prefix, Long> baseTally) {
		Map<Integer, Map<Prefix, Long>> tallies = new HashMap<Integer, Map<Prefix, Long>>();
		tallies.put(BASE_RES, baseTally);
		
		int res = BASE_RES;
		while (true) {
			Map<Prefix, Long> curLevel = tallies.get(res);
			Map<Prefix, Long> nextLevel = new DefaultMap<Prefix, Long>() {
				public Long defaultValue(Prefix _) {
					return 0L;
				}
			};
			for (Map.Entry<Prefix, Long> e : curLevel.entrySet()) {
				Prefix parent = new Prefix(e.getKey(), res + 1);
				nextLevel.put(parent, nextLevel.get(parent) + e.getValue());
			}
			if (nextLevel.size() == curLevel.size()) {
				break;
			}
			
			res += 1;
			tallies.put(res, nextLevel);
		}
		
		Set<Prefix> buckets = new HashSet<Prefix>();
		for (Prefix p : tallies.get(res).keySet()) {
			consolidateDescend(p, tallies, buckets);
		}
		return buckets;
	}
	
	static void consolidateDescend(Prefix p, Map<Integer, Map<Prefix, Long>> tallies, Set<Prefix> buckets) {
		if (tallies.get(p.res).get(p) <= BUCKET_MAX_POINTS || p.res == BASE_RES) {
			buckets.add(p);
		} else {
			for (Prefix child : p.children(-1)) {
				if (tallies.get(child.res).containsKey(child)) {
					consolidateDescend(child, tallies, buckets);
				}
			}
		}
	}
		
	static void partition(int phase, boolean up, Set<Prefix> buckets) {
		final long MAX_EDGES_AT_ONCE = Long.parseLong(DEMManager.props.getProperty("memory")) / 32;
		for (Iterable<Edge> chunk : chunker(new EdgeIterator(up, phase), MAX_EDGES_AT_ONCE)) {
			partitionChunk(phase, up, buckets, chunk);
		}
	}
	
	static Prefix matchPrefix(long ix, Set<Prefix> buckets) {
		for (int res = BASE_RES; res < 24; res++) {
			Prefix p = new Prefix(ix, res);
			if (buckets.contains(p)) {
				return p;
			}
		}
		return null;
	}

	static String segmentPath(boolean up, Prefix p, int phase) {
		return prefixPath(up, null, p, phase);
	}
	
	static String prefixPath(boolean up, String mode, Prefix p, int phase) {
		int[] pp = PointIndex.split(p.prefix);
		String dir = EdgeIterator.dir(phase, false);
		return String.format("%s/%s%s-%d,%d,%d,%d", dir, mode != null ? mode : "", up ? "up" : "down", p.res, pp[0], pp[1], pp[2]);		
	}
	
	static void partitionChunk(int phase, boolean up, Set<Prefix> buckets, Iterable<Edge> edges) {
		final Map<Prefix, ByteArrayOutputStream> _f = new HashMap<Prefix, ByteArrayOutputStream>();
		Map<Prefix, DataOutputStream> f = new DefaultMap<Prefix, DataOutputStream>() {
			public DataOutputStream defaultValue(Prefix key) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				_f.put(key, baos);
				return new DataOutputStream(baos);
			}
		};

		for (Edge e : edges) {
			Prefix bucket1 = matchPrefix(e.a, buckets);
			Prefix bucket2 = (e.pending() ? null : matchPrefix(e.b, buckets));
			
			e.write(f.get(bucket1));
			if (bucket2 != null && !bucket2.equals(bucket1)) {
				e.write(f.get(bucket2));
			}
		}
		
		for (Map.Entry<Prefix, ByteArrayOutputStream> e : _f.entrySet()) {
			Prefix p = e.getKey();
			ByteArrayOutputStream out = e.getValue();

			try {
				FileOutputStream fout = new FileOutputStream(segmentPath(up, p, phase), true);
				fout.write(out.toByteArray());
				fout.close();			
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}
	
	static <E> Iterable<Iterable<E>> chunker(final Iterator<E> stream, final long size) {
		return new Iterable<Iterable<E>>() {
			public Iterator<Iterable<E>> iterator() {
				return new Iterator<Iterable<E>>() {
					public boolean hasNext() {
						return stream.hasNext();
					}

					public Iterable<E> next() {
						return new Iterable<E>() {
							public Iterator<E> iterator() {
								return new Iterator<E>() {
									long count = 0;
									
									public boolean hasNext() {
										return stream.hasNext() && count < size;
									}

									public E next() {
										count += 1;
										return stream.next();
									}

									public void remove() {
										throw new UnsupportedOperationException();
									}
								};
							}
						};
					}

					public void remove() {
						throw new UnsupportedOperationException();
					}				
				};
			}
		};
	}
	
	static void cacheElevation(int phase, final boolean up, Set<Prefix> buckets, DEMManager dm) {
		PagedElevGrid m = new PagedElevGrid(dm.partitionDEM(), dm.MESH_MAX_POINTS); // can reuse this for up and down phases
		Map<Prefix, Set<Prefix>> segmentMap = new DefaultMap<Prefix, Set<Prefix>>() {
			public Set<Prefix> defaultValue(Prefix key) {
				return new HashSet<Prefix>();
			}
		};
		for (Prefix p : buckets) {
			if (p.res <= dm.GRID_TILE_SIZE) {
				segmentMap.get(new Prefix(p, dm.GRID_TILE_SIZE)).add(p);
			} else {
				for (Prefix child : p.children(p.res - dm.GRID_TILE_SIZE)) {
					segmentMap.get(child).add(p);
				}
			}
		}
		
		for (Map.Entry<Prefix, Set<Prefix>> e : segmentMap.entrySet()) {
			Prefix grid = e.getKey();
			Set<Prefix> pfs = e.getValue();
			
			m.loadPage(grid);
			for (final Prefix p : pfs) {
				Set<Long> ixs = new HashSet<Long>();
				for (Edge edge : new EdgeIterator(up, p, phase).toIter()) {
					ixs.add(edge.a);
					if (!edge.pending() && p.isParent(edge.b)) {
						ixs.add(edge.b);
					}
				}

				DataOutputStream f;
				try {
					f = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(prefixPath(up, "elev", p, phase), true)));
					
					for (long ix : ixs) {
						if (grid.isParent(ix)) {
							f.writeLong(ix);
							f.writeFloat(m.get(ix).elev);
						}
					}

					f.close();					
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}		
			}
		}
	}
	
	public static void postprocessTopology(int phase, boolean up, Set<Prefix> buckets) {
		Map<Prefix, List<Edge>> postToAdd = new DefaultMap<Prefix, List<Edge>>() {
			public List<Edge> defaultValue(Prefix key) {
				return new ArrayList<Edge>();
			}			
		};
		Map<Prefix, Set<Edge>> postToRemove = new DefaultMap<Prefix, Set<Edge>>() {
			public Set<Edge> defaultValue(Prefix key) {
				return new HashSet<Edge>();
			}
		};
		
		Logging.log("correcting topology");
		
		int i = 0;
	    for (Prefix p : buckets) {
	    	postprocessBucket(buckets, p, up, phase, postToAdd, postToRemove);
	    	if (++i % 1000 == 0) {
	    		Logging.log(i + " buckets");
	    	}
	    }

	    // careful-- this is memory-unbounded
	    i = 0;
	    for (Prefix p : buckets) {
			filterBucket(buckets, p, up, phase, postToAdd.get(p), postToRemove.get(p), null, null);
	    	if (++i % 1000 == 0) {
	    		Logging.log(i + " buckets");
	    	}
	    }
	}
	
	public static void postprocessBucket(Set<Prefix> buckets, final Prefix p, boolean up, int phase,
				Map<Prefix, List<Edge>> postToAdd,
				Map<Prefix, Set<Edge>> postToRemove) {
		Map<Long, List<Edge>> bySaddle = new DefaultMap<Long, List<Edge>>() {
			public List<Edge> defaultValue(Long key) {
				return new ArrayList<Edge>();
			}
		};
		Map<Long, Edge> pending = new HashMap<Long, Edge>();
		
		for (Edge edge : new EdgeIterator(up, p, phase).toIter()) {
			if (!p.isParent(edge.a)) {
				continue;
			}
			if (edge.pending()) {
				pending.put(edge.a, edge);
			} else {
				bySaddle.get(edge.a).add(edge);
			}
		}
		for (Map.Entry<Long, Edge> e : pending.entrySet()) {
			bySaddle.get(e.getKey()).add(e.getValue());
		}

		Set<Edge> toRemove = new HashSet<Edge>();
		List<Edge> toAdd = new ArrayList<Edge>();
		
		for (Map.Entry<Long, List<Edge>> e : bySaddle.entrySet()) {
			long saddleIx = e.getKey();
			List<Edge> edges = e.getValue();
			Set<Long> uniqPeaks = new HashSet<Long>();
			for (Edge _e : edges) {
				uniqPeaks.add(_e.b);
			}
			
			if (uniqPeaks.size() == 1 && edges.get(0).pending()) {
				// loner saddle: all leads go off edge; does not connect to rest of network

				toRemove.add(new Edge(saddleIx, -1L));
			} else if (edges.size() > 2) {
				// multi-saddle: has more than two leads

				Collections.sort(edges, new Comparator<Edge>() {
					public int compare(Edge e, Edge f) {
						return Integer.compare(e.i, f.i);
					}
				});

				long vPeak = PointIndex.clone(saddleIx, 1);
				for (int i = 0; i < edges.size(); i++) {
					Edge edge = edges.get(i);
					toRemove.add(new Edge(saddleIx, edge.b));
					long vSaddle = PointIndex.clone(saddleIx, -i); // would be nice to randomize the saddle ordering
					
					// the disambiguation pattern is not symmetrical between the 'up' and 'down' networks;
					// this is the cost of making them consistent with each other
					if (up) {
						toAdd.add(new Edge(vSaddle, edge.b, edge.i));
						toAdd.add(new Edge(vSaddle, vPeak));
					} else {
						Edge prev_edge = edges.get(Util.mod(i - 1, edges.size()));
						if (edge.b != prev_edge.b) {
							toAdd.add(new Edge(vSaddle, edge.b, edge.i));
							toAdd.add(new Edge(vSaddle, prev_edge.b, prev_edge.i));
						} else {
							// would create a ring
							deRing(false, toAdd, vSaddle, edge, prev_edge);
						}
					}
				}
			} else if (uniqPeaks.size() < 2) {
				// ring: both saddle's leads go to same point

				long peak = deRing(up, toAdd, saddleIx, edges.get(0), edges.get(1));
				toRemove.add(new Edge(saddleIx, peak));
			}
		}
		
		filterBucket(buckets, p, up, phase, toAdd, toRemove, postToAdd, postToRemove);
	}
	
	static void filterBucket(Set<Prefix> buckets, Prefix p, boolean up, int phase, List<Edge> toAdd, Set<Edge> toRemove,
			Map<Prefix, List<Edge>> postToAdd,
			Map<Prefix, Set<Edge>> postToRemove) {
		if (toAdd.size() == 0 && toRemove.size() == 0) {
			return;
		}
		
		List<Edge> edges = new ArrayList<Edge>();
		for (Edge e : new EdgeIterator(up, p, phase).toIter()) {
			if (!toRemove.contains(e)) {
				edges.add(e);
			}
		}
		edges.addAll(toAdd);

		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream(segmentPath(up, p, phase), false));
			for (Edge e : edges) {
				e.write(out);
			}
			out.close();			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
		saveForLater(buckets, p, toAdd, postToAdd);
		saveForLater(buckets, p, toRemove, postToRemove);
	}

	static void saveForLater(Set<Prefix> buckets, Prefix p, Collection<Edge> c, Map<Prefix, ? extends Collection<Edge>> later) {
		if (later == null) {
			return;
		}
		
		for (Edge e : c) {
			Prefix other = null;
			if (!p.isParent(e.a)) {
				other = matchPrefix(e.a, buckets);
			} else if (!e.pending() && !p.isParent(e.b)) {
				other = matchPrefix(e.b, buckets);
			}
			if (other != null) {
				later.get(other).add(e);
			}
		}
	}
	
	static long deRing(boolean up, List<Edge> toAdd, long saddle, Edge edge1, Edge edge2) {
		if (edge1.b != edge2.b) {
			throw new IllegalArgumentException();
		}
		
		long dst = edge1.b;
		long altDst = PointIndex.clone(dst, up ? 1 : -1);
		long altSaddle = PointIndex.clone(dst, up ? -1 : 1);
		toAdd.add(new Edge(saddle, dst, edge1.i));
		toAdd.add(new Edge(saddle, altDst, edge2.i));
		toAdd.add(new Edge(altSaddle, dst));
		toAdd.add(new Edge(altSaddle, altDst));
		return dst;
	}
	
	public static void preprocess(DEMManager dm, final boolean up) {
		Logging.log("preprocessing network");
		int phase = EdgeIterator.PHASE_RAW;
		
        Map<Prefix, Long> tally = initialTally(new EdgeIterator(up, phase).toIter());
	    Set<Prefix> buckets = consolidateTally(tally);
	    Logging.log(buckets.size() + " buckets");
	    partition(phase, up, buckets);
	    postprocessTopology(phase, up, buckets);
	    cacheElevation(phase, up, buckets, dm);
	}
	
	
	public static void processMST(DEMManager dm, final boolean up, Point highest) {
		Logging.log("processing MST");
		_procMST(dm, up, highest, EdgeIterator.PHASE_MST, new Meta[] {new PromMeta(), new ThresholdMeta()});
	    trimMST(highest, up);
	}

	public static void processRMST(DEMManager dm, final boolean up, Point highest) {
		Logging.log("processing RMST");
		_procMST(dm, up, highest, EdgeIterator.PHASE_RMST, new Meta[] {new PromMeta()});
	}

	static void _procMST(DEMManager dm, final boolean up, Point highest, int phase, Meta[] metas) {
        Map<Prefix, Long> tally = initialTally(new EdgeIterator(up, phase).toIter());
	    Set<Prefix> buckets = consolidateTally(tally);
	    Logging.log(buckets.size() + " buckets");
	    partition(phase, up, buckets);
	    for (Meta m : metas) {
	    	partitionMeta(phase, up, buckets, m);
	    }
	    cacheElevation(phase, up, buckets, dm);
	}
	
	
	public static void trimMST(Point highest, boolean up) {
		Logging.log("trimming MST");
		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta()});
		highest = tn.get(highest.ix);

		MSTFront front = new MSTFront(false);
		Comparator<BasePoint> cmp = BasePoint.cmpElev(up);
		MSTWriter w = new MSTWriter(up, EdgeIterator.PHASE_RMST);
		
		for (Point adj : tn.adjacent(highest)) {
			front.add(adj, highest);
		}
		while (front.size() > 0) {
			Point cur = front.next();
			Point parent = front.parents.get(cur);
			
			MSTFront inner = new MSTFront(true);
			inner.bt.add(parent, null);
			inner.add(cur, parent);
			
			Set<Point> frontiers = new HashSet<Point>();
			
			while (inner.size() > 0) {
				boolean noteworthyPoint = false;
				Point cur2 = inner.next();
				PromMeta pm = (PromMeta)tn.getMeta(cur2, "prom");
				if (pm != null) {
					// prominent point -- delegate back to main front
					for (Point adj : tn.adjacent(cur2)) {
						if (!PromNetwork.isUpstream(tn, cur2, adj)) {
							front.add(adj, cur2);
						}
					}
					noteworthyPoint = true;
				} else {
					for (Point adj : tn.adjacent(cur2)) {
						inner.add(adj, cur2);
					}				
				}
				if (tn.pendingSaddles.contains(cur2)) {
					noteworthyPoint = true;
				}
				inner.doneWith(cur2);
				
				if (noteworthyPoint) {
					frontiers.add(cur2);
				}
			}
			front.doneWith(cur);

			List<List<Point>> paths = new ArrayList<List<Point>>();
			for (Point fr : frontiers) {
				List<Point> path = Lists.newArrayList(inner.bt.trace(fr));
				path = Lists.reverse(path);
				if (tn.pendingSaddles.contains(fr)) {
					path.add(null); // sentinel; treat like a peak
				}
				paths.add(path);
			}
			if (paths.isEmpty()) {
				continue;
			}
			
			List<List<Point>> splitPaths = new ArrayList<List<Point>>();
			partitionPaths(paths, splitPaths);

			List<Edge> edges = new ArrayList<Edge>();
			for (List<Point> path : splitPaths) {
				Point head = path.get(0);
				Point tail = path.get(path.size() - 1);
				boolean headIsSaddle = (head.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT));
				boolean tailIsSaddle = (tail != null && (tail.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)));
				long tailIx = (tail != null ? tail.ix : -1);
				
				if (headIsSaddle) {
					if (tailIsSaddle) {
						throw new RuntimeException("shouldn't happen");
					} else {
						edges.add(new Edge(head.ix, tailIx, 1));
					}
				} else {
					if (tailIsSaddle) {
						edges.add(new Edge(tailIx, head.ix, 0));						
					} else {
						// two peaks: one is a junction peak
						Point junctionSaddle = null;
						for (Point p : path) {
							if (p == null) {
								continue; // the sentinel
							}
							if (junctionSaddle == null || cmp.compare(p, junctionSaddle) < 0) {
								junctionSaddle = p;
							}
						}
						edges.add(new Edge(junctionSaddle.ix, head.ix, 0));
						edges.add(new Edge(junctionSaddle.ix, tailIx, 1));
					}
				}
			}
			for (Edge e : edges) {
				e.write(w.out);
			}
		}
		w.finalize();
	}
	
	static void partitionPaths(List<List<Point>> paths, List<List<Point>> split) {
		if (paths.size() == 1) {
			split.add(paths.get(0));
			return;
		}
		
		Map<Point, List<List<Point>>> p;
		int i = 0;
		while (true) {
			p = new DefaultMap<Point, List<List<Point>>>() {
				public List<List<Point>> defaultValue(Point key) {
					return new ArrayList<List<Point>>();
				}
			};
			
			for (List<Point> path : paths) {
				p.get(path.get(i)).add(path);
			}
			if (p.size() > 1) {
				// divergence!
				break;
			}
			i++;
		}
		
		split.add(paths.get(0).subList(0, i));
		for (List<List<Point>> subPaths : p.values()) {
			List<List<Point>> _sps = new ArrayList<List<Point>>();
			for (List<Point> sp : subPaths) {
				_sps.add(sp.subList(i - 1, sp.size()));
			}
			partitionPaths(_sps, split);
		}
	}
	
	static class Meta {
		long ix;
		
		public Meta() {}
		
		public Meta(long ix) {
			this.ix = ix;
		}
		
		void read(DataInputStream in) throws EOFException {
			try {
				this.ix = in.readLong();
				readData(in);
			} catch (EOFException eof) {
				throw eof;
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
		}
		void readData(DataInputStream in) throws IOException {}

		void write(DataOutputStream out) {
			try {
				out.writeLong(this.ix);
				writeData(out);
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
		}
		void writeData(DataOutputStream out) throws IOException {}

		int recSize() {
			return 8 + dataSize();
		}
		int dataSize() { return 0; }
		String getName() { return null; }
		
		String bulkPath(boolean up) { // bulk meta is always located in the 'mst' directory
			return EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/" + getName() + "-" + (up ? "up" : "down");
		}
		String chunkPath(int phase, boolean up, Prefix p) {
			return prefixPath(up, getName(), p, phase);
		}
		Meta fuckingHell() { return null; }
		
		class MetaIterator implements Iterator<Meta> {
			DataInputStream in;
			int count = 0;
			Meta nextMeta;
			
			public MetaIterator(String path) {
				try {
					in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
					readNext();
				} catch (FileNotFoundException e) {
					//throw new RuntimeException();
					nextMeta = null;
				}
			}
			
			public MetaIterator(boolean up) {
				this(bulkPath(up));
			}

			public MetaIterator(int phase, boolean up, Prefix p) {
				this(chunkPath(phase, up, p));
			}
			
			void readNext() {
				try {
					nextMeta = fuckingHell();
					nextMeta.read(in);
				} catch (EOFException eof) {
					nextMeta = null;
					try {
						in.close();
					} catch (IOException e) { }
				}
				count += 1;
			}
			
			public boolean hasNext() {
				return nextMeta != null;
			}

			public Meta next() {
				Meta m = nextMeta;
				if (count % 1000000 == 0) {
					Logging.log(count + " read (" + getName() + ")");
				}
				readNext();
				return m;
			}

			public void remove() {
				throw new UnsupportedOperationException();			
			}
			
			public Iterable<Meta> toIter() {
				return new Iterable<Meta>() {
					public Iterator<Meta> iterator() {
						return MetaIterator.this;
					}
				};
			}
		}
		MetaIterator iterator(boolean up) {
			return new MetaIterator(up);
		}
		MetaIterator iterator(int phase, boolean up, Prefix p) {
			return new MetaIterator(phase, up, p);
		}
	}
	
	static class PromMeta extends Meta {
		float prom;
		private long otherIx;
		boolean forward;
		
		public PromMeta() {}
		
		public PromMeta(long ix, float prom, long otherIx, boolean forward) {
			super(ix);
			this.prom = prom;
			this.otherIx = otherIx;
			this.forward = forward;
		}
		
		void readData(DataInputStream in) throws IOException {
			this.prom = in.readFloat();
			this.otherIx = in.readLong();
			this.forward = in.readBoolean();
		}
		void writeData(DataOutputStream out) throws IOException {
			out.writeFloat(this.prom);	
			out.writeLong(this.otherIx);
			out.writeBoolean(this.forward);		
		}
		
		long getPeak(boolean isSaddle) {
			if (isSaddle) {
				return otherIx;
			} else {
				throw new IllegalArgumentException();
			}
		}
		
		long getSaddle(boolean isSaddle) {
			if (isSaddle) {
				throw new IllegalArgumentException();
			} else {
				return otherIx;
			}			
		}
		
		String getName() {
			return "prom";
		}
		
		int dataSize() {
			return 13;
		}		
		
		Meta fuckingHell() { return new PromMeta(); }
	}

	static class ThresholdMeta extends Meta {
				
		public ThresholdMeta() {}
		
		public ThresholdMeta(long ix) {
			super(ix);
		}
		
		String getName() {
			return "thresh";
		}
		
		int dataSize() {
			return 0;
		}		
		
		Meta fuckingHell() { return new ThresholdMeta(); }
	}

	
	public static void partitionMeta(int phase, boolean up, Set<Prefix> buckets, Meta spec) {
		Logging.log("--" + spec.getName() + "--");
		final long MAX_UNITS_AT_ONCE = Long.parseLong(DEMManager.props.getProperty("memory")) / (3 * spec.recSize());
		for (Iterable<Meta> chunk : chunker(spec.iterator(up), MAX_UNITS_AT_ONCE)) {
			partitionMetaChunk(phase, up, buckets, chunk, spec);
		}
	}
	
	static void partitionMetaChunk(int phase, boolean up, Set<Prefix> buckets, Iterable<Meta> metas, Meta spec) {
		final Map<Prefix, ByteArrayOutputStream> _f = new HashMap<Prefix, ByteArrayOutputStream>();
		Map<Prefix, DataOutputStream> f = new DefaultMap<Prefix, DataOutputStream>() {
			public DataOutputStream defaultValue(Prefix key) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				_f.put(key, baos);
				return new DataOutputStream(baos);
			}
		};

		for (Meta m : metas) {
			Prefix bucket = matchPrefix(m.ix, buckets);
			m.write(f.get(bucket));
		}
		
		for (Map.Entry<Prefix, ByteArrayOutputStream> e : _f.entrySet()) {
			Prefix p = e.getKey();
			ByteArrayOutputStream out = e.getValue();

			try {
				FileOutputStream fout = new FileOutputStream(spec.chunkPath(phase, up, p), true);
				fout.write(out.toByteArray());
				fout.close();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		
	}
}
