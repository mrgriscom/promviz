package promviz;

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
import java.util.Set;

import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.Util;

public class PreprocessNetwork {
	
	static final int BASE_RES = 5;
	static final int BUCKET_MAX_POINTS = 2048;
	
	static class Edge {
		long a;
		long b;
		int i;
		
		public Edge(long a, long b, int i) {
			this.a = a;
			this.b = b;
			this.i = i;
		}
		
		public Edge(long a, long b) {
			this(a, b, -1);
		}
		
		public boolean pending() {
			return b == -1;
		}

		void write(DataOutputStream out) {
			try {
				out.writeLong(this.a);
				out.writeLong(this.b);
				out.writeByte(this.i);
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
		
		static Edge read(DataInputStream in) {
			try {
				return new Edge(in.readLong(), in.readLong(), in.readByte());
			} catch (EOFException eof) {
				return null;
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
		
		public boolean equals(Object o) {
			if (o instanceof Edge) {
				Edge e = (Edge)o;
				return this.a == e.a && this.b == e.b;
			} else {
				return false;
			}
		}
		
		public int hashCode() {
			return Long.valueOf(this.a).hashCode() | Long.valueOf(this.b).hashCode();
		}
		
		public String toString() {
			return String.format("%s => %s (%d)", PointIndex.print(a), PointIndex.print(b), i);
		}
	}
	
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
		
		static String fuckJava(boolean up, int phase) {
			if (phase == PHASE_RAW) {
				return DEMManager.props.getProperty("dir_netdump") + "/" + (up ? "up" : "down");
			} else if (phase == PHASE_MST) {
				return DEMManager.props.getProperty("dir_mstdump") + "/" + (up ? "up" : "down");				
			}
			return null;
		}
		
		public EdgeIterator(boolean up, int phase) {
			this(fuckJava(up, phase));
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
		String dir = null;
		if (phase == EdgeIterator.PHASE_RAW) {
			dir = "dir_net";
		} else if (phase == EdgeIterator.PHASE_MST) {
			dir = "dir_mst";			
		}
		return String.format("%s/%s%s-%d,%d,%d,%d", DEMManager.props.getProperty(dir), mode != null ? mode : "", up ? "up" : "down", p.res, pp[0], pp[1], pp[2]);		
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
		PagedMesh m = new PagedMesh(dm.partitionDEM(), dm.MESH_MAX_POINTS); // can reuse this for up and down phases
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
		int phase = EdgeIterator.PHASE_MST;
		
        Map<Prefix, Long> tally = initialTally(new EdgeIterator(up, phase).toIter());
	    Set<Prefix> buckets = consolidateTally(tally);
	    Logging.log(buckets.size() + " buckets");
	    partition(phase, up, buckets);
	    partitionMeta(up, buckets, new PromMeta());
	    cacheElevation(phase, up, buckets, dm);
	    
	    //trimMST(highest, up);

	    partitionMeta(up, buckets, new SaddleMeta());
	}
	
	public static void trimMST(Point highest, boolean up) {
		Logging.log("trimming MST");
		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta()});
		System.err.println(tn.get(highest.ix));

		Deque<Point> q = new ArrayDeque<Point>();
		Set<Point> seen = new HashSet<Point>();
		q.add(highest);
		
		while (!q.isEmpty()) {
			Point cur = q.removeFirst();
			seen.add(cur);
			for (Point adj : tn.adjacent(cur)) {
				if (!seen.contains(adj)) {
					q.addLast(adj);
				}
			}
			
			PromMeta m = (PromMeta)tn.getMeta(cur, "prom");
			if (m != null) {
				System.err.println("ppt! " + cur + " # " + m.prom);
			}
		}
		
		/*
		 * from a prom pt, explore all branches until termed at other prom pts
		 * add term pts to queue
		 * consolidate branches and write out to stream
		 * 
		 * 
		 * special handling for pend
		 * 
		 */
	}
	
	static class Meta {
		long ix;
		
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
		
		String bulkPath(boolean up) {
			return DEMManager.props.getProperty("dir_mstdump") + "/" + getName() + "-" + (up ? "up" : "down");
		}
		String chunkPath(boolean up, Prefix p) {
			return prefixPath(up, getName(), p, EdgeIterator.PHASE_MST);
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

			public MetaIterator(boolean up, Prefix p) {
				this(chunkPath(up, p));
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
		MetaIterator iterator(boolean up, Prefix p) {
			return new MetaIterator(up, p);
		}
	}
	
	static class PromMeta extends Meta {
		float prom;
		
		void readData(DataInputStream in) throws IOException {
			this.prom = in.readFloat();
		}
		void writeData(DataOutputStream out) throws IOException {
			out.writeFloat(this.prom);		
		}
		
		String getName() {
			return "prom";
		}
		
		int dataSize() {
			return 4;
		}		
		
		Meta fuckingHell() { return new PromMeta(); }
	}
	
	static class SaddleMeta extends Meta {
		long peakIx;
		boolean forward;
		
		void readData(DataInputStream in) throws IOException {
			this.peakIx = in.readLong();
			this.forward = in.readBoolean();
		}
		void writeData(DataOutputStream out) throws IOException {
			out.writeLong(this.peakIx);		
			out.writeBoolean(this.forward);		
		}

		String getName() {
			return "saddle";
		}
		
		int dataSize() {
			return 9;
		}

		Meta fuckingHell() { return new SaddleMeta(); }
	}
	
	public static void partitionMeta(boolean up, Set<Prefix> buckets, Meta spec) {
		Logging.log("--" + spec.getName() + "--");
		final long MAX_UNITS_AT_ONCE = Long.parseLong(DEMManager.props.getProperty("memory")) / (3 * spec.recSize());
		for (Iterable<Meta> chunk : chunker(spec.iterator(up), MAX_UNITS_AT_ONCE)) {
			partitionMetaChunk(up, buckets, chunk, spec);
		}
	}
	
	static void partitionMetaChunk(boolean up, Set<Prefix> buckets, Iterable<Meta> metas, Meta spec) {
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
				FileOutputStream fout = new FileOutputStream(spec.chunkPath(up, p), true);
				fout.write(out.toByteArray());
				fout.close();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		
	}
}
