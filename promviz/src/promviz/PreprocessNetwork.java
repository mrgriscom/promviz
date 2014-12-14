package promviz;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.util.DefaultMap;
import promviz.util.Logging;

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
		
		public boolean pending() {
			return b == -1;
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
				
		public EdgeIterator(boolean up) {
			this(DEMManager.props.getProperty("dir_netdump") + "/" + (up ? "up" : "down"));
		}

		public EdgeIterator(boolean up, Prefix p) {
			this(segmentPath(up, p));
		}
		
		void readNext() {
			try {
				nextEdge = new Edge(in.readLong(), in.readLong(), in.readByte());
			} catch (EOFException eof) {
				nextEdge = null;
				try {
					in.close();
				} catch (IOException e) { }
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
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
		
	static void partition(boolean up, Set<Prefix> buckets) {
		final long MAX_EDGES_AT_ONCE = Long.parseLong(DEMManager.props.getProperty("memory")) / 32;
		for (Iterable<Edge> chunk : chunker(new EdgeIterator(up), MAX_EDGES_AT_ONCE)) {
			partitionChunk(up, buckets, chunk);
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

	static void writeEdge(Map<Prefix, DataOutputStream> f, Prefix p, Edge edge) {
		DataOutputStream out = f.get(p);
		try {
			out.writeLong(edge.a);
			out.writeLong(edge.b);
			out.writeByte(edge.i);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}	
	
	static String segmentPath(boolean up, Prefix p) {
		return prefixPath(up ? "up" : "down", p);
	}
	
	static String prefixPath(String mode, Prefix p) {
		int[] pp = PointIndex.split(p.prefix);
		return String.format("%s/%s-%d,%d,%d,%d", DEMManager.props.getProperty("dir_net"), mode, p.res, pp[0], pp[1], pp[2]);		
	}
	
	static void partitionChunk(boolean up, Set<Prefix> buckets, Iterable<Edge> edges) {
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
			
			writeEdge(f, bucket1, e);
			if (bucket2 != null && !bucket2.equals(bucket1)) {
				writeEdge(f, bucket2, e);
			}
		}
		
		for (Map.Entry<Prefix, ByteArrayOutputStream> e : _f.entrySet()) {
			Prefix p = e.getKey();
			ByteArrayOutputStream out = e.getValue();

			try {
				FileOutputStream fout = new FileOutputStream(segmentPath(up, p), true);
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
	
	static void cacheElevation(final boolean up, Set<Prefix> buckets, DEMManager dm) {
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
				for (Edge edge : new EdgeIterator(up, p).toIter()) {
					ixs.add(edge.a);
					if (!edge.pending() && p.isParent(edge.b)) {
						ixs.add(edge.b);
					}
				}

				DataOutputStream f;
				try {
					f = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(prefixPath("elev" + (up ? "up" : "down"), p), true)));
					
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
	
	public static void postprocessBucket(final Prefix p, boolean up) {
		Map<Long, List<Long>> connections = new DefaultMap<Long, List<Long>>() {
			public List<Long> defaultValue(Long key) {
				return new ArrayList<Long>();
			}
		};
		Set<Long> pending = new HashSet<Long>();
		
		for (Edge edge : new EdgeIterator(up, p).toIter()) {
			if (!p.isParent(edge.a)) {
				continue;
			}
			if (edge.pending()) {
				pending.add(edge.a);
			} else {
				connections.get(edge.a).add(edge.b);				
			}
		}
		for (Long pend : pending) {
			connections.get(pend).add(-1L);
		}

		for (Map.Entry<Long, List<Long>> e : connections.entrySet()) {
			long saddleIx = e.getKey();
			List<Long> peaks = e.getValue();
			Set<Long> uniqPeaks = new HashSet<Long>(peaks);
			
			if (uniqPeaks.size() == 1 && uniqPeaks.iterator().next() == -1) {
				// loner saddle: all leads go off edge; does not connect to rest of network
				System.err.println("XXLONER " + new BasePoint(saddleIx, 0));
				// remove all edges (saddleIx, -1)
				// no impact on other tiles
				continue;
			}
			if (peaks.size() > 2) {
				// multi-saddle: has more than two leads
				System.err.println("XXMULTI-SADDLE " + new BasePoint(saddleIx, 0) + " " + peaks.size() + " " + uniqPeaks.size());
				continue;
			}
			if (uniqPeaks.size() < 2) {
				// ring: both saddle's leads go to same point
				System.err.println("XXRING " + GeoCode.print(saddleIx));
				continue;
			}
		}
	}
	
	public static void preprocess(DEMManager dm, final boolean up) {
		Logging.log("preprocessing network");
		
        Map<Prefix, Long> tally = initialTally(new EdgeIterator(up).toIter());
	    Set<Prefix> buckets = consolidateTally(tally);
	    Logging.log(buckets.size() + " buckets");
	    partition(up, buckets);
	    
	    for (Prefix p : buckets) {
	    	postprocessBucket(p, up);
	    }
	    
	    cacheElevation(up, buckets, dm);
	}	
}
