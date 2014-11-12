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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;

public class PreprocessNetwork {
	
	static final int BASE_RES = 5;
	static final int BUCKET_MAX_POINTS = 2048;
	
	static class EdgeIterator implements Iterator<long[]> {
		DataInputStream in;
		int count = 0;
		long[] nextEdge;
		
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
				nextEdge = new long[] {in.readLong(), in.readLong()};
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

		public long[] next() {
			long[] e = nextEdge;
			if (count % 1000000 == 0) {
				Logging.log(count + " read");
			}
			readNext();
			return e;
		}

		public void remove() {
			throw new UnsupportedOperationException();			
		}		
	}

	static Map<Prefix, Long> initialTally(Iterable<long[]> edges) {
		Map<Prefix, Long> buckets = new DefaultMap<Prefix, Long>() {
			public Long defaultValue(Prefix _) {
				return 0L;
			}
		};
		for (long[] e : edges) {
			Prefix pf1 = new Prefix(e[0], BASE_RES);
			buckets.put(pf1, buckets.get(pf1) + 1);
			
			if (e[1] != -1) {
				Prefix pf2 = new Prefix(e[1], BASE_RES);
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
		for (Iterable<long[]> chunk : chunker(new EdgeIterator(up), MAX_EDGES_AT_ONCE)) {
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

	static void writeEdge(Map<Prefix, DataOutputStream> f, Prefix p, long[] edge) {
		DataOutputStream out = f.get(p);
		try {
			out.writeLong(edge[0]);
			out.writeLong(edge[1]);
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
	
	static void partitionChunk(boolean up, Set<Prefix> buckets, Iterable<long[]> edges) {
		final Map<Prefix, ByteArrayOutputStream> _f = new HashMap<Prefix, ByteArrayOutputStream>();
		Map<Prefix, DataOutputStream> f = new DefaultMap<Prefix, DataOutputStream>() {
			public DataOutputStream defaultValue(Prefix key) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				_f.put(key, baos);
				return new DataOutputStream(baos);
			}
		};

		for (long[] e : edges) {
			Prefix bucket1 = matchPrefix(e[0], buckets);
			Prefix bucket2 = (e[1] == -1 ? null : matchPrefix(e[1], buckets));
			
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
				for (long[] edge : new Iterable<long[]>() {
					public Iterator<long[]> iterator() {
						return new EdgeIterator(up, p);
					}
				}) {
					ixs.add(edge[0]);
					if (edge[1] != -1 && p.isParent(edge[1])) {
						ixs.add(edge[1]);
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
	
	public static void preprocess(DEMManager dm, final boolean up) {
		Logging.log("preprocessing network");
		
        Map<Prefix, Long> tally = initialTally(new Iterable<long[]>() {
			public Iterator<long[]> iterator() {
				return new EdgeIterator(up);
			}
	    });
	    Set<Prefix> buckets = consolidateTally(tally);
	    Logging.log(buckets.size() + " buckets");
	    partition(up, buckets);
	    
	    cacheElevation(up, buckets, dm);
	}	
}
