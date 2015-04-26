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
	
	static final int BUCKET_MAX_POINTS = 2048;
		

	
		
	
	static Prefix matchPrefix(long ix, Set<Prefix> buckets) {
		for (int res = BASE_RES; res < 24; res++) {
			Prefix p = new Prefix(ix, res);
			if (buckets.contains(p)) {
				return p;
			}
		}
		return null;
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
