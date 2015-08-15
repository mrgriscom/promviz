package promviz.debug;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import promviz.Edge;
import promviz.FileUtil;
import promviz.Main;
import promviz.Point;
import promviz.PointIndex;
import promviz.Prefix;
import promviz.TopologyBuilder;
import promviz.dem.GeoProjection;
import promviz.dem.Projection;
import promviz.util.GeoCode;
import promviz.util.Logging;
import promviz.util.Util;

import com.google.gson.Gson;

public class Runoff {

	public static void main(String[] args) {
		Logging.init();
		Main.initProps();
		
		final Map<Integer, Projection> projs = new HashMap<Integer, Projection>();
		projs.put(0, GeoProjection.fromArcseconds(3.));
		Projection.authority = new Projection.Authority() {
			public Projection forRef(int refID) {
				return projs.get(refID);
			}
		};
		
		boolean up = (args[0].equals("u"));
		List<Long> ixs = new ArrayList<Long>();
		for (int i = 1; i < args.length; i++) {
			double[] c = GeoCode.toCoord(Long.valueOf(args[i], 16));
			int[] xy = Projection.authority.forRef(0).toGrid(c[0], c[1]);
			long ix = PointIndex.make(0, xy[0], xy[1]);
			Logging.log(Util.print(ix));
			ixs.add(ix);
		}
		
		List<List<Long>> ro = runoff(up, ixs);
		
		List<List<double[]>> paths = new ArrayList<List<double[]>>();
		for (List<Long> roseg : ro) {
			List<double[]> seg = new ArrayList<double[]>();
			paths.add(seg);
			for (long ix : roseg) {
				seg.add(PointIndex.toLatLon(ix));
			}
		}
		
		Gson ser = new Gson();
		System.out.println(ser.toJson(paths));
	}
	
	public static List<List<Long>> runoff(boolean up, List<Long> ixs) {
		Map<Long, Edge> mst = new HashMap<Long, Edge>();
		
		return chaseAll(up, mst, anchors(up, ixs));
	}

	static class Trace {
		List<Long> path = new ArrayList<Long>();
		Set<Long> set = new HashSet<Long>();
		
		boolean contains(long ix) {
			return set.contains(ix);
		}
		
		void add(long ix) {
			assert !contains(ix);
			path.add(ix);
			set.add(ix);
		}
		
		int len() {
			return path.size();
		}

		int pos(int i) {
			return i < 0 ? len() - i : i;
		}
		
		long get(int i) {
			return path.get(pos(i));
		}
		
		int find(long ix) {
			return path.indexOf(ix);
		}
		
		long head() {
			return get(-1);
		}

		List<Long> trimAt(int i) { // removes path[i] and after
			i = pos(i);
			List<Long> trimmed = new ArrayList<Long>(path.subList(i, path.size()));
			set.removeAll(trimmed); // requires no loop
			path = new ArrayList<Long>(path.subList(0, i));
			return trimmed;
		}
		
		List<Long> trimAfter(long ix) {
			int i = find(ix);
			assert i >= 0;
			return trimAt(i + 1);
		}
	}

	static final long END_OF_WORLD = PointIndex.NULL - 1; // sentinel

	public static List<List<Long>> chaseAll(boolean up, Map<Long, Edge> mst, Map<Long, long[]> seed) {
		List<Trace> traces = new ArrayList<Trace>();
		Set<Trace> completed = new HashSet<Trace>();
		for (Entry<Long, long[]> e : seed.entrySet()) {
			long ix = e.getKey();
			for (long anch : e.getValue()) {
				Trace t = new Trace();
				t.add(ix);
				t.add(anch);
				traces.add(t);
			}
		}

		// cache the edges we fetch from mst to avoid reloading chunks if we need to recover parts of traces
		Map<Long, Edge> cache = new HashMap<Long, Edge>();
		while (completed.size() < traces.size()) {
			int shortest = -1;
			for (Trace t : traces) {
				if (!completed.contains(t) && (shortest == -1 || t.len() < shortest)) {
					shortest = t.len();
				}
			}
			
			for (Trace t : traces) {
				if (completed.contains(t)) {
					continue;
				}
				// let shorter traces catch up so ideally all active traces are the same length
				if (t.len() != shortest) {
					continue;
				}
				
				long cur = t.get(-1);
				if (cur == PointIndex.NULL) {
					// end of world
					t.trimAt(-1);
					completed.add(t);
					continue;
				}
				
				// check if we've intersected another trace
				Trace intersected = null;
				// TODO replace iteration over traces with ix -> trace(s) index. probably not necessary as # traces is usually small
				for (Trace other : traces) {
					if (t == other) {
						continue;
					}
					if (other.contains(cur)) {
						if (other.head() == cur && completed.contains(other)) {
							// this is a multi-intersection and current trace is odd one out; continue trace
							continue;
						}
						intersected = other;
						break;
					}
				}
				if (intersected != null) {
					// check if that trace had already intersected another trace; if so, resume
					// the other intersected trace
					if (completed.contains(intersected)) {
						// t.head() == intersected.head() only if last node before end-of-world
						Trace intersectedComplement = null;
						for (Trace other : completed) {
							if (other != intersected && other.head() == intersected.head()) {
								intersectedComplement = other;
								break;
							}
						}
						if (intersectedComplement != null) {
							completed.remove(intersectedComplement);
						}
					} else {
						completed.add(intersected);
					}
					intersected.trimAfter(cur);
					completed.add(t);
					continue;
				}
				
				// no intersection; keep tracing
				Edge e = getNext(cur, mst, cache, up);
				if (e.saddle != END_OF_WORLD) {
					t.add(e.saddle);
				}
				long next = e.b;
				// loop check (in theory the MST data structure should not have loops;
				// in practice, we protect against them)
				if (t.contains(next)) {
					Logging.log("mst loop detected");
					assert next == t.get(-4); // ensure loop only in last two steps
					t.trimAt(-1);
					completed.add(t);
				}
				t.add(next);
			}
		}
		
		List<List<Long>> ro = new ArrayList<List<Long>>();
		for (Trace t : traces) {
			ro.add(t.path);
		}
		return ro;
	}

	static Edge getNext(long cur, Map<Long, Edge> mst, Map<Long, Edge> cache, boolean up) {
		Edge e = cache.get(cur);
		if (e == null) {
			e = mst.get(cur);
		}
		if (e == null) {
			loadChunk(mst, TopologyBuilder._chunk(cur), !up);
			e = mst.get(cur);
		}
		if (e == null) {
			// we don't have end-of-world min-saddles
			e = new Edge(cur, PointIndex.NULL, END_OF_WORLD);
		}
		cache.put(cur, e);
		return e;
	}					
	
	public static Map<Long, long[]> anchors(boolean up, List<Long> ixs) {
		Set<Prefix> chunks = new HashSet<Prefix>();
		for (long ix : ixs) {
			chunks.add(TopologyBuilder._chunk(ix));
		}
		
		Map<Long, Edge> bySaddle = new HashMap<Long, Edge>();
		for (Prefix chunk : chunks) {
			Logging.log(chunk);
			Iterable<Edge> edges = FileUtil.loadEdges(!up, chunk, FileUtil.PHASE_RAW);
			for (Edge e : edges) {
				bySaddle.put(e.saddle, e);
			}
		}
		
		Map<Long, long[]> anch = new HashMap<Long, long[]>();
		for (long ix : ixs) {
			Edge e = bySaddle.get(ix);
			anch.put(ix, e != null ? new long[] {e.a, e.b} : new long[] {PointIndex.NULL, PointIndex.NULL});
			// note: it's valid for one or both leads to be null (saddle indeterminate in opposite-world)
		}
		return anch;
	}
		
	static void loadChunk(Map<Long, Edge> mst, Prefix chunk, boolean up) {
		Logging.log("loading " + chunk);
		Iterable<Edge> edges = FileUtil.loadEdges(up, chunk, FileUtil.PHASE_MST);
		for (Edge e : edges) {
			if (chunk.isParent(e.a)) {
				mst.put(e.a, e);
			}
		}
	}
}
