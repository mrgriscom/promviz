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
//		List<List<Long>> ro = new ArrayList<List<Long>>();
//		for (Entry<Long, long[]> e : anchors(up, ixs).entrySet()) {
//			long ix = e.getKey();
//			Logging.log("ss:"+Util.print(ix));
//			for (long anch : e.getValue()) {
//				Logging.log("  anch:" + Util.print(anch));
//				ro.add(chase(mst, up, ix, anch));
//			}
//		}
//		return ro;
	}

	static class Trace {
		List<Long> path = new ArrayList<Long>();
		Set<Long> set = new HashSet<Long>();
		
		boolean contains(long ix) {
			return set.contains(ix);
		}
		
		void add(long ix) {
			path.add(ix);
			set.add(ix);
		}
		
		long head() {
			return path.get(path.size() - 1);
		}
		
		void trimAfter(long ix) {
			int i0 = path.indexOf(ix) + 1;
			assert i0 > 0;
			for (int i = i0; i < path.size(); i++) {
				set.remove(path.get(i));
			}
			path = new ArrayList<Long>(path.subList(0, i0));
		}
	}

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

		final long END_OF_WORLD = PointIndex.NULL - 1;
		
		int i = 0;
		while (completed.size() < traces.size()) {
			for (Trace t : traces) {
				if (completed.contains(t)) {
					continue;
				}
				
				long cur = t.path.get(t.path.size() - 1);
				if (cur == PointIndex.NULL) {
					t.trimAfter(t.path.get(t.path.size() - 2));
					completed.add(t);
					continue;
				}
				
				Trace intersected = null;
				for (Trace ot : traces) {
					if (t == ot) {
						continue;
					}
					if (ot.contains(cur)) {
						if (completed.contains(ot) && ot.head() == cur) {
							//multi-fuckage
							continue;
						}
						intersected = ot;
						break;
					}
				}
				if (intersected != null) {
					completed.add(t);
					if (completed.contains(intersected)) {
						Trace other = null;
						for (Trace o : completed) {
							if (o != intersected && o.head() == intersected.head()) {
								other = o;
								break;
							}
						}
						completed.remove(other);
					}
					completed.add(intersected);
					intersected.trimAfter(cur);
					continue;
				}
				
				Edge e = mst.get(cur);
				if (e == null) {
					loadChunk(mst, TopologyBuilder._chunk(cur), !up);
					e = mst.get(cur);
					if (e == null) {
						// we don't have end-of-world min-saddles
						e = new Edge(cur, PointIndex.NULL, END_OF_WORLD);
					}
				}
				if (e.saddle != END_OF_WORLD) {
					t.add(e.saddle);
				}

				long next = e.b;
				if (t.contains(next)) {
					// loop check
					Logging.log("mst loop detected");
					assert next == t.path.get(t.path.size() - 4); // ensure loop only in last two steps
					completed.add(t);
				}
				t.add(next);
			}
			
			i++;
		}
		
		List<List<Long>> ro = new ArrayList<List<Long>>();
		for (Trace t : traces) {
			ro.add(t.path);
		}
		return ro;
	}
	
	public static List<Long> chase(Map<Long, Edge> mst, boolean up, long ix, long anch) {
		List<Long> path = new ArrayList<Long>();
		Set<Long> _inPath = new HashSet<Long>();
		path.add(ix);
		_inPath.add(ix);
		
		final long END_OF_WORLD = PointIndex.NULL - 1;
		
		long cur = anch;		
		while (cur != PointIndex.NULL) {
			path.add(cur);
			_inPath.add(cur);
			
			Edge e = mst.get(cur);
			if (e == null) {
				loadChunk(mst, TopologyBuilder._chunk(cur), !up);
				e = mst.get(cur);
				if (e == null) {
					// we don't have end-of-world min-saddles
					e = new Edge(cur, PointIndex.NULL, END_OF_WORLD);
				}
			}
			if (e.saddle != END_OF_WORLD) {
				path.add(e.saddle);
				_inPath.add(e.saddle);
			}
			cur = e.b;

			// loop check
			if (_inPath.contains(cur)) {
				Logging.log("mst loop detected");
				assert cur == path.get(path.size() - 4); // ensure loop only in last two steps
				break;
			}
		}
		return path;
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
