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
		List<List<Long>> ro = new ArrayList<List<Long>>();
		
		for (Entry<Long, long[]> e : anchors(up, ixs).entrySet()) {
			long ix = e.getKey();
			Logging.log("ss:"+Util.print(ix));
			for (long anch : e.getValue()) {
				Logging.log("  anch:" + Util.print(anch));
				ro.add(chase(mst, up, ix, anch));
			}
		}
		return ro;
	}

	public static List<Long> chase(Map<Long, Edge> mst, boolean up, long ix, long anch) {
		List<Long> path = new ArrayList<Long>();
		Set<Long> _inPath = new HashSet<Long>();
		path.add(ix);
		_inPath.add(ix);
		
		long cur = anch;		
		while (cur != PointIndex.NULL) {
			path.add(cur);
			_inPath.add(cur);
			
			// still getting loop at eow?			
			Edge e = mst.get(cur);
			if (e == null) {
				loadChunk(mst, TopologyBuilder._chunk(cur), !up);
				e = mst.get(cur);
				if (e == null) { // necessary check because we don't have end-of-world min-saddles
					break;
				}
			}
			path.add(e.saddle);
			_inPath.add(e.saddle);
			cur = e.b;

			if (_inPath.contains(cur)) {
				Logging.log("loop?");
				Logging.log(cur == path.get(path.size() - 4));
				
				Logging.log(new Point(cur, 0));
				Logging.log(new Point(path.get(path.size() - 2), 0));
				
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
