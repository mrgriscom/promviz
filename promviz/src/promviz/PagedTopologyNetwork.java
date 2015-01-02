package promviz;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.PreprocessNetwork.EdgeIterator;
import promviz.util.DefaultMap;
import promviz.util.Logging;

import com.google.common.collect.Lists;

public class PagedTopologyNetwork extends TopologyNetwork {

	static final int MAX_POINTS = (int)(Long.parseLong(DEMManager.props.getProperty("memory")) / 768);

	long ctr = 0;
	class PrefixInfo {
		String path;
		String elevPath;
		boolean loaded;
		List<Point> points;
		long ctr;
	}
	
	Map<Prefix, PrefixInfo> prefixes;
	PagedMesh m; // unused
	
	public PagedTopologyNetwork(boolean up, DEMManager dm) {
		this.up = up;
		points = new HashMap<Long, Point>((int)(MAX_POINTS / .75));
		pendingSaddles = new HashSet<Point>();

		prefixes = new HashMap<Prefix, PrefixInfo>();
		loadPrefixes();
		Logging.log("prefixes inventoried (" + prefixes.size() + ")");

		if (prefixes.size() > 0 && dm != null) {
			m = new PagedMesh(dm.partitionDEM(), dm.MESH_MAX_POINTS);
			Logging.log("dem coverage paritioned");
			this.dm = dm;			
		}
	}
	
	void loadPrefixes() {
		File folder = new File(DEMManager.props.getProperty("dir_net"));
		File[] listOfFiles = folder.listFiles();
		for (File f : listOfFiles) {
			String[] a = f.getName().split("-", 2);
			if (!a[0].equals(this.up ? "up" : "down")) {
				continue;
			}

			String[] b = a[1].split(",");
			Prefix pf = new Prefix(PointIndex.make(Integer.parseInt(b[1]), Integer.parseInt(b[2]), Integer.parseInt(b[3])), Integer.parseInt(b[0]));
			PrefixInfo pfi = new PrefixInfo();
			pfi.path = DEMManager.props.getProperty("dir_net") + "/" + f.getName();
			pfi.elevPath = PreprocessNetwork.prefixPath(this.up, "elev", pf, EdgeIterator.PHASE_RAW);
			pfi.loaded = false;
			prefixes.put(pf, pfi);
		}
	}
	
	public Point get(long ix) {
		Prefix pf = matchPrefix(ix);
		if (pf != null) {
			// important: do this before loading segment
			prefixes.get(pf).ctr = ctr++;
		}
		
		Point p = points.get(ix);
		if (p == null) {
			loadSegmentFor(ix);
		}
		return points.get(ix);
	}

	void loadSegmentFor(long ix) {
		if (matchPrefix(ix) == null) {
			System.err.println(ix);
		}
		loadSegment(matchPrefix(ix));
	}
	
	List<Point> loadSegment(Prefix prefix) {
		PrefixInfo info = prefixes.get(prefix);
		if (info.loaded) {
			return info.points;
		}

		Logging.log("pagedtn: loading network segment " + prefix);
		
		List<long[]> data = new ArrayList<long[]>();
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(prefixes.get(prefix).path)));
			try {
				while (true) {
					data.add(new long[] {in.readLong(), in.readLong()});
					int leadIx = in.readByte();
				}
			} catch (EOFException eof) {}
			in.close();
		} catch (IOException ioe) {
			throw new RuntimeException();
		}

		Map<Long, Float> elev = new HashMap<Long, Float>();
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(prefixes.get(prefix).elevPath)));
			try {
				while (true) {
					elev.put(in.readLong(), in.readFloat());
				}
			} catch (EOFException eof) {}
			in.close();
		} catch (IOException ioe) {
			throw new RuntimeException();
		}
		
		Set<Point> newPoints = new HashSet<Point>();
		for (long[] e : data) {
			for (long ix : e) {
				if (ix == 0xFFFFFFFFFFFFFFFFL) {
					continue;
				}
				if (!prefix.isParent(ix)) {
					continue;
				}

				Point p = points.get(ix);
				if (p == null) {
					p = new Point(ix, elev.get(ix));
					points.put(p.ix, p);
					newPoints.add(p);
				}
			}
		}
		
		for (long[] edge : data) {
			if (edge[1] == 0xFFFFFFFFFFFFFFFFL) {
				pendingSaddles.add(points.get(edge[0]));
			} else {
				if (prefix.isParent(edge[0])) {
					addDirectedEdge(points.get(edge[0]), edge[1]);
				}
				if (prefix.isParent(edge[1])) {
					addDirectedEdge(points.get(edge[1]), edge[0]);
				}
			}
		}

		Logging.log("pagedtn: " + newPoints.size() + " points loaded, " + points.size() + " total in network");
		while (points.size() > MAX_POINTS) {
			ejectSegment();
		}
		
		info.loaded = true;
		info.points = new ArrayList<Point>(newPoints);
		return info.points;
	}
	
	void ejectSegment() {
		Map.Entry<Prefix, PrefixInfo> toEject = null;
		for (Map.Entry<Prefix, PrefixInfo> e : prefixes.entrySet()) {
			PrefixInfo info = e.getValue();
			if (!info.loaded) {
				continue;
			}
			if (toEject == null || info.ctr < toEject.getValue().ctr) {
				toEject = e;
			}
		}
		removeSegment(toEject.getKey());
		Logging.log("pagedtn: removed " + toEject.getKey() + "; " + points.size() + " points");
	}
	
	void removeSegment(Prefix prefix) {
		PrefixInfo info = prefixes.get(prefix);
		for (Point p : info.points) {
			points.remove(p.ix);
			pendingSaddles.remove(p);
		}
		info.points = null;
		info.loaded = false;
	}
	
	class PointsIterator implements Iterator<Point> {
		Iterator<Prefix> prefixIterator;
		Iterator<Point> pointIterator;
		
		int c = 0;
		
		public PointsIterator(List<Prefix> pfs) {
			prefixIterator = pfs.iterator();	
		}
		
		public boolean hasNext() {
			return prefixIterator.hasNext() || pointIterator.hasNext();
		}

		public Point next() {
			while (pointIterator == null || !pointIterator.hasNext()) {
				System.err.println(++c + " / " + prefixes.size());
				pointIterator = loadSegment(prefixIterator.next()).iterator();
			}
			return pointIterator.next();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	public Iterable<Point> allPoints() {
		final List<Prefix> pfs = Lists.newArrayList(prefixes.keySet());
		Collections.sort(pfs, new Comparator<Prefix>() {
			public int compare(Prefix a, Prefix b) {
				int[] xa = PointIndex.split(a.prefix);
				int[] xb = PointIndex.split(b.prefix);
				
				if (xa[2] != xb[2]) {
					return (xa[2] < xb[2] ? -1 : 1);
				} else if (xa[1] != xb[1]) {
					return (xa[1] < xb[1] ? -1 : 1);
				} else {
					return 0;
				}
			}
		});
		return new Iterable<Point>() {
			public Iterator<Point> iterator() {
				return new PointsIterator(pfs);
			}
			
		};
	}
	
	Point getPoint(Point p) {
		return get(p.ix);
//		Point match = points.get(p.ix);
//		if (match == null) {
//			match = new Point(p.ix, p.elev);
//			points.put(match.ix, match);
//		}
//		return match;
	}
	
	void addEdge(Point a, Point b) {
		addDirectedEdge(a, b);
		addDirectedEdge(b, a);
	}
		
	void addDirectedEdge(Point from, long to_ix) {
		Point p = getPoint(from);
		// FUCKING JAVA!!
		// all this does is add the new point's geocode to the adjacency array if it isn't already in there
		boolean exists = false;
		for (Long l : p._adjacent) {
			if (l == to_ix) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			long[] new_ = new long[p._adjacent.length + 1];
			System.arraycopy(p._adjacent, 0, new_, 0, p._adjacent.length);
			new_[p._adjacent.length] = to_ix;
			p._adjacent = new_;
		}
	}
	
	Prefix matchPrefix(long ix) {
		for (int res = 0; res <= 24; res++) {
			Prefix pf = new Prefix(ix, res);
			if (prefixes.containsKey(pf)) {
				return pf;
			}
		}
		return null;
	}
			
	Set<Point> adjacent(Point p) {
		Point match = getPoint(p);
		Set<Point> adj = new HashSet<Point>();
		for (long l : match._adjacent) {
			adj.add(this.get(l));
		}
		return adj;
	}

}
