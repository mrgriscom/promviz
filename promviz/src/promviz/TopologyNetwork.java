package promviz;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;



public class TopologyNetwork implements IMesh {

	Map<Long, Point> points;
	Map<Point, Set<Long>> pending;
	Set<Long> unprocessedFringe;
	boolean up;
	
	Set<Long> pendInterior = new HashSet<Long>();
	
	DEMManager dm;
	
	DataOutputStream f;
	int numEdges = 0;
	
	class PendingMap extends DefaultMap<Point, Set<Long>> {
		@Override
		public Set<Long> defaultValue(Point _) {
			return new HashSet<Long>();
		}
	}
	
	public TopologyNetwork() { }
	
	public TopologyNetwork(boolean up, DEMManager dm) {
		this.up = up;
		points = new HashMap<Long, Point>();
		pending = new PendingMap();
		unprocessedFringe = new HashSet<Long>();
		
		this.dm = dm;
	}
	
	public void enableCache() {
		try {
			f = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(DEMManager.props.getProperty("dir_netdump") + "/" + (up ? "up" : "down"))));
		} catch (IOException ioe) {
			throw new RuntimeException();
		}		
	}
	
	public static TopologyNetwork load(boolean up, DEMManager dm) {
		TopologyNetwork tn = new TopologyNetwork(up, dm);
		PagedMesh m = new PagedMesh(dm.partitionDEM(), dm.MESH_MAX_POINTS);
		try {
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(DEMManager.props.getProperty("dir_netdump") + "/" + (up ? "up" : "down"))));
			try {
				while (true) {
					long[] ix = {in.readLong(), in.readLong()};
					Point[] p = new Point[2];
					for (int i = 0; i < 2; i++) {
						long _ix = ix[i];
						if (_ix == 0xFFFFFFFFFFFFFFFFL) {
							continue;
						}
						Point _p = tn.get(_ix);
						if (_p == null) {
							_p = m.get(_ix);
						}
						if (_p == null) {
							m.loadPage(new DEMManager.Prefix(_ix, DEMManager.GRID_TILE_SIZE));
							_p = m.get(_ix);
						}
						p[i] = _p;
					}
					if (ix[1] == 0xFFFFFFFFFFFFFFFFL) {
						tn.pending.put(tn.getPoint(p[0]), null);
					} else {
						tn.addEdge(tn.getPoint(p[0]), tn.getPoint(p[1]));
					}
				}
			} catch (EOFException eof) {}		
		} catch (IOException ioe) {
			throw new RuntimeException();
		}
		return tn;
	}
	
	public Point get(long ix) {
		return points.get(ix);
	}
	
	Point getPoint(Point p) {
		Point match = points.get(p.ix);
		if (match == null) {
			match = new Point(p.ix, p.elev);
			points.put(match.ix, match);
		}
		return match;
	}
	
	void addEdge(Point a, Point b) {
		if (f == null) {
			addDirectedEdge(a, b);
			addDirectedEdge(b, a);
		}
		try {
			if (f != null) {
				f.writeLong(a.ix);
				f.writeLong(b.ix);
			}
			numEdges++;
		} catch (IOException ioe) {
			throw new RuntimeException();
		}
	}
	
	public Iterable<Point> allPoints() {
		return points.values();
	}
	
	void cleanup() {
		try {
			for (Point p : pending.keySet()) {
				f.writeLong(p.ix);
				f.writeLong(0xFFFFFFFFFFFFFFFFL);
			}
			f.close();
		} catch (IOException ioe) {
			throw new RuntimeException();
		}
	}
	
	
	
	void addDirectedEdge(Point from, Point to) {
		Point p = getPoint(from);
		// FUCKING JAVA!!
		// all this does is add the new point's geocode to the adjacency array if it isn't already in there
		boolean exists = false;
		for (Long l : p._adjacent) {
			if (l == to.ix) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			long[] new_ = new long[p._adjacent.length + 1];
			System.arraycopy(p._adjacent, 0, new_, 0, p._adjacent.length);
			new_[p._adjacent.length] = to.ix;
			p._adjacent = new_;
		}
	}

	void addPending(Point saddle, Point term) {
		saddle = getPoint(saddle);
		pending.get(saddle).add(term.ix);
	}
	
	public void build(IMesh m, List<DEMFile.Sample> points) {
		long start = System.currentTimeMillis();
		
		for (DEMFile.Sample s : points) {
			Point p = new GridPoint(s);
			int pointClass = p.classify(m);
			if (pointClass == Point.CLASS_SADDLE) {
				processSaddle(m, p);
			} else if (pointClass == Point.CLASS_INDETERMINATE) {
				unprocessedFringe.add(p.ix);
			}
		}
		
		Logging.log("@build new page " + (System.currentTimeMillis() - start));
	}

	void processSaddle(IMesh m, Point p) {
		for (Point lead : p.leads(m, up)) {
			processLead(m, p, lead);
		}
	}
	
	ChaseResult processLead(IMesh m, Point p, Point lead) {
		ChaseResult result = chase(m, lead, up);
		if (!result.indeterminate) {
			addEdge(p, result.p);
		} else {
			addPending(p, result.p);
		}
		return result;
	}
	
	public void buildPartial(PagedMesh m, List<DEMFile.Sample> newPage) {
		Map<Point, Set<Long>> oldPending = pending;
		pending = new PendingMap();
		for (Entry<Point, Set<Long>> e : oldPending.entrySet()) {
			Point saddle = e.getKey();
			for (long ix : e.getValue()) {
				Point lead = m.get(ix);
				if (lead == null) {
					// point not loaded -- effectively indeterminate
					pending.get(saddle).add(ix); // replicate entry in new map
				} else {
					processLead(m, saddle, lead);
				}
			}
		}
		Logging.log("# pending: " + oldPending.size() + " -> " + pending.size());
		Set<Long> fringeNowProcessed = new HashSet<Long>();
		for (long ix : unprocessedFringe) {
			// TODO don't reprocess points that were also pending leads?
			// arises when a saddle leads to another saddle
			Point p = m.get(ix);
			int pointClass = (p != null ? p.classify(m) : Point.CLASS_INDETERMINATE);
			if (pointClass != Point.CLASS_INDETERMINATE) {
				fringeNowProcessed.add(ix);
				if (pointClass == Point.CLASS_SADDLE) {
					processSaddle(m, p);
				}
			}
		}
		unprocessedFringe.removeAll(fringeNowProcessed);
		Logging.log("# fringe: " + (unprocessedFringe.size() + fringeNowProcessed.size()) + " -> " + unprocessedFringe.size());

		if (newPage != null) {
			build(m, newPage);
		}
		
		trimNetwork();
	}
	
	void trimNetwork() {
		List<Point> toRemove = new ArrayList<Point>();
		for (Point p : points.values()) {
			if (!pending.containsKey(p)) {
				toRemove.add(p);
			}
		}
		for (Point p : toRemove) {
			points.remove(p);
		}
	}
	
	public boolean complete(Set<Prefix> allPrefixes, Set<Prefix> unprocessed) {
		// FIXME what about interior nodata nodes? we will never load data for them. if they're on the ridgepath, infinite loop?
		if (!unprocessed.isEmpty()) {
			return false;
		}
		for (Entry<Set<Prefix>, Integer> e : tallyPending(allPrefixes).entrySet()) {
			if (e.getValue() > 0) {
				return false;
			}
		}
		return true;
	}

	public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
		Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
			@Override
			public Integer defaultValue(Set<Prefix> _) {
				return 0;
			}
		};
		tallyPending(allPrefixes, frontierTotals);
		return frontierTotals;
	}
	
	public void tallyPending(Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> frontierTotals) {
		for (Entry<Point, Set<Long>> e : pending.entrySet()) {
			Set<Long> terms = e.getValue();
			List<Long> edgePend = new ArrayList<Long>();	
			for (long term : terms) {
				boolean interior = tallyAdjacency(term, allPrefixes, frontierTotals);
				if (interior) {
					pendInterior.add(term);
				} else {
					edgePend.add(term);
					
					try {
						f.writeLong(e.getKey().ix);
						f.writeLong(0xFFFFFFFFFFFFFFFFL);
					} catch (IOException ioe) { }
				}
			}
			terms.removeAll(edgePend);
		}
		
		List<Long> edgeFringe = new ArrayList<Long>();
		for (long fringe : unprocessedFringe) {
			boolean interior = tallyAdjacency(fringe, allPrefixes, frontierTotals);
			if (interior) {
				pendInterior.add(fringe);
			} else {
				edgeFringe.add(fringe);
			}
		}
		unprocessedFringe.removeAll(edgeFringe);

		// trim pendInterior to only what is still relevant
		Set<Long> newPendInterior = new HashSet<Long>();
		for (Set<Long> terms : pending.values()) {
			for (long term : terms) {
				if (pendInterior.contains(term)) {
					newPendInterior.add(term);
				}
			}
		}
		for (long fringe : unprocessedFringe) {
			if (pendInterior.contains(fringe)) {
				newPendInterior.add(fringe);
			}
		}
		pendInterior = newPendInterior;
	}

	Prefix matchPrefix(long ix, Set<Prefix> prefixes) {
		Prefix p = new Prefix(ix, DEMManager.GRID_TILE_SIZE);
		return (prefixes.contains(p) ? p : null);
	}
	
	boolean tallyAdjacency(long ix, Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> totals) {
		Set<Prefix> frontiers = new HashSet<Prefix>();
		frontiers.add(matchPrefix(ix, allPrefixes));
		for (long adj : DEMManager.adjacency(ix)) {
			// find the parititon the adjacent point lies in
			Prefix partition = matchPrefix(adj, allPrefixes);

			if (!pendInterior.contains(ix)) {
				if (partition == null) {
					// point is adjacent to a point that will never be loaded, i.e., on the edge of the
					// entire region of interest; it will be indeterminate forever
					return false;
				}
				if (!dm.inScope(adj)) {
				//i think this is the only required check; the prefix-matching above can be discarded
					return false;
				}
			}
				
			frontiers.add(partition);
		}
		
		totals.put(frontiers, totals.get(frontiers) + 1);
		return true;
	}
	
	class ChaseResult {
		Point p;
		boolean indeterminate;
		
		public ChaseResult(Point p, boolean indeterminate) {
			this.p = p;
			this.indeterminate = indeterminate;
		}
	}
	
	ChaseResult chase(IMesh m, Point p, boolean up) {
		while (p.classify(m) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
			if (p.classify(m) == Point.CLASS_INDETERMINATE) {
				return new ChaseResult(p, true);
			}
		
			p = p.leads(m, up).get(0);
		}
		return new ChaseResult(p, false);
	}
		
	Set<Point> adjacent(Point p) {
		Point match = getPoint(p);
		Set<Point> adj = new HashSet<Point>();
		for (long l : match._adjacent) {
			adj.add(points.get(l));
		}
		return adj;
	}
}
