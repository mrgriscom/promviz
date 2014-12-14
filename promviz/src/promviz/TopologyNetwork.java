package promviz;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import promviz.Point.Lead;
import promviz.util.DefaultMap;
import promviz.util.Logging;

public class TopologyNetwork implements IMesh {

	Map<Long, Point> points;
	Set<Lead> pendingLeads;
	Set<Point> pendingSaddles;
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
		pendingLeads = new HashSet<Lead>();
		pendingSaddles = new HashSet<Point>();
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
	
//	public static TopologyNetwork load(boolean up, DEMManager dm) {
//		TopologyNetwork tn = new TopologyNetwork(up, dm);
//		PagedMesh m = new PagedMesh(dm.partitionDEM(), dm.MESH_MAX_POINTS);
//		try {
//			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(DEMManager.props.getProperty("dir_netdump") + "/" + (up ? "up" : "down"))));
//			try {
//				while (true) {
//					long[] ix = {in.readLong(), in.readLong()};
//					Point[] p = new Point[2];
//					for (int i = 0; i < 2; i++) {
//						long _ix = ix[i];
//						if (_ix == 0xFFFFFFFFFFFFFFFFL) {
//							continue;
//						}
//						Point _p = tn.get(_ix);
//						if (_p == null) {
//							_p = m.get(_ix);
//						}
//						if (_p == null) {
//							m.loadPage(new DEMManager.Prefix(_ix, DEMManager.GRID_TILE_SIZE));
//							_p = m.get(_ix);
//						}
//						p[i] = _p;
//					}
//					if (ix[1] == 0xFFFFFFFFFFFFFFFFL) {
//						tn.pending.put(tn.getPoint(p[0]), null);
//					} else {
//						tn.addEdge(tn.getPoint(p[0]), tn.getPoint(p[1]));
//					}
//				}
//			} catch (EOFException eof) {}		
//		} catch (IOException ioe) {
//			throw new RuntimeException();
//		}
//		return tn;
//	}
	
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
	
	void addEdge(Lead lead) {
		Point a = lead.p0;
		Point b = lead.p;
		int i = lead.i;
		
		if (f == null) {
			addDirectedEdge(a, b);
			addDirectedEdge(b, a);
		}
		try {
			if (f != null) {
				f.writeLong(a.ix);
				f.writeLong(b.ix);
				f.writeByte(i);
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
		for (Lead lead : pendingLeads) {
			writePendingLead(lead);
		}
		try {
			f.close();
		} catch (IOException ioe) {
			throw new RuntimeException();
		}
	}
	
	void writePendingLead(Lead lead) {
		try {
			f.writeLong(lead.p0.ix);
			f.writeLong(0xFFFFFFFFFFFFFFFFL);
			f.writeByte(lead.i);
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

	void addPending(Lead lead) {
		lead.p0 = getPoint(lead.p0); // cargo culting?
		pendingLeads.add(lead);
		pendingSaddles.add(lead.p0);
	}
	
	public void build(IMesh m, Iterable<DEMFile.Sample> points) {
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
		for (Lead lead : p.leads(m, up)) {
			processLead(m, lead);
		}
	}
	
	ChaseResult processLead(IMesh m, Lead lead) {
		ChaseResult result = chase(m, lead, up);
		lead = result.lead;
		if (!result.indeterminate) {
			addEdge(lead);
		} else {
			addPending(lead);
		}
		return result;
	}
	
	public void buildPartial(PagedMesh m, Iterable<DEMFile.Sample> newPage) {
		Set<Lead> oldPending = pendingLeads;
		pendingLeads = new HashSet<Lead>();
		pendingSaddles = new HashSet<Point>();
		for (Lead lead : oldPending) {
			Point saddle = lead.p0;
			Point head = m.get(lead.p.ix);
			if (head == null) {
				// point not loaded -- effectively indeterminate
				addPending(lead); // replicate entry in new map
			} else {
				processLead(m, lead);
			}
		}
		
		Logging.log("# pending: " + oldPending.size() + " -> " + pendingLeads.size());
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
			if (!pendingSaddles.contains(p)) {
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
		for (Lead lead : pendingLeads) {
			long term = lead.p.ix;
			boolean interior = tallyAdjacency(term, allPrefixes, frontierTotals);
			if (interior) {
				pendInterior.add(term);
			} else {
				writePendingLead(lead);
			}
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
		for (Lead lead : pendingLeads) {
			long term = lead.p.ix;
			if (pendInterior.contains(term)) {
				newPendInterior.add(term);
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
		Lead lead;
		boolean indeterminate;
		
		public ChaseResult(Lead lead, Point term, boolean indeterminate) {
			this.lead = new Lead(lead.p0, term, lead.i);
			this.indeterminate = indeterminate;
		}
	}
	
	ChaseResult chase(IMesh m, Lead lead, boolean up) {
		Point p = lead.p;
		while (p.classify(m) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
			if (p.classify(m) == Point.CLASS_INDETERMINATE) {
				return new ChaseResult(lead, p, true);
			}
		
			p = p.leads(m, up).get(0).p;
		}
		return new ChaseResult(lead, p, false);
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
