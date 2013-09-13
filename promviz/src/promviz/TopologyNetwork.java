package promviz;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;



public class TopologyNetwork implements IMesh {

	Map<Long, Point> points;
	Map<Point, Set<Long>> pending;
	Set<Long> unprocessedFringe;
	boolean up;
	
	DEMManager dm;
	
	class PendingMap extends DefaultMap<Point, Set<Long>> {
		@Override
		public Set<Long> defaultValue() {
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
	
	public Point get(long ix) {
		return points.get(ix);
	}
	
	Point getPoint(Point p) {
		Point match = points.get(p.geocode);
		if (match == null) {
			match = new Point(p.geocode, p.elev);
			points.put(match.geocode, match);
		}
		return match;
	}
	
	void addEdge(Point a, Point b) {
		addDirectedEdge(a, b);
		addDirectedEdge(b, a);
	}
	
	void addDirectedEdge(Point from, Point to) {
		Point p = getPoint(from);
		// FUCKING JAVA!!
		// all this does is add the new point's geocode to the adjacency array if it isn't already in there
		boolean exists = false;
		for (Long l : p._adjacent) {
			if (l == to.geocode) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			long[] new_ = new long[p._adjacent.length + 1];
			System.arraycopy(p._adjacent, 0, new_, 0, p._adjacent.length);
			new_[p._adjacent.length] = to.geocode;
			p._adjacent = new_;
		}
	}

	void addPending(Point saddle, Point term) {
		saddle = getPoint(saddle);
		pending.get(saddle).add(term.geocode);
	}
	
	public void build(Mesh m) {
		build(m, m.points.values());
	}
	
	public void build(IMesh m, Collection<Point> points) {
		for (Point p : points) {
			int pointClass = p.classify(m);
			if (pointClass == Point.CLASS_SADDLE) {
				processSaddle(m, p);
			} else if (pointClass == Point.CLASS_INDETERMINATE) {
				unprocessedFringe.add(p.geocode);
			}
		}
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
	
	public void buildPartial(PagedMesh m, Set<Point> newPage) {
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

		if (newPage != null) {
			build(m, newPage);
		}
	}
	
	public boolean complete(Set<Prefix> allPrefixes, Set<Prefix> unprocessed) {
		// FIXME what about interior nodata nodes? we will never load data for them. if they're on the ridgepath, infinite loop?
		for (Entry<Set<Prefix>, Integer> e : tallyPending(allPrefixes).entrySet()) {
			if (e.getValue() > 0) {
				return false;
			}
		}
		return unprocessed.isEmpty();
	}

	public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
		Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
			@Override
			public Integer defaultValue() {
				return 0;
			}
		};
		tallyPending(allPrefixes, frontierTotals);
		return frontierTotals;
	}
	
	public void tallyPending(Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> frontierTotals) {
		for (Set<Long> terms : pending.values()) {
			for (long term : terms) {
				tallyAdjacency(term, allPrefixes, frontierTotals);
			}
		}
		for (long fringe : unprocessedFringe) {
			tallyAdjacency(fringe, allPrefixes, frontierTotals);
		}
	}

	Prefix matchPrefix(long ix, Set<Prefix> prefixes) {
		for (Prefix potential : prefixes) {
			if (potential.isParent(ix)) {
				return potential;
			}
		}
		return null;
	}
	
	boolean tallyAdjacency(long ix, Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> totals) {
		Set<Prefix> frontiers = new HashSet<Prefix>();
		frontiers.add(matchPrefix(ix, allPrefixes));
		for (long adj : DEMManager.adjacency(ix)) {
			// find the parititon the adjacent point lies in
			Prefix partition = matchPrefix(adj, allPrefixes);
			if (partition == null) {
				// point is adjacent to a point that will never be loaded, i.e., on the edge of the
				// entire region of interest; it will be indeterminate forever
				return false;
			}
			if (!dm.inScope(adj)) {
			//i think this is the only required check; the prefix-matching above can be discarded
				return false;
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
