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
	boolean up;
	
	public TopologyNetwork(boolean up) {
		this.up = up;
		points = new HashMap<Long, Point>();
		pending = new HashMap<Point, Set<Long>>();
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
			for (int i = 0; i < p._adjacent.length; i++) {
				new_[i] = p._adjacent[i];
			}
			new_[p._adjacent.length] = to.geocode;
			p._adjacent = new_;
		}
	}

	void addPending(Point saddle, Point term) {
		saddle = getPoint(saddle);
		Set<Long> terms = pending.get(saddle);
		if (terms == null) {
			terms = new HashSet<Long>();
			pending.put(saddle, terms);
		}
		terms.add(term.geocode);
	}
	
	public void build(Mesh m) {
		build(m, m.points.values());
	}
	
	public void build(IMesh m, Collection<Point> points) {
		for (Point p : points) {
			if (p.classify(m) == Point.CLASS_SADDLE) {
				for (Point lead : p.leads(m, up)) {
					processLead(m, p, lead);
				}
			}
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
		List<Point[]> pendings = new ArrayList<Point[]>();
		for (Entry<Point, Set<Long>> e : pending.entrySet()) {
			Point p = e.getKey();
			for (long ix : e.getValue()) {
				Point lead = m.get(ix); // TODO feels like we should ensure the lead-terms are loaded somehow (we only check their adjacency)
				if (lead == null) {
					continue;
				}

				pendings.add(new Point[] {p, lead});
			}
		}
		for (Point[] pair : pendings) {
			Point p = pair[0];
			Point lead = pair[1];
			ChaseResult result = processLead(m, p, lead);
				
			if (!result.indeterminate || !result.p.equals(lead)) {
				pending.get(p).remove(lead.geocode);
				if (pending.get(p).isEmpty()) {
					pending.remove(p);
				}
			}
		}		
		
		if (newPage != null) {
			build(m, newPage);
		}
	}
	
	public boolean complete(Set<Prefix> allPrefixes, Set<Prefix> unprocessed) {
		for (Entry<Prefix, Integer> e : tallyPending(allPrefixes).entrySet()) {
			if (e.getValue() > 0) {
				return false;
			}
		}
		return unprocessed.isEmpty();
	}

	public Map<Prefix, Integer> tallyPending(Set<Prefix> allPrefixes) {
		Map<Prefix, Integer> frontierTotals = new DefaultMap<Prefix, Integer>() {
			@Override
			public Integer defaultValue() {
				return 0;
			}
		};
		for (Set<Long> terms : pending.values()) {
			for (Long term : terms) {
				Set<Prefix> frontiers = new HashSet<Prefix>();
				for (Long adj : DEMManager.adjacency(term)) {
					for (Prefix potential : allPrefixes) {
						if (potential.isParent(adj)) {
							frontiers.add(potential);
							break;
						}
					}
				}
				for (Prefix p : frontiers) {
					frontierTotals.put(p, frontierTotals.get(p) + 1);
				}
			}
		}
		return frontierTotals;
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
