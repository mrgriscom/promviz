package promviz;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



public class TopologyNetwork {

	Map<Long, Point> points;
	Map<Point, Set<Long>> pending;
	boolean up;
	
	public TopologyNetwork(boolean up) {
		this.up = up;
		points = new HashMap<Long, Point>();
		pending = new HashMap<Point, Set<Long>>();
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
		for (Point p : m.points.values()) {
			if (p.classify(m) == Point.CLASS_SADDLE) {
				for (Point lead : p.leads(m, up)) {
					ChaseResult result = chase(m, lead, up);
					if (!result.indeterminate) {
						addEdge(p, result.p);
					} else {
						addPending(p, result.p);
					}
				}
			}
		}
	}

	class ChaseResult {
		Point p;
		boolean indeterminate;
		
		public ChaseResult(Point p, boolean indeterminate) {
			this.p = p;
			this.indeterminate = indeterminate;
		}
	}
	
	ChaseResult chase(Mesh m, Point p, boolean up) {
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
