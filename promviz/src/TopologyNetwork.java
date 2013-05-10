import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



public class TopologyNetwork {

	Mesh m;
	Map<Point, Set<Point>> edges;
	boolean up;
	
	public TopologyNetwork(Mesh m, boolean up) {
		this.m = m;
		this.up = up;
		edges = new HashMap<Point, Set<Point>>();
	}
	
	void addEdge(Point a, Point b) {
		addDirectedEdge(a, b);
		if (b != null) {
			addDirectedEdge(b, a);
		}
	}
	
	void addDirectedEdge(Point from, Point to) {
		Set<Point> adj = edges.get(from);
		if (adj == null) {
			adj = new HashSet<Point>();
			edges.put(from, adj);
		}
		adj.add(to);
	}
	
	public void build() {
		for (Point p : m.points) {
			if (p.classify() == Point.CLASS_SADDLE) {
				for (Point lead : p.leads(up)) {
					addEdge(p, chase(lead, up));
				}
			}
		}
	}

	Point chase(Point p, boolean up) {
		while (p.classify() != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
			if (p.classify() == Point.CLASS_INDETERMINATE) {
				return null;
			}
		
			p = p.leads(up).get(0);
		}
		return p;
	}
		
	Set<Point> adjacent(Point p) {
		return this.edges.get(p);
	}
}
