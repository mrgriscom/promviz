import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class PromNetwork {

	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		Comparator<Point> c;
		
		public PromInfo(Point p, Comparator<Point> c) {
			this.p = p;
			this.c = c;
		}
		
		public double prominence() {
			return Math.abs(p.elev - saddle.elev);
		}
		
		public void add(Point cur) {
			if (saddle == null || c.compare(cur, saddle) < 0) {
				saddle = cur;
			}
		}
	}
	
	static class Front {
		PriorityQueue<Point> queue;
		Set<Point> set;
		TopologyNetwork tree;
		
		public Front(final Comparator<Point> c, TopologyNetwork tree) {
			this.tree = tree;
			queue = new PriorityQueue<Point>(10, new Comparator<Point>() {
				@Override
				public int compare(Point p1, Point p2) {
					return -c.compare(p1, p2);
				}				
			});
			set = new HashSet<Point>();
		}
		
		public void add(Point p) {
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
			}
		}
		
		public Point next() {
			Point p = queue.poll();
			set.remove(p);
			return p;
		}
		
		public Set<Point> adjacent() {
			Set<Point> frontAdj = new HashSet<Point>();
			for (Point f : queue) {
				for (Point adj : tree.adjacent(f)) {
					frontAdj.add(adj);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return set.size();
		}
	}
	
	public static PromInfo prominence(TopologyNetwork tree, Point p, final boolean up) {
		Comparator<Point> c = new Comparator<Point>() {
			@Override
			public int compare(Point p0, Point p1) {
				return up ? Double.compare(p0.elev, p1.elev) : Double.compare(p1.elev, p0.elev);
			}
		};
		
		PromInfo pi = new PromInfo(p, c);
		Front front = new Front(c, tree);
		front.add(p);
		Set<Point> seen = new HashSet<Point>();
		int seenPruneThreshold = 1;

		// point is not part of network (ie too close to edge to have connecting saddle)
		if (tree.edges.get(p) == null) {
			pi.add(p);
			pi.min_bound_only = true;
			return pi;
		}

		outer:
		while (true) {
			Point cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				pi.global_max = true;
				break;
			}
			pi.add(cur);
			if (c.compare(cur, p) > 0) {
				break;
			}

			seen.add(cur);

			for (Point adj : tree.edges.get(cur)) {
				if (adj == null) {
					// reached an edge
					pi.min_bound_only = true;
					break outer;
				}
				
				if (!seen.contains(adj)) {
					front.add(adj);
				}
			}

			// prune set of already handled points
//			if (seen.size() > seenPruneThreshold) {
//				seen.retainAll(front.adjacent(tree));
//				seenPruneThreshold = 2 * seen.size();
//			}
		}
		
		return pi;
	}

	
}