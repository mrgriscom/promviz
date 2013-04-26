import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;


public class Prominence {

	static class PromInfo {
		Point p;
		Point saddle;
		boolean global_max;
		boolean min_bound_only;
		
		public PromInfo(Point p) {
			this.p = p;
		}
		
		public double prominence() {
			return p.elev - saddle.elev;
		}
		
		public void add(Point cur) {
			if (saddle == null || cur.elev < saddle.elev) {
				saddle = cur;
			}
		}
	}
	
	static class Front {
		PriorityQueue<Point> queue;
		Set<Point> set;
		
		public Front() {
			queue = new PriorityQueue<Point>(10, new Comparator<Point>() {
				@Override
				public int compare(Point p0, Point p1) {
					return Double.compare(p1.elev, p0.elev);
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
				for (Point adj : f.adjacent) {
					frontAdj.add(adj);
				}
			}
			return frontAdj;
		}
		
		public int size() {
			return set.size();
		}
	}
	
	public static PromInfo prominence(Point p) {
		PromInfo pi = new PromInfo(p);
		Front front = new Front();
		front.add(p);
		Set<Point> seen = new HashSet<Point>();
		int seenPruneThreshold = 1;
		
		outer:
		while (true) {
			Point cur = front.next();
			if (cur == null) {
				// we've searched the whole world
				pi.global_max = true;
				break;
			}
			pi.add(cur);
			if (cur.elev > p.elev) {
				break;
			}

			seen.add(cur);

			for (Point adj : cur.adjacent) {
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
			if (seen.size() > seenPruneThreshold) {
				seen.retainAll(front.adjacent());
				seenPruneThreshold = 2 * seen.size();
			}
		}
		
		return pi;
	}
	
	public static double subsidence(Point p) {
		return -1;
	}

}
