package promviz;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;

public class PagedMesh implements IMesh {

	int maxPoints;
	Map<Long, Point> points;
	Queue<Prefix> loadedSegments;
	
	public PagedMesh(int maxPoints) {
		this.maxPoints = maxPoints;
		points = new HashMap<Long, Point>();
		loadedSegments = new LinkedList<Prefix>();
	}
	
	public Point get(long ix) {
		return points.get(ix);
	}
	
	public void loadPage(Prefix prefix, Collection<Point> newPoints) {
		while (points.size() + newPoints.size() > maxPoints) {
			removeOldestPage();
		}
		loadedSegments.add(prefix);
		for (Point p : newPoints) {
			points.put(p.geocode, p);
		}
		Logging.log(String.format("%d total points in mesh", points.size()));
		
	}
	
	public void removeOldestPage() {
		Prefix oldest = loadedSegments.remove();
		Logging.log("booting " + oldest);
		Iterator<Long> it = points.keySet().iterator();
		while (it.hasNext()) {
			long ix = it.next();
			if (oldest.isParent(ix)) {
				it.remove();
			}
		}
	}
	
}
