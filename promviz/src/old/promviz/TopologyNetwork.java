package old.promviz;
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

import old.promviz.Point.Lead;
import old.promviz.util.Logging;
import promviz.IMesh;
import promviz.PagedElevGrid;
import promviz.Prefix;
import promviz.util.DefaultMap;

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
		getPoint(from).adjAdd(to.ix);
	}

	void addPending(Lead lead) {
		lead.p0 = getPoint(lead.p0); // cargo culting?
		pendingLeads.add(lead);
		pendingSaddles.add(lead.p0);
	}
	
	
	public void buildPartial(PagedElevGrid m, Iterable<DEMFile.Sample> newPage) {
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
	
		
	Set<Point> adjacent(Point p) {
		Point match = getPoint(p);
		Set<Point> adj = new HashSet<Point>();
		for (long l : match._adjacent) {
			adj.add(points.get(l));
		}
		return adj;
	}
	
	public PreprocessNetwork.Meta getMeta(BasePoint p, String type) {
		throw new RuntimeException("not implemented");
	}
}
