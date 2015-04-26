package promviz.debug;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.Edge;
import promviz.FileUtil;
import promviz.GridPoint;
import promviz.IMesh;
import promviz.MeshPoint;
import promviz.PagedElevGrid;
import promviz.Point;
import promviz.PointIndex;
import promviz.Prefix;
import promviz.debug.PromNetwork.PromInfo;
import promviz.dem.DEMFile;
import promviz.dem.DEMFile.Sample;
import promviz.util.GeoCode;
import promviz.util.Logging;
import promviz.util.Util;

import com.google.gson.Gson;

public class Harness {

	
	///////// HERE BE DRAGONS

	
	static class TopologyNetwork implements IMesh {
		PagedElevGrid mesh;
		Iterable<Sample> samples;
		boolean up;
		
		Map<Long, MeshPoint> points;
		Set<MeshPoint> pendingSaddles;
		
		public TopologyNetwork(boolean up, Map<Prefix, Set<DEMFile>> coverage) {
			this.up = up;
			this.mesh = new PagedElevGrid(coverage, 1 << 29);
			this.samples = this.mesh.bulkLoadPage(coverage.keySet());
			
			points = new HashMap<Long, MeshPoint>();
			pendingSaddles = new HashSet<MeshPoint>();
			
			load();
		}

		void load() {
			String path = FileUtil.dir(FileUtil.PHASE_RAW);
			File folder = new File(path);
			for (File f : folder.listFiles()) {
				if (!f.getPath().contains((up ? "U" : "D") + "-")) {
					continue;
				}
				loadSegment(f.getPath());
			}
		}

		void loadSegment(String path) {
			int i = 0;
			for (Edge e : FileUtil.loadEdges(path)) {
				addPoint(e.a);
				addPoint(e.saddle);
				get(e.a).adjAdd(e.saddle, e.tagA, true);
				get(e.saddle).adjAdd(e.a, e.tagA, false);
				if (e.pending()) {
					pendingSaddles.add(get(e.saddle));
				} else {
					addPoint(e.b);
					get(e.b).adjAdd(e.saddle, e.tagB, true);
					get(e.saddle).adjAdd(e.b, e.tagB, false);
				}
			}
		}

		void addPoint(long ix) {
			if (!points.containsKey(ix)) {
				points.put(ix, new MeshPoint(ix, mesh.get(ix).elev));
			}
		}
		
		@Override
		public MeshPoint get(long ix) {
			return points.get(ix);
		}
		
		Set<MeshPoint> adjacent(MeshPoint p) {
			return new HashSet<MeshPoint>(getPoint(p).adjacent(this));
		}
		
		public MeshPoint getHighest() {
			MeshPoint highest = null;
			Comparator<Point> cmp = Point.cmpElev(up);
			for (DEMFile.Sample s : this.samples) {
				MeshPoint p = new GridPoint(s);
				if (highest == null || cmp.compare(p, highest) > 0) {
					highest = p;
				}					
			}
			return highest;
		}
		
		MeshPoint getPoint(Point p) {
			return get(p.ix);
		}
	}
	
	
	//static boolean oldSchool = true;
	static boolean oldSchool = false;
	public static void promSearch(List<DEMFile> DEMs, final boolean up, double cutoff) {
		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		
		TopologyNetwork tn = new TopologyNetwork(up, coverage);
		
		if (oldSchool) {
			
//			for (Point p : tn.allPoints()) {
//				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
//					continue;
//				}
//				
//				PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
//				if (pi != null && pi.prominence() >= cutoff) {
//					outputPromPoint(pi, up);
//				}
//			}
			
		} else {
			
			
			
			MeshPoint highest = tn.getHighest();
			Logging.log("highest: " + highest);
			
//			final DataOutputStream promOut;
//			final DataOutputStream thresholdsOut;
//			try {
//				promOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/prom-" + (up ? "up" : "down"))));
//				thresholdsOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/thresh-" + (up ? "up" : "down"))));
//			} catch (IOException ioe) {
//				throw new RuntimeException();
//			}
			PromNetwork.bigOlPromSearch(up, highest, tn, new OnProm() {
				public void onprom(PromInfo pi) {
					outputPromPoint(pi, up);
					
					// note: if we ever have varying cutoffs this will take extra care
					// to make sure we always capture the parent/next highest peaks for
					// peaks right on the edge of the lower-cutoff region
//					new PromMeta(pi.p.ix, pi.prominence(), pi.saddle.ix, pi.forward).write(promOut);
//					new PromMeta(pi.saddle.ix, pi.prominence(), pi.p.ix, pi.forward).write(promOut);
					
//					new ThresholdMeta(pi.path.get(0)).write(thresholdsOut);
				}
			}, null, cutoff);
//			try {
//				promOut.close();
//				thresholdsOut.close();
//			} catch (IOException ioe) {
//				throw new RuntimeException();
//			}
			
//			processMST(highest, dm, up, null);
		}
	}

	
	public static interface OnProm {
		void onprom(PromNetwork.PromInfo pi);
	}
//	public static interface OnPromParent {
//		void onparent(PromNetwork.ParentInfo pi);
//	}
	
	
	static void outputPromPoint(PromNetwork.PromInfo pi, boolean up) {
		Gson ser = new Gson();
		
		PromNetwork.PromInfo parentfill = new PromNetwork.PromInfo(pi.p, pi.c);
		parentfill.saddle = pi.saddle;
		parentfill.path = new ArrayList<Long>();
		parentfill.path.add(pi.p.ix);
		parentfill.path.add(pi.saddle.ix);
		parentfill.min_bound_only = true;
		PromNetwork.PromInfo parentage = parentfill; //PromNetwork.parent(tn, p, up, prominentPoints);
		
		List<Point> domainSaddles = null; //PromNetwork.domainSaddles(tn, p, saddleIndex, (float)pi.prominence());
//				List<String> domainLimits = new ArrayList<String>();
//				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
//					domainLimits.add(pathToStr(ro));					
//				}
		//domainSaddles.remove(pi.saddle);
		
		System.out.println(ser.toJson(new PromData(
				up, pi.p, pi, parentage, domainSaddles
			)));
	}
	
	static void outputPromParentage(PromNetwork.PromInfo pi, boolean up) {
		Gson ser = new Gson();
		
		if (pi.min_bound_only || pi.path.isEmpty()) {
			return;
		}
		
		System.out.println(ser.toJson(new ParentData(up, pi.p, pi)));
	}

	static void outputSubsaddles(Point p, List<Integer/*DomainSaddleInfo*/> dsi, boolean up) {
		if (dsi.isEmpty()) {
			return;
		}
		Gson ser = new Gson();
		System.out.println(ser.toJson(new SubsaddleData(up, p, dsi)));
	}

	static void outputPromThresh(PrintWriter w, PromNetwork.PromInfo pi, boolean up) {
		if (pi.min_bound_only) {
			return;
		}
		w.write(new PromPoint(pi.p.ix).geo + " " + new PromPoint(pi.path.get(0)).geo + "\n");
	}

	
	static class PromPoint {
		double coords[];
		String geo;
		double elev;
		double prom;
		
		public PromPoint(Point p, PromNetwork.PromInfo pi) {
			this.coords = PointIndex.toLatLon(p.ix);
			this.geo = Util.print(GeoCode.fromCoord(this.coords[0], this.coords[1]));
			this.elev = p.elev;
			if (pi != null) {
				prom = pi.prominence();
			}
		}
		
		public PromPoint(long ix) {
			this.coords = PointIndex.toLatLon(ix);
			this.geo = Util.print(GeoCode.fromCoord(this.coords[0], this.coords[1]));			
		}
		
		public PromPoint(double[] c) {
			this.coords = c;
			this.geo = Util.print(GeoCode.fromCoord(this.coords[0], this.coords[1]));			
		}
	}
	
	static class PromData {
		boolean up;
		PromPoint summit;
		PromPoint saddle;
		boolean min_bound;
		PromPoint higher;
		PromPoint parent;
		List<double[]> higher_path;
		List<double[]> parent_path;
		//List<PromPoint> secondary_saddles;
		PromPoint _thresh;
		
		public PromData(boolean up, Point p, PromNetwork.PromInfo pi, PromNetwork.PromInfo parentage,
				List<Point> domainSaddles) {
			this.up = up;
			this.summit = new PromPoint(p, pi);
			this.saddle = new PromPoint(pi.saddle, null);
			this.min_bound = pi.min_bound_only;
		
			/*
			this.secondary_saddles = new ArrayList<PromPoint>();
			for (Point ss : domainSaddles) {
				this.secondary_saddles.add(new PromPoint(ss, null));
			}
			*/
			
			this.higher_path = new ArrayList<double[]>();
			for (long k : pi.path) {
				this.higher_path.add(PointIndex.toLatLon(k));
			}
			if (pi.thresholdFactor >= 0) {
				// this is a stop-gap; actually threshold should be determined by following chase from saddle
				double[] last = this.higher_path.get(0);
				double[] nextToLast = this.higher_path.get(1);
				for (int i = 0; i < 2; i++) {
					last[i] = last[i] * pi.thresholdFactor + nextToLast[i] * (1. - pi.thresholdFactor);
				}
			}
			this._thresh = new PromPoint(pi.path.get(0));
			
			this.parent_path = new ArrayList<double[]>();
			for (long k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k));
			}
			
			if (!pi.min_bound_only && !pi.path.isEmpty()) {
				this.higher = new PromPoint(this.higher_path.get(0));
			}
			if (!parentage.min_bound_only && !parentage.path.isEmpty()) {
				this.parent = new PromPoint(parentage.path.get(0));
			}
		}
	}
	
	static class ParentData {
		boolean up;
		PromPoint summit;
		PromPoint parent;
		List<double[]> parent_path;
		String addendum = "parent";
		
		public ParentData(boolean up, Point p, PromNetwork.PromInfo parentage) {
			if (parentage.min_bound_only) {
				throw new IllegalArgumentException();
			}
			
			this.up = up;
			this.summit = new PromPoint(p.ix);

			this.parent_path = new ArrayList<double[]>();
			for (long k : parentage.path) {
				this.parent_path.add(PointIndex.toLatLon(k));
			}
			this.parent = new PromPoint(parentage.path.get(0));
		}
	}
	
	static class SubsaddleData {
		boolean up;
		PromPoint summit;
		String addendum = "subsaddles";

		static class Subsaddle {
			PromPoint saddle;
			PromPoint peak;
			boolean higher;
			boolean domain;
		}
		List<Subsaddle> subsaddles;
		
		public SubsaddleData(boolean up, Point p, List<Integer/*DomainSaddleInfo*/> dsis) {
			this.up = up;
			this.summit = new PromPoint(p.ix);

			this.subsaddles = new ArrayList<Subsaddle>();
//			for (DomainSaddleInfo dsi : dsis) {
//				Subsaddle SS = new Subsaddle();
//				SS.saddle = new PromPoint((Point)dsi.saddle, null);
//				SS.peak = new PromPoint(dsi.peakIx);
//				SS.higher = dsi.isHigher;
//				SS.domain = dsi.isDomain;
//				this.subsaddles.add(SS);
//			}
		}
	}
	

	
	
}
