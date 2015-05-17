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
import promviz.Prominence.OnProm;
import promviz.Prominence.PromInfo;
import promviz.dem.DEMFile;
import promviz.dem.DEMFile.Sample;
import promviz.util.GeoCode;
import promviz.util.Logging;
import promviz.util.Util;

import com.google.gson.Gson;

public class Harness {

	
	///////// HERE BE DRAGONS

	
	
	
//	//static boolean oldSchool = true;
//	static boolean oldSchool = false;
//	public static void promSearch(List<DEMFile> DEMs, final boolean up, double cutoff) {
//		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
//		
//		TopologyNetwork tn = new TopologyNetwork(up, coverage);
//		
//		if (oldSchool) {
//			
////			for (Point p : tn.allPoints()) {
////				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
////					continue;
////				}
////				
////				PromInfo pi = PromNetwork.prominence(tn, p, up);
////				if (pi != null && pi.prominence() >= cutoff) {
////					outputPromPoint(pi, up);
////				}
////			}
//			
//		} else {
//			
//			
//			
//			MeshPoint highest = tn.getHighest();
//			Logging.log("highest: " + highest);
//			
////			final DataOutputStream promOut;
////			final DataOutputStream thresholdsOut;
////			try {
////				promOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/prom-" + (up ? "up" : "down"))));
////				thresholdsOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/thresh-" + (up ? "up" : "down"))));
////			} catch (IOException ioe) {
////				throw new RuntimeException();
////			}
//			PromNetwork.bigOlPromSearch(up, highest, tn, new OnProm() {
//				public void onprom(PromInfo pi) {
//					outputPromPoint(pi, up);
//					
//					// note: if we ever have varying cutoffs this will take extra care
//					// to make sure we always capture the parent/next highest peaks for
//					// peaks right on the edge of the lower-cutoff region
////					new PromMeta(pi.p.ix, pi.prominence(), pi.saddle.ix, pi.forward).write(promOut);
////					new PromMeta(pi.saddle.ix, pi.prominence(), pi.p.ix, pi.forward).write(promOut);
//					
////					new ThresholdMeta(pi.path.get(0)).write(thresholdsOut);
//				}
//			}, null, cutoff);
////			try {
////				promOut.close();
////				thresholdsOut.close();
////			} catch (IOException ioe) {
////				throw new RuntimeException();
////			}
//			
////			processMST(highest, dm, up, null);
//		}
//	}

	
//	public static interface OnPromParent {
//		void onparent(PromNetwork.ParentInfo pi);
//	}
	
	
	public static void outputPromPoint(PromInfo pi) {
		Gson ser = new Gson();
		
		PromInfo parentfill = new PromInfo(pi.up, pi.p, pi.saddle);
		parentfill._finalizeDumb();
		parentfill.min_bound_only = true;
		PromInfo parentage = parentfill; //PromNetwork.parent(tn, p, up, prominentPoints);
		
		List<Point> domainSaddles = null; //PromNetwork.domainSaddles(tn, p, saddleIndex, (float)pi.prominence());
//				List<String> domainLimits = new ArrayList<String>();
//				for (List<Point> ro : PromNetwork.runoff(anti_tn, pi.saddle, up)) {
//					domainLimits.add(pathToStr(ro));					
//				}
		//domainSaddles.remove(pi.saddle);
		
		System.out.println(ser.toJson(new PromData(
				pi.up, pi.p, pi, parentage, domainSaddles
			)));
	}
	
	public static void outputPThresh(boolean up, long peak, long pthresh) {
		Gson ser = new Gson();
		System.out.println(ser.toJson(new PThreshData(
				up, peak, pthresh
			)));		
	}
	
	static void outputPromParentage(PromInfo pi, boolean up) {
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

	static void outputPromThresh(PrintWriter w, PromInfo pi, boolean up) {
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
		
		public PromPoint(Point p, PromInfo pi) {
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
		
		public PromData(boolean up, Point p, PromInfo pi, PromInfo parentage,
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
			if (pi.pthresh != null) {
				this._thresh = new PromPoint(pi.pthresh.ix);
			}
			
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
	
	static class PThreshData {
		boolean up;
		PromPoint summit;
		PromPoint pthresh;
		String addendum = "pthresh";
		
		public PThreshData(boolean up, long pix, long threshix) {
			this.up = up;
			this.summit = new PromPoint(pix);
			this.pthresh = new PromPoint(threshix);
		}
	}
	
	static class ParentData {
		boolean up;
		PromPoint summit;
		PromPoint parent;
		List<double[]> parent_path;
		String addendum = "parent";
		
		public ParentData(boolean up, Point p, PromInfo parentage) {
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
	

//
//	static void processMST(Point highest, DEMManager dm, boolean up, String region) {
//		if (region != null) {
//			loadDEMs(dm, region);
//		}
//		if (highest == null) {
//			highest = getHighest(dm, up);
//		}
//		
//		File folder = new File(DEMManager.props.getProperty("dir_mst"));
//		if (folder.listFiles().length != 0) {
//			throw new RuntimeException("/mst not empty!");
//		}
//		PreprocessNetwork.processMST(dm, up, highest);
//		folder = new File(DEMManager.props.getProperty("dir_rmst"));
//		if (folder.listFiles().length != 0) {
//			throw new RuntimeException("/rmst not empty!");
//		}
//		PreprocessNetwork.processRMST(dm, up, highest);
//	}
//	
//	static void promPass2(Point highest, DEMManager dm, final boolean up, String region) {
//		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_RMST, up, null, new Meta[] {new PromMeta()});
//		
//		if (oldSchool) {
//
//			for (Point p : tn.allPoints()) {
//				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
//					continue;
//				}
//
//				// we know where the saddle is; would could start the search from there
//				// also, using the MST requires less bookkeeping than this search function provides
//				PromInfo pi = PromNetwork.parent(tn, p, up);
//				if (pi != null) {
//					outputPromParentage(pi, up);
//				}
//
//				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
//				outputSubsaddles(p, dsi, up);
//			}
//			
//		} else {
//			if (region != null) {
//				loadDEMs(dm, region);
//			}
//			if (highest == null) {
//				highest = getHighest(dm, up);
//			}
//
//			PromNetwork.bigOlPromParentSearch(up, highest, tn, new DEMManager.OnPromParent() {
//				public void onparent(ParentInfo pi) {
//					PromInfo _pi = new PromInfo(new Point(pi.pIx, 0), null);
//					_pi.path = pi.path;
//					outputPromParentage(_pi, up);
//				}
//			});
//			
//			// just use old-school method for subsaddle search
//			for (Point p : tn.allPoints()) {
//				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
//					continue;
//				}
//				
//				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
//				outputSubsaddles(p, dsi, up);
//			}
//
//		}
//		
//		// must operate on *untrimmed* MST!
//		tn = null;
//		TopologyNetwork tnfull = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta(), new ThresholdMeta()});
//		try {
//			PrintWriter w = new PrintWriter("/tmp/thresh");
//			for (Point p : tnfull.allPoints()) {
//				if (p.classify(tnfull) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tnfull.getMeta(p, "thresh") == null) {
//					continue;
//				}
//				
//				PromInfo pi = PromNetwork.promThresh(tnfull, p, up);
//				if (pi != null) {
//					outputPromThresh(w, pi, up);
//				}
//			}
//			w.close();
//		} catch (IOException ioe) { throw new RuntimeException(); }
//
//	}
//	
//	public static void do(String[] args) {
//		
//		String region = args[1];
//		if (args[0].equals("--build")) {
//			buildTopologyNetwork(dm, region);
//			preprocessNetwork(dm, null);
//		} else if (args[0].equals("--prepnet")) {
//			preprocessNetwork(dm, region);
//		} else if (args[0].equals("--verify")) {
//			verifyNetwork();
//		} else if (args[0].equals("--searchup") || args[0].equals("--searchdown")) {
//			boolean up = args[0].equals("--searchup");
//			double cutoff = Double.parseDouble(args[2]);
//			promSearch(up, cutoff, dm, region);
//		} else if (args[0].equals("--mstup") || args[0].equals("--mstdown")) {
//			boolean up = args[0].equals("--mstup");
//			processMST(null, dm, up, region);
//		} else if (args[0].equals("--searchup2") || args[0].equals("--searchdown2")) {
//			boolean up = args[0].equals("--searchup2");
//			promPass2(null, dm, up, region);
//		} else {
//			throw new RuntimeException("operation not specified");
//		}
//		
////		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n42w073/floatn42w073_13.flt",
////				10812, 10812, 40.9994444, -73.0005555, 9.259259e-5, 9.259259e-5, true));
////		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n45w072/floatn45w072_13.flt",
////				10812, 10812, 43.9994444, -72.0005555, 9.259259e-5, 9.259259e-5, true));
//		
//		Logging.log("java out");
//	}
//	

	
}
