package promviz.debug;

import java.util.ArrayList;
import java.util.List;

import promviz.Point;
import promviz.PointIndex;
import promviz.Prominence;
import promviz.Prominence.PromBaseInfo;
import promviz.Prominence.PromFact;
import promviz.Prominence.PromParent;
import promviz.Prominence.PromPending;
import promviz.Prominence.PromSubsaddle;
import promviz.Prominence.PromThresh;
import promviz.util.GeoCode;
import promviz.util.Util;

import com.google.gson.Gson;

public class Harness {
	
	public static void outputPromInfo(boolean up, long pIx, List<PromFact> pfs) {
		Gson ser = new Gson();
		System.out.println(ser.toJson(new PromInfo(up, pIx, pfs)));
	}
	
	static class PromInfo {
		String key;
		boolean up;
		List<PromFactInfo> facts;
		
		public PromInfo(boolean up, long pix, List<PromFact> pfs) {
			this.up = up;
			key = PointIndex.geocode(pix);
			facts = new ArrayList<PromFactInfo>();
			for (PromFact pf : pfs) {
				facts.add(packageFact(pf));
			}
		}
		
		PromFactInfo packageFact(PromFact pf) {
			if (pf instanceof PromBaseInfo) {
				return new PFBaseInfo((PromBaseInfo)pf);
			} else if (pf instanceof PromPending) {
				return new PFBaseInfo((PromPending)pf);
			} else if (pf instanceof PromThresh) {
				return new PFPThreshInfo((PromThresh)pf);
			} else if (pf instanceof PromParent) {
				return new PFParentInfo((PromParent)pf);
			} else if (pf instanceof PromSubsaddle) {
				return new PFSubsaddleInfo((PromSubsaddle)pf);
			}
			throw new RuntimeException();
		}
	}
	
	static abstract class PromFactInfo {}
	
	static class PFBaseInfo extends PromFactInfo {
		PromPoint summit;
		PromPoint saddle;
		boolean min_bound;
		PromPoint thresh;
		List<double[]> path;

		public PFBaseInfo(PromBaseInfo i) {
			load(i.p, i.saddle, i.thresh, i.path, i.thresholdFactor);
		}
		
		public PFBaseInfo(PromPending i) {
			load(i.p, i.pendingSaddle, null, i.path, -1);
		}
		
		void load(Point p, Point saddle, Point thresh, List<Long> path, double thresholdFactor) {
			this.summit = new PromPoint(p, saddle);
			this.saddle = new PromPoint(saddle);
			if (thresh != null) {
				this.thresh = new PromPoint(thresh);
			} else {
				this.min_bound = true;
			}
			this.path = makePath(path, thresholdFactor);
			if (thresh != null && thresholdFactor >= 0) {
				this.thresh = new PromPoint(this.path.get(0));
			}
		}
	}
	
	static class PFPThreshInfo extends PromFactInfo {
		PromPoint pthresh;
		
		public PFPThreshInfo(PromThresh i) {
			this.pthresh = new PromPoint(i.pthresh.ix);
		}
	}

	static class PFParentInfo extends PromFactInfo {
		PromPoint parent;
		List<double[]> path;
		
		public PFParentInfo(PromParent i) {
			this.parent = new PromPoint(i.parent);
			this.path = makePath(i.path, -1);
		}
	}
	
	static List<double[]> makePath(List<Long> ixpath, double thresholdFactor) {
		List<double[]> cpath = new ArrayList<double[]>();
		for (long ix : ixpath) {
			cpath.add(PointIndex.toLatLon(ix));
		}
		if (thresholdFactor >= 0) {
			// this is a stop-gap; actually threshold should be determined by following chase from saddle
			double[] last = cpath.get(0);
			double[] nextToLast = cpath.get(1);
			for (int i = 0; i < 2; i++) {
				last[i] = last[i] * thresholdFactor + nextToLast[i] * (1. - thresholdFactor);
			}
		}
		return cpath;
	}

	static class PFSubsaddleInfo extends PromFactInfo {
		PromPoint saddle;
		PromPoint forPeak;
		boolean isDomain;
		boolean subsaddle = true; // for tagging

		public PFSubsaddleInfo(PromSubsaddle i) {
			saddle = new PromPoint(i.subsaddle.saddle);
			forPeak = new PromPoint(i.subsaddle.peak);
			isDomain = (i.type == PromSubsaddle.TYPE_PROM);
		}
	}
	
	static class PromPoint {
		double coords[];
		String geo;
		double elev;
		double prom;
		
		public PromPoint(Point p) {
			this(p.ix);
			this.elev = p.elev;
		}

		public PromPoint(Point p, Point saddle) {
			this(p);
			this.prom = Prominence.prominence(p, saddle);
		}
		
		public PromPoint(long ix) {
			this.coords = PointIndex.toLatLon(ix);
			this.geo = PointIndex.geocode(ix);
		}
		
		public PromPoint(double[] coords) {
			this.coords = coords;
			this.geo = Util.print(GeoCode.fromCoord(coords[0], coords[1]));
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
