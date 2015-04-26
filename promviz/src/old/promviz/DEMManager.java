package old.promviz;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import old.promviz.PreprocessNetwork.EdgeIterator;
import old.promviz.PreprocessNetwork.Meta;
import old.promviz.PreprocessNetwork.PromMeta;
import old.promviz.PreprocessNetwork.ThresholdMeta;
import old.promviz.PromNetwork.DomainSaddleInfo;
import old.promviz.PromNetwork.ParentInfo;
import old.promviz.PromNetwork.PromInfo;
import old.promviz.util.Util;
import promviz.PagedElevGrid;
import promviz.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;

import com.google.gson.Gson;


public class DEMManager {

	

	static void processMST(Point highest, DEMManager dm, boolean up, String region) {
		if (region != null) {
			loadDEMs(dm, region);
		}
		if (highest == null) {
			highest = getHighest(dm, up);
		}
		
		File folder = new File(DEMManager.props.getProperty("dir_mst"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/mst not empty!");
		}
		PreprocessNetwork.processMST(dm, up, highest);
		folder = new File(DEMManager.props.getProperty("dir_rmst"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/rmst not empty!");
		}
		PreprocessNetwork.processRMST(dm, up, highest);
	}
	
	static void promPass2(Point highest, DEMManager dm, final boolean up, String region) {
		MESH_MAX_POINTS = (1 << 26);
		TopologyNetwork tn = new PagedTopologyNetwork(EdgeIterator.PHASE_RMST, up, null, new Meta[] {new PromMeta()});
		
		if (oldSchool) {

			for (Point p : tn.allPoints()) {
				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
					continue;
				}

				// we know where the saddle is; would could start the search from there
				// also, using the MST requires less bookkeeping than this search function provides
				PromNetwork.PromInfo pi = PromNetwork.parent(tn, p, up);
				if (pi != null) {
					outputPromParentage(pi, up);
				}

				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
				outputSubsaddles(p, dsi, up);
			}
			
		} else {
			if (region != null) {
				loadDEMs(dm, region);
			}
			if (highest == null) {
				highest = getHighest(dm, up);
			}

			PromNetwork.bigOlPromParentSearch(up, highest, tn, new DEMManager.OnPromParent() {
				public void onparent(ParentInfo pi) {
					PromInfo _pi = new PromInfo(new Point(pi.pIx, 0), null);
					_pi.path = pi.path;
					outputPromParentage(_pi, up);
				}
			});
			
			// just use old-school method for subsaddle search
			for (Point p : tn.allPoints()) {
				if (p.classify(tn) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tn.getMeta(p, "prom") == null) {
					continue;
				}
				
				List<DomainSaddleInfo> dsi = PromNetwork.domainSaddles(up, tn, p);
				outputSubsaddles(p, dsi, up);
			}

		}
		
		// must operate on *untrimmed* MST!
		tn = null;
		TopologyNetwork tnfull = new PagedTopologyNetwork(EdgeIterator.PHASE_MST, up, null, new Meta[] {new PromMeta(), new ThresholdMeta()});
		try {
			PrintWriter w = new PrintWriter("/tmp/thresh");
			for (Point p : tnfull.allPoints()) {
				if (p.classify(tnfull) != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT) || tnfull.getMeta(p, "thresh") == null) {
					continue;
				}
				
				PromNetwork.PromInfo pi = PromNetwork.promThresh(tnfull, p, up);
				if (pi != null) {
					outputPromThresh(w, pi, up);
				}
			}
			w.close();
		} catch (IOException ioe) { throw new RuntimeException(); }

	}
	
	public static void do(String[] args) {
		
		String region = args[1];
		if (args[0].equals("--build")) {
			buildTopologyNetwork(dm, region);
			preprocessNetwork(dm, null);
		} else if (args[0].equals("--prepnet")) {
			preprocessNetwork(dm, region);
		} else if (args[0].equals("--verify")) {
			verifyNetwork();
		} else if (args[0].equals("--searchup") || args[0].equals("--searchdown")) {
			boolean up = args[0].equals("--searchup");
			double cutoff = Double.parseDouble(args[2]);
			promSearch(up, cutoff, dm, region);
		} else if (args[0].equals("--mstup") || args[0].equals("--mstdown")) {
			boolean up = args[0].equals("--mstup");
			processMST(null, dm, up, region);
		} else if (args[0].equals("--searchup2") || args[0].equals("--searchdown2")) {
			boolean up = args[0].equals("--searchup2");
			promPass2(null, dm, up, region);
		} else {
			throw new RuntimeException("operation not specified");
		}
		
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n42w073/floatn42w073_13.flt",
//				10812, 10812, 40.9994444, -73.0005555, 9.259259e-5, 9.259259e-5, true));
//		dm.DEMs.add(new GridFloatDEM("/mnt/ext/gis/tmp/ned/n45w072/floatn45w072_13.flt",
//				10812, 10812, 43.9994444, -72.0005555, 9.259259e-5, 9.259259e-5, true));
		
		Logging.log("java out");
	}
	
}
