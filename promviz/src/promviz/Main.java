package promviz;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import promviz.dem.DEMFile;
import promviz.dem.Projection;
import promviz.dem.SRTMDEM;
import promviz.util.Logging;

public class Main {

	static int NUM_WORKERS;
	
	static Properties props;
	
	public static void initProps() {
		props = new Properties();
		try {
			props.load(ClassLoader.getSystemResourceAsStream("config.properties"));
		} catch (IOException e) {
			throw new RuntimeException();
		}		
	}
	
	public static List<DEMFile> loadDEMs(String region) {
		List<DEMFile> dems = new ArrayList<DEMFile>();
		try {
	        Process proc = new ProcessBuilder(new String[] {"python", "demregion.py", region}).start();
	        BufferedReader stdin = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	        
	        String s = null;
	        while ((s = stdin.readLine()) != null) {
	        	String[] parts = s.split(",");
	        	String type = parts[0];
	        	String path = parts[1];
	        	int w = Integer.parseInt(parts[2]);
	        	int h = Integer.parseInt(parts[3]);
	        	double lat = Double.parseDouble(parts[4]);
	        	double lon = Double.parseDouble(parts[5]);
	        	int res = Integer.parseInt(parts[6]);
	        	
	        	DEMFile dem = new SRTMDEM(path, w, h, lat, lon, res);
	        	dems.add(dem);
	        }
	    } catch (IOException e) {
	        throw new RuntimeException();
	    }		
		return dems;
	}

	public static void main(String[] args) {
		Logging.init();
		initProps();

		NUM_WORKERS = Integer.parseInt(args[0]);
		String action = args[1];
		String region = args[2];

		List<DEMFile> DEMs = loadDEMs(region);
		final Map<Integer, Projection> projs = new HashMap<Integer, Projection>();
		projs.put(0, DEMs.get(0).proj);
		Projection.authority = new Projection.Authority() {
			public Projection forRef(int refID) {
				return projs.get(refID);
			}
		};
		
		if (action.equals("--topobuild")) {
			TopologyBuilder.buildTopology(DEMs);
			
		} else if (action.equals("--searchup") || action.equals("--searchdown")) {
			boolean up = action.endsWith("up");
			double cutoff = Double.parseDouble(args[3]);
			Prominence.promSearch(DEMs, up, cutoff);
//			ProminenceRef.promSearch(DEMs, up, cutoff);
		}
	}
	

}
