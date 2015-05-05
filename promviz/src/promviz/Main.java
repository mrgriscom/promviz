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

	static final int NUM_WORKERS = 3;
	
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
	        Process proc = new ProcessBuilder(new String[] {"python", "/home/drew/dev/pv2/script/demregion.py", region}).start();
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
		
		List<DEMFile> DEMs = loadDEMs(args[0]);
		final Map<Integer, Projection> projs = new HashMap<Integer, Projection>();
		projs.put(0, DEMs.get(0).proj);
		Projection.authority = new Projection.Authority() {
			public Projection forRef(int refID) {
				return projs.get(refID);
			}
		};
		
//		TopologyBuilder.buildTopology(DEMs);
		
		final double PROM_CUTOFF_UP = 20.;
		final double PROM_CUTOFF_DOWN = 20.;
		FileUtil.ensureEmpty(FileUtil.PHASE_PROMTMP);
		FileUtil.ensureEmpty(FileUtil.PHASE_MST);
		Prominence.promSearch(DEMs, true, PROM_CUTOFF_UP);
//		Prominence.promSearch(DEMs, false, PROM_CUTOFF_DOWN);
	}
	

}
