import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.io.LittleEndianDataInputStream;


public class LoadDEM {

	public static Mesh load(String path, double lat0, double lon0, double step, int width, int height) throws IOException {
//		BufferedReader wf = new BufferedReader(new FileReader(prefix + ".tfw"));
//		List<String> lines = new ArrayList<String>();
//		while (true) {
//			String line = wf.readLine();
//			if (line == null) {
//				break;
//			}
//			lines.add(line);
//        }

		double dx = step;
		double dy = step;
		
		Mesh m = new Mesh();
		Point[][] meshBuffer = new Point[3][width];
		
		LittleEndianDataInputStream f = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
		Random r = new Random(0);
		for (int row = 0; row < height; row++) {
			for (int col = 0; col < width; col++) {
				double lon = lon0 + dx * col;
				double lat = lat0 + dy * (height - 1 - row);
				double elev = f.readShort();
				if (elev == -32768) {
					elev = 0; // cgiar has voids filled so nodata is actually ocean
				}
				elev += r.nextDouble() - 0.5;
				Point p = new Point(lat, lon, elev);
				m.points.add(p);
				meshBuffer[row % 3][col] = p;
			}
			
			processAdjacency(meshBuffer, row, height);
		}
		processAdjacency(meshBuffer, height, height);
		
		return m;
	}
	
	public static void processAdjacency(Point[][] meshBuffer, int row, int height) {
		row -= 1;
		if (row < 0) {
			return;
		}
		int width = meshBuffer[0].length;
		
		for (int col = 0; col < width; col++) {
			Point active = meshBuffer[row % 3][col];
			if (active == null) {
				continue;
			}
			
			List<int[]> adjIx = adjacency(row, col);
			Point[] adjPt = new Point[adjIx.size()];
			for (int i = 0; i < adjPt.length; i++) {
				int[] ix = adjIx.get(i);
				int c = ix[0];
				int r = ix[1];
				Point p;
				if (c < 0 || c >= width || r < 0 || r >= height) {
					p = null;
				} else {
					p = meshBuffer[r % 3][c];
				}
				adjPt[i] = p;
			}
			active.adjacent = adjPt;
		}
	}
	
	public static List<int[]> adjacency(int r, int c) {
		int[][] offsets = {
				{0, 1},
				{1, 1},
				{1, 0},
				{1, -1},
				{0, -1},
				{-1, -1},
				{-1, 0},
				{-1, 1},
			};
		List<int[]> adj = new ArrayList<int[]>();
		boolean fully_connected = (r + c) % 2 == 0;
		for (int[] offset : offsets) {
			boolean diagonal_connection = (offset[0] + offset[1] + 2) % 2 == 0;
			if (fully_connected || !diagonal_connection) {
				adj.add(new int[] {c + offset[0], r + offset[1]});
			}
		}
		return adj;
	}
	
	public static void main(String[] args) {
		
		Mesh m;
		double lat0 = Double.parseDouble(args[1]);
		double lon0 = Double.parseDouble(args[2]);
		double step = Double.parseDouble(args[3]);
		int width = Integer.parseInt(args[4]);
		int height = Integer.parseInt(args[5]);
		boolean up = !(args.length > 6 && args[6].equals("sub")); 
		try {
			m = load(args[0], lat0, lon0, step, width, height);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		TopologyNetwork tn = new TopologyNetwork(m, up);
		tn.build();
		
		for (Point p : m.points) {
			if (p.classify() != (up ? Point.CLASS_SUMMIT : Point.CLASS_PIT)) {
				continue;
			}
			
			double PROM_CUTOFF = 50.;
			PromNetwork.PromInfo pi = PromNetwork.prominence(tn, p, up);
			if (pi != null && pi.prominence() > PROM_CUTOFF) {
				StringBuilder path = new StringBuilder();
				for (int i = 0; i < pi.path.size(); i++) {
					path.append(String.format("[%f, %f]", pi.path.get(i).lat, pi.path.get(i).lon) + (i < pi.path.size() - 1 ? ", " : ""));
				}
				System.out.println(String.format(
						"{\"summit\": [%.5f, %.5f], \"elev\": %.1f, \"prom\": %.1f, \"saddle\": [%.5f, %.5f], \"min_bound\": %s, \"path\": [%s]}",
						p.lat, p.lon, p.elev, pi.prominence(), pi.saddle.lat, pi.saddle.lon, pi.min_bound_only ? "true" : "false", path.toString()));
			}
		}
	}
}
