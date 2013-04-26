import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.LittleEndianDataInputStream;


public class LoadDEM {

	public static Mesh load(String prefix) throws IOException {
//		BufferedReader wf = new BufferedReader(new FileReader(prefix + ".tfw"));
//		List<String> lines = new ArrayList<String>();
//		while (true) {
//			String line = wf.readLine();
//			if (line == null) {
//				break;
//			}
//			lines.add(line);
//        }

		double LON0 = -75;
		double LAT0 = 40;
		double dx = 3 /*1*/ / 1200f;
		double dy = dx;
		int WIDTH = 2001; //6001
		int HEIGHT = 2001; //6001

		Mesh m = new Mesh();
		Point[][] meshBuffer = new Point[3][WIDTH];
		
		LittleEndianDataInputStream f = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream("/tmp/data2")));
		for (int row = 0; row < HEIGHT; row++) {
			for (int col = 0; col < WIDTH; col++) {
				double lon = LON0 + dx * col;
				double lat = LAT0 + dy * (HEIGHT - 1 - row);
				double elev = f.readShort();
				Point p;
				if (elev == -32768) {
					elev = 0; // cgiar has voids filled so nodata is actually ocean
				}
				p = new Point(lat, lon, elev);
				m.points.add(p);
				meshBuffer[row % 3][col] = p;
			}
			
			processAdjacency(meshBuffer, row, HEIGHT);
		}
		processAdjacency(meshBuffer, HEIGHT, HEIGHT);
		
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
		for (int[] offset : offsets) {
			if ((r + c) % 2 == 0 || (offset[0] + offset[1] + 2) % 2 == 0) {
				adj.add(new int[] {c + offset[0], r + offset[1]});
			}
		}
		return adj;
	}
	
	public static void main(String[] args) {
		
		Mesh m;
		try {
			m = load("asdf");
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}

		for (Point p : m.points) {
			Prominence.PromInfo pi = Prominence.prominence(p);
			if (pi.prominence() > 50.) {
				System.out.println(String.format(
						"{\"summit\": [%.5f, %.5f], \"elev\": %.1f, \"prom\": %.1f, \"saddle\": [%.5f, %.5f], \"min_bound\": %s}",
						p.lat, p.lon, p.elev, pi.prominence(), pi.saddle.lat, pi.saddle.lon, pi.min_bound_only ? "true" : "false"));
			}
		}
	}
}
