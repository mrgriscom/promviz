import java.io.IOException;
import java.util.Iterator;


public class DEMFile {

	int width;
	int height;
	double lat0;
	double lon0;
	double dx;
	double dy;
	boolean sample_mode; // true == lat0/lon0 correspond to center of pixel; false == correspond to corner of pixel
	String path;
	
	public DEMFile(String path, int width, int height, double lat0, double lon0, double dx, double dy, boolean sample_mode) {
		this.path = path;
		this.width = width;
		this.height = height;
		this.lat0 = lat0;
		this.lon0 = lon0;
		this.dx = dx;
		this.dy = dy;
		this.sample_mode = sample_mode;
	}
	
	class CoordsIterator implements Iterator<Long> {
		int r;
		int c;
		
		public CoordsIterator() {
			r = 0;
			c = 0;
		}
		
		@Override
		public boolean hasNext() {
			return r < height;
		}

		@Override
		public Long next() {
			double lon = lon0 + dx * (c + (sample_mode ? 0 : 0.5));
			double lat = lat0 + dy * (height - (r + (sample_mode ? 1 : 0.5)));
			c++;
			if (c == width) {
				c = 0;
				r++;
			}
			return GeoCode.fromCoord(lat, lon);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
	
	
	public static Mesh load(String path, double lat0, double lon0, double step, int width, int height) throws IOException {
		Mesh m = new Mesh();
//		Point[][] meshBuffer = new Point[3][width];
//		
//		LittleEndianDataInputStream f = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
//		for (int row = 0; row < height; row++) {
//			for (int col = 0; col < width; col++) {
//				double elev = f.readShort();
//				if (elev == -32768) {
//					elev = 0; // cgiar has voids filled so nodata is actually ocean
//				}
//				//Point p = new Point(lat, lon, elev);
//				m.points.put(p.geocode, p);
//				meshBuffer[row % 3][col] = p;
//			}
//			
////			processAdjacency(meshBuffer, row, height);
//		}
////		processAdjacency(meshBuffer, height, height);
//		
//		// ensure no two adjacent points are the same exact height
//		Random r = new Random(0);
//		for (Point p : m.points.values()) {
//			p.elev += r.nextDouble() - 0.5;
//		}
//		boolean foundSameHeight;
//		do {
//			foundSameHeight = false;
//			for (Point p : m.points.values()) {
//				for (Point adj : p.adjacent(m)) {
//					if (adj != null && p.elev == adj.elev) {
//						foundSameHeight = true;
//						adj.elev += 0.1 * (r.nextDouble() - 0.5);
//					}
//				}
//			}
//		} while (foundSameHeight);
		
		return m;
	}

	
	
	
	
	
	
	
	public Iterable<Long> coords() {
		return new Iterable<Long>() {
			@Override
			public Iterator<Long> iterator() {
				return new CoordsIterator();
			}
		};
	}
	
	public Iterable<Point> samples() {
		return null;
	}
	
	public static void main(String[] args) {
		for (Long l : new DEMFile("", 20, 10, 40, -75, .01, .01, true).coords()) {
			System.out.println(GeoCode.print(GeoCode.prefix(l, 26)));
		}
	}
}
