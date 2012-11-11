import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;


public class LoadDEM {

	public static void load(String prefix) throws IOException {
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
		double dx = 1 / 1200f;
		double dy = dx;
		int WIDTH = 6001;
		int HEIGHT = 6001;
		
		DataInputStream f = new DataInputStream(new BufferedInputStream(new FileInputStream("/tmp/data")));
		System.out.println(f.readShort());
	}
	
	public static void main(String[] args) {
		
		try {
			load("asdf");
		} catch (IOException ioe) {
			
		}
		
	}
}
