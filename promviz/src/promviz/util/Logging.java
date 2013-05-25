package promviz.util;

public class Logging {
	
	static long start;
	
	public static void init() {
		 start = System.currentTimeMillis();
	}
	
	static double clock() {
		return (System.currentTimeMillis() - start) / 1000.;
	}
	
	public static void log(String msg) {
		System.err.println(String.format("%07.2f %s", clock(), msg));
	}
}
