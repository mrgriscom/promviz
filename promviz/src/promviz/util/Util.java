package promviz.util;

public class Util {

	public static String print(long L) {
		return String.format("%016x", L);
	}

	public static int mod(int a, int b) {
		return ((a % b) + b) % b;
	}

}
