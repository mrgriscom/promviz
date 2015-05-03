package promviz.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Util {

	public static int pow2(int exp) {
		assert exp >= 0 && exp < 31;
		return (1 << exp);
	}
	
	public static boolean overflowHigh(int n, int bits) {
		return n >= pow2(bits - 1);
	}

	public static boolean overflowLow(int n, int bits) {
		return n < -pow2(bits - 1);
	}
	
	public static boolean inSignedRange(int n, int bits) {
		return !overflowLow(n, bits) && !overflowHigh(n, bits);
	}

	public static int toSignedRange(int n, int bits) {
		return n - (overflowHigh(n, bits) ? pow2(bits) : 0);
	}
	
	public static String print(long L) {
		return String.format("%016x", L);
	}

	public static int mod(int a, int b) {
		return ((a % b) + b) % b;
	}

	static <E> Iterable<Iterable<E>> chunker(final Iterator<E> stream, final long size) {
		assert size > 0;
		
		return new SaneIterable<Iterable<E>>() {
			public Iterable<E> genNext() {
				if (stream.hasNext()) {
					return new SaneIterable<E>() {
						int i = 0;
						
						public E genNext() {
							if (stream.hasNext() && i < size) {
								return stream.next();
							} else {
								throw new NoSuchElementException();
							}
						}
					};
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}
	
}
