
public class GeoCode {

	static class CubePos {
		final int PREC_BITS = (64 /* bits in long */ - 3 /* bits to encode face */) / 2 /* split among two dimensions */;
		final long MAX_VAL = (1L << PREC_BITS);

		int face;
		double u;
		double v;

		public CubePos(double lat, double lon) {
			double rlat = Math.toRadians(lat);
			double rlon = Math.toRadians(lon);
			
			double x = Math.cos(rlon) * Math.cos(rlat);
			double y = Math.sin(rlon) * Math.cos(rlat);
			double z = Math.sin(rlat);

			double u_, v_;
			if (Math.abs(z) > Math.abs(x) && Math.abs(z) > Math.abs(y)) {
				face = (z > 0 ? 0 : 3);
				u_ = y / z;
				v_ = -x / z;
			} else if (Math.abs(x) > Math.abs(y)) {
				face = (x > 0 ? 1 : 4);
				u_ = y / x;
				v_ = z / x;
			} else {
				face = (y > 0 ? 2 : 5);
				u_ = -x / y;
				v_ = z / y;
			}
			if (face < 3) {
				u = u_;
				v = v_;
			} else {
				u = -v_;
				v = -u_;
			}
		}
		
		public CubePos(long ix) {
			face = (int)(ix >>> (2 * PREC_BITS));
			long[] rc = deinterlace(ix, PREC_BITS);
			long r = rc[0];
			long c = rc[1];
			double x = (c + 0.5) / MAX_VAL;
			double y = (r + 0.5) / MAX_VAL;
			u = 2. * (x - 0.5);
			v = 2. * (0.5 - y);
		}
		
		static long gate(long v, long min, long max) {
			return (v < min ? min : (v >= max ? max - 1 : v));
		}
		
		static char getBit(long x, int i) {
			return (char)((x >> i) & 0x1);
		}

		static long setBit(long x, char bit, int i) {
			return x | ((long)bit << i);
		}
		
		static long interlace(long a, long b, int limit) {
			if (limit > 32) {
				throw new IllegalArgumentException();
			}
			
			long result = 0;
			for (int i = 0; i < limit; i++) {
				result = setBit(result, getBit(a, i), 2 * i + 1);
				result = setBit(result, getBit(b, i), 2 * i);
			}
			return result;
		}
		
		static long[] deinterlace(long x, int limit) {
			if (limit > 32) {
				throw new IllegalArgumentException();
			}
			
			long[] result = new long[2];
			for (int i = 0; i < limit; i++) {
				result[0] = setBit(result[0], getBit(x, 2 * i + 1), i);
				result[1] = setBit(result[1], getBit(x, 2 * i), i);
			}
			return result;			
		}
		
		long toIndex() {
			double x = 0.5 * (u + 1.);
			double y = 0.5 * (1. - v);
			
			long c = (long)Math.floor(x * (double)MAX_VAL);
			long r = (long)Math.floor(y * (double)MAX_VAL);
			c = gate(c, 0, MAX_VAL);
			r = gate(r, 0, MAX_VAL);

			return (long)face << (2 * PREC_BITS) | interlace(r, c, PREC_BITS);
		}
		
		double[] toCoords() {
			double u_, v_;
			if (face < 3) {
				u_ = u;
				v_ = v;
			} else {
				u_ = v;
				v_ = u;
			}
			
			double x, y, z;
			switch (face % 3) {
			case 0:
				x = -v_;
				y = u_;
				z = (face < 3 ? 1 : -1);
				break;
			case 1:
				x = (face < 3 ? 1 : -1);
				y = u_;
				z = v_;
				break;
			case 2:
				x = -u_;
				y = (face < 3 ? 1 : -1);
				z = v_;
				break;
			default:
				throw new RuntimeException("can't happen");
			}
			
			double lon = Math.toDegrees(Math.atan2(y, x));
			double lat = Math.toDegrees(Math.atan2(z, Math.sqrt(x*x + y*y)));
			return new double[] {lat, lon};
		}
	}
	
	public static long fromCoord(double lat, double lon) {
		return new CubePos(lat, lon).toIndex();
	}
		
	public static double[] toCoord(long geocode) {
		return new CubePos(geocode).toCoords();
	}
	
	public static long prefix(long ix, int depth) {
		if (depth == 0) {
			return 0L;
		} else {
			return ix & (~0L << (64 - depth));
		}
	}
	
	public static String print(long ix) {
		return String.format("%016x", ix);
	}

}
