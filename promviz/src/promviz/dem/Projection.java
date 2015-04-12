package promviz.dem;

public abstract class Projection {
	
	static final int SAMPLE_CENTER = 1;  // grid coordinates refer to center of pixel
	static final int SAMPLE_CORNER = 2;  // grid coordinates delineate edges of pixel
	
	int refID;
	int sampleMode;

	public Projection() {
		this(SAMPLE_CENTER);
	}
	
	public Projection(int sampleMode) {
		this.sampleMode = sampleMode;
	}
	
	public abstract double[] toXY(double lat, double lon);
	public abstract double[] fromXY(double x, double y);
	
	double pxOffset() {
		return (sampleMode == SAMPLE_CENTER ? 0 : 0.5);
	}
	
	public double[] fromGrid(int x, int y) {
		return this.fromXY(x + pxOffset(), y + pxOffset());
	}
	
	public int[] toGrid(double lat, double lon) {
		double[] xy = toXY(lat, lon);
		double offset = .5 - pxOffset();
		return new int[] {(int)Math.floor(xy[0] + offset), (int)Math.floor(xy[1] + offset)};
	}
}
