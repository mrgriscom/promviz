package promviz;

public abstract class Projection {
	
	int refID;
	
	boolean sampleMode; // true == lat0/lon0 correspond to center of pixel; false == correspond to corner of pixel

	public Projection() {
		this(true);
	}
	
	public Projection(boolean sampleMode) {
		this.sampleMode = sampleMode;
	}
	
	public abstract double[] toXY(double lat, double lon);
	public abstract double[] fromXY(double x, double y);
	
	public double[] fromGrid(int x, int y) {
		return this.fromXY(x + (sampleMode ? 0 : 0.5), y + (sampleMode ? 0 : 0.5));
	}
	
	public int[] toGrid(double lat, double lon) {
		double[] xy = toXY(lat, lon);
		// todo: need epsilon for non-sample mode?
		return new int[] {(int)Math.floor(xy[0] + (sampleMode ? .5 : 0)), (int)Math.floor(xy[1] + (sampleMode ? .5 : 0))};
	}
}
