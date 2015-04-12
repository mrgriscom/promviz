package promviz.dem;

public class GeoProjection extends Projection {

	double x0;
	double y0;
	double xscale;
	double yscale;

	public GeoProjection(double scale) {
		this(scale, scale);
	}
	
	public GeoProjection(double xscale, double yscale) {
		this(xscale, yscale, 0., 0., SAMPLE_CENTER);
	}
	
	public GeoProjection(double xscale, double yscale, double x0, double y0, int sampleMode) {
		super(sampleMode);
		this.xscale = xscale;
		this.yscale = yscale;
		this.x0 = x0;
		this.y0 = y0;
	}
	
	static GeoProjection fromArcseconds(double res) {
		return new GeoProjection(res / 3600.);
	}
	
	@Override
	public double[] toXY(double lat, double lon) {
		return new double[] {(lon - x0) / xscale, (lat - y0) / yscale};
	}

	@Override
	public double[] fromXY(double x, double y) {
		return new double[] {yscale * y + y0, xscale * x + x0};
	}

}
