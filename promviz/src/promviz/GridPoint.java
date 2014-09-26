package promviz;


public class GridPoint extends Point {

	public GridPoint(long ix, float elev) {
		super(ix, elev);
	}
	
	public GridPoint(DEMFile.Sample s) {
		this(s.ix, s.elev);
	}
	
	public long[] adjIx() {
		return DEMManager.adjacency(this.ix);
	}
	
}
