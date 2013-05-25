package promviz;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Mesh implements IMesh {

	Map<Long, Point> points;
	List<Tri> tris;
	
	public Mesh() {
		points = new HashMap<Long, Point>();
		tris = new ArrayList<Tri>();
	}
	
	public Point get(long ix) {
		return points.get(ix);
	}
	
	//todo: point[3]->tri index
	
}
