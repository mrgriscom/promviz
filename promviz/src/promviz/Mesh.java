package promviz;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Mesh {

	Map<Long, Point> points;
	List<Tri> tris;
	
	public Mesh() {
		points = new HashMap<Long, Point>();
		tris = new ArrayList<Tri>();
	}
	
	//todo: point[3]->tri index
	
}
