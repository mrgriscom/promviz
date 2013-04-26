import java.util.ArrayList;
import java.util.List;


public class Mesh {

	List<Point> points;
	List<Tri> tris;
	
	public Mesh() {
		points = new ArrayList<Point>();
		tris = new ArrayList<Tri>();
	}
	
	//todo: point[3]->tri index
	
}
