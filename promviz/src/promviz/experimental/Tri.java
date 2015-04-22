package promviz.experimental;

import promviz.Point;

public class Tri {

	Point[] points;
	
	//todo (as shorts):
	//angles (only need to store 2 angles)
	//gradient (need to handle 'flat')
	
	// this could be computed dynamically from the adjacency
	// lists of the individual points, but since this is a very
	// common operation, we pre-compute
	// adjacent[k] is the tri with the shared edge points[k:k+1]
	Tri[] adjacent;
	
}
