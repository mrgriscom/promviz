

public class Point {

	//todo: geopoint index (as long) instead of lat/lon
	double lat;
	double lon;
	float elev; // meters

	/* list of points adjacent to this point, listed in a clockwise direction
	 * if any two consecutive points are NOT adjacent to each other (i.e., the
	 * three points do not form a Tri), this list will have one or more nulls
	 * inserted between them
	 */
	Point[] adjacent;

	public Point(double lat, double lon, double elev) {
		this.lat = lat;
		this.lon = lon;
		this.elev = (float)(elev + Math.random() - 0.5);
	}
	
	
	
}
