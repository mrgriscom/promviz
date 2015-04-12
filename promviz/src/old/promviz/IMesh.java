package old.promviz;

import old.promviz.PreprocessNetwork.Meta;

public interface IMesh {
	public Point get(long geocode);
	public Meta getMeta(BasePoint p, String type);
}