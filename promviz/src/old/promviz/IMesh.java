package old.promviz;

import old.promviz.PreprocessNetwork.Meta;
import promviz.MeshPoint;

public interface IMesh {
	public MeshPoint get(long geocode);
	public Meta getMeta(BasePoint p, String type);
}