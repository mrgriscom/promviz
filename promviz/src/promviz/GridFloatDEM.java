package promviz;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class GridFloatDEM extends DEMFile {

	public GridFloatDEM(String path, int width, int height, double lat0, double lon0, double dx, double dy, boolean sample_mode) {
		super(
			path,
			new GeoProjection(dx, dy, 0, 0, sample_mode),
			width,
			height,
			new GeoProjection(dx, dy, 0, 0, sample_mode).toGrid(lat0, lon0)[0],
			new GeoProjection(dx, dy, 0, 0, sample_mode).toGrid(lat0, lon0)[1]
		); // fucking java
	}
	
	public Object getReader(String path) throws FileNotFoundException {
		return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
	}
	
	public float getNextSample(Object reader) throws IOException {
		float e = ((LittleEndianDataInputStream)reader).readFloat();
		return e;
	}

}
