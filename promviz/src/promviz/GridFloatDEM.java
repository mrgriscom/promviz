package promviz;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.LittleEndianDataInputStream;

public class GridFloatDEM extends DEMFile {

	final boolean NODATA_IS_OCEAN = true;
	
	public static Projection NEDProjection() {
		return new GeoProjection(1 / 10800.);
	}
	
	public GridFloatDEM(String path, int width, int height, double lat0, double lon0, double dx, double dy, boolean sample_mode) {
		super(
			path,
			NEDProjection(),
			width,
			height,
			NEDProjection().toGrid(lat0, lon0)[0],
			NEDProjection().toGrid(lat0, lon0)[1]
		); // fucking java
	}
	
	public Object getReader(String path) throws FileNotFoundException {
		return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
	}

	public void closeReader(Object f) throws IOException {
		((InputStream)f).close();
	}

	public float getNextSample(Object reader) throws IOException {
		float e = ((LittleEndianDataInputStream)reader).readFloat();
		if (e == -9999) {
			e = (NODATA_IS_OCEAN ? 0 : Float.NaN);
		}
		return e;
	}

}
