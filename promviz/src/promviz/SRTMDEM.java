package promviz;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class SRTMDEM extends DEMFile {

	public static Projection SRTMProjection(double downscaling) {
		return new GeoProjection(downscaling / 1200.);
	}
	
	public SRTMDEM(String path, int width, int height, double lat0, double lon0, double downscaling) {
		super(
			path,
			SRTMProjection(downscaling),
			width,
			height,
			SRTMProjection(downscaling).toGrid(lat0, lon0)[0],
			SRTMProjection(downscaling).toGrid(lat0, lon0)[1]
		); // fucking java
	}
	
	public Object getReader(String path) throws FileNotFoundException {
		return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
	}
	
	public float getNextSample(Object reader) throws IOException {
		int elev = ((LittleEndianDataInputStream)reader).readShort();
		if (elev == -32768) {
			elev = 0; // cgiar has voids filled so nodata is actually ocean
		}
		return elev;
	}

}
