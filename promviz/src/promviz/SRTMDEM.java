package promviz;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class SRTMDEM extends DEMFile {

	final boolean LITTLE_ENDIAN = false;
	final boolean NODATA_IS_OCEAN = false;
	
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
		if (LITTLE_ENDIAN) {
			return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
		} else {
			return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));			
		}
	}
	
	public float getNextSample(Object reader) throws IOException {
		float elev = ((DataInput)reader).readShort();
		if (elev == -32768) {
			elev = (NODATA_IS_OCEAN ? 0 : Float.NaN);
		}
		return elev;
	}

}
