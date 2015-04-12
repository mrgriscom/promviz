package promviz.dem;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.LittleEndianDataInputStream;

public class SRTMDEM extends DEMFile {

	final boolean LITTLE_ENDIAN = false;
	
	public SRTMDEM(String path, int width, int height, double lat0, double lon0, int res) {
		super(path, GeoProjection.fromArcseconds(res), width, height, lat0, lon0);
	}
	
	public Object getReader(String path) throws FileNotFoundException {
		if (LITTLE_ENDIAN) {
			return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
		} else {
			return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));			
		}
	}
	
	public double getNextSample(Object reader) throws IOException {
		return ((DataInput)reader).readShort();
	}

	public double noDataVal() {
		return -32768;
	}
	
}
