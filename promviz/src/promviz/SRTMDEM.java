package promviz;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class SRTMDEM extends DEMFile {

	public SRTMDEM(String path, int width, int height, double lat0, double lon0, double dx, double dy, boolean sample_mode) {
		super(path, width, height, lat0, lon0, dx, dy, sample_mode);
	}
	
	public Object getReader(String path) throws FileNotFoundException {
		return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
	}
	
	public double getNextSample(Object reader) throws IOException {
		return ((LittleEndianDataInputStream)reader).readShort();
	}

}
