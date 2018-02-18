package com.mrgris.prominence.dem;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class GridFloatFormat extends DEMFileFormat {

	public Object getReader(String path) throws FileNotFoundException {
		return new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(path)));
	}

	public double getNextSample(Object reader) throws IOException {
		return ((LittleEndianDataInputStream)reader).readFloat();
	}

	public double noDataVal() {
		return -9999;
	}
}
