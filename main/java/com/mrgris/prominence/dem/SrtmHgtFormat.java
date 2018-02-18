package com.mrgris.prominence.dem;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.google.common.io.LittleEndianDataInputStream;

public class SrtmHgtFormat extends DEMFileFormat {

	final boolean LITTLE_ENDIAN = false;
	
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
