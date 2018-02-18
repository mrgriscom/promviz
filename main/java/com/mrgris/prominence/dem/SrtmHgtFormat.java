package com.mrgris.prominence.dem;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.google.common.io.LittleEndianDataInputStream;

public class SrtmHgtFormat extends DEMFileFormat {

	final boolean LITTLE_ENDIAN = false;
	
	public Object getReader(String path) throws FileNotFoundException {
		InputStream is;
		if (path.startsWith("http://") || path.startsWith("https://")) {
			try {
				is = new URL(path).openStream();
			} catch (IOException ioe) {
				throw new FileNotFoundException(path);
			}
		} else {
			is = new FileInputStream(path);
		}
		
		if (LITTLE_ENDIAN) {
			return new LittleEndianDataInputStream(new BufferedInputStream(is));
		} else {
			return new DataInputStream(new BufferedInputStream(is));			
		}
	}
	
	public double getNextSample(Object reader) throws IOException {
		return ((DataInput)reader).readShort();
	}

	public double noDataVal() {
		return -32768;
	}
	
}
