package com.mrgris.prominence.dem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.gdal.gdal.gdal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GDALUtil {

	private static final Logger LOG = LoggerFactory.getLogger(GDALUtil.class);
	
	public static boolean initialized = false;
	
    public static synchronized void initializeGDAL() {
    	if (initialized) {
    		return;
    	}
    	
    	// install gdal + java bindings
    	runproc(new ProcessBuilder("apt", "update"));
    	runproc(new ProcessBuilder("apt", "install", "-y", "libgdal-java"));

    	// set up jni
    	System.load("/usr/lib/jni/libgdalconstjni.so");
    	System.load("/usr/lib/jni/libgdaljni.so");
    	System.load("/usr/lib/jni/libogrjni.so");
    	System.load("/usr/lib/jni/libosrjni.so");

    	// init gdal
    	gdal.AllRegister();
    	
    	initialized = true;
    }
	
    public static void runproc(ProcessBuilder pb) {
    	pb.redirectErrorStream(true);    	
    	LOG.info("subprocess start");
	    try {
	        Process p = pb.start();

	        String line;
	        BufferedReader output =
	                new BufferedReader
	                        (new InputStreamReader(p.getInputStream()));
	        while ((line = output.readLine()) != null) {
	            LOG.debug("subprocess: " + line);
	        }
	        output.close();
	    } catch (IOException e) {
	    	throw new RuntimeException(e);
	    }    	
    	LOG.info("subprocess end");
    }
	
}
