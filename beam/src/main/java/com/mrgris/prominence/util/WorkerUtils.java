package com.mrgris.prominence.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.gdal.gdal.gdal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class WorkerUtils {

	private static final Logger LOG = LoggerFactory.getLogger(WorkerUtils.class);
	
	private static boolean aptUpdated = false;
	synchronized private static void aptUpdate() {
		if (!aptUpdated) {
			exec(new ProcessBuilder("apt", "update"));
			aptUpdated = true;
		}
	}
	
	synchronized public static void aptInstall(String... packages) {
		aptUpdate();
		
		List<String> args = Lists.newArrayList(Iterables.concat(
				ImmutableList.of("apt", "install", "-y"), Arrays.asList(packages)));
		exec(new ProcessBuilder(args));
	}
	
	public static boolean GDALinitialized = false;
    synchronized public static void initializeGDAL() {
    	if (!GDALinitialized) {
	    	aptInstall("libgdal-java");
	
	    	// set up jni
	    	System.load("/usr/lib/jni/libgdalconstjni.so");
	    	System.load("/usr/lib/jni/libgdaljni.so");
	    	System.load("/usr/lib/jni/libogrjni.so");
	    	System.load("/usr/lib/jni/libosrjni.so");
	
	    	// init gdal
	    	gdal.AllRegister();
	    	
	    	GDALinitialized = true;
    	}
    }
    // DON'T make this synchronized
    public static void checkGDAL() {
    	if (!GDALinitialized) {
    		throw new RuntimeException("GDAL has not been initialized");
    	}
    }
    
	private static boolean spatialiteInitialized = false;
    synchronized public static void initializeSpatialite() {
    	if (!spatialiteInitialized) {
    		aptInstall("libsqlite3-mod-spatialite");
    		spatialiteInitialized = true;
    	}
    }
    
    public static void exec(ProcessBuilder pb) {
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
