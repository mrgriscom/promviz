package com.mrgris.prominence.dem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;

import com.google.gson.Gson;
import com.mrgris.prominence.TopologyNetworkPipeline;

public class DEMIndex {
	
	static DEMIndex _inst;
	
	public static DEMIndex instance() {
		if (_inst == null) {
			_inst = load();
		}
		return _inst;
	}
	
	public static class Grid {
		public int id;
		public String srs;
		public double spacing;
		public double x_stretch = 1.;
		public double[] subpx_offset;
		public int[] origin_offset = {0, 0};
		
		CoordinateTransformation tx;
		
		public double[] toProjXY(int x, int y) {
			return new double[] {(x + subpx_offset[0] + origin_offset[0]) * spacing * x_stretch,
				            	 (y + subpx_offset[1] + origin_offset[1]) * spacing};
		}
	
		CoordinateTransformation loadTransform() {
	        SpatialReference dst = new SpatialReference();
	        dst.ImportFromEPSG(4326);
	        
	        String srsType = srs.substring(0, srs.indexOf(":"));
	        String srsParams = srs.substring(srs.indexOf(":") + 1);
	        SpatialReference src = new SpatialReference();
	        if (srsType.equals("epsg")) {
	        	src.ImportFromEPSG(Integer.parseInt(srsParams));
	        } else if (srsType.equals("proj")) {
	        	src.ImportFromProj4(srsParams);
	        }
	        
	        return new CoordinateTransformation(src, dst);
		}
		
		CoordinateTransformation getTransform() {
			if (tx == null) {
				tx = loadTransform();
			}
			return tx;
		}
		
		public double[] toLatLon(int x, int y) {
			double[] p = toProjXY(x, y);
			double[] lonlat = getTransform().TransformPoint(p[0], p[1]);
			return new double[] {lonlat[1], lonlat[0]};
		}
	}
	
	public Grid[] grids;
	public DEMFile[] dems;
	
	public static DEMIndex load() {
		try {
			InputStream is = new URL(TopologyNetworkPipeline.DEM_ROOT + "index.json").openStream();
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			return DEMIndex.load(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static DEMIndex load(Reader r) {
		Gson deser = new Gson();
		return deser.fromJson(r, DEMIndex.class);
	}
	
}
