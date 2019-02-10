package com.mrgris.prominence.util;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;

public class GeoCode {

	public static long fromCoord(double lat, double lon) {
		return S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).id();
	}

}
