package com.mrgris.prominence;

import java.util.ArrayList;
import java.util.List;

import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.Util;

public class GridPoint extends MeshPoint {

	public GridPoint(long ix, float elev) {
		super(ix, elev);
	}
	
	public GridPoint(DEMFile.Sample s) {
		this(s.ix, s.elev);
	}
	
	public long[] adjIx() {
		return adjacency(this.ix);
	}
	
	public static long[] adjacency(Long ix) {
		int[] _ix = PointIndex.split(ix);
		int[] rc = {_ix[1], _ix[2]};
		
		int[][] offsets = {
				{0, 1},
				{1, 1},
				{1, 0},
				{1, -1},
				{0, -1},
				{-1, -1},
				{-1, 0},
				{-1, 1},
			};
		List<Long> adj = new ArrayList<Long>();
		boolean fully_connected = (Util.mod(rc[0] + rc[1], 2) == 0);
		for (int[] offset : offsets) {
			boolean diagonal_connection = (Util.mod(offset[0] + offset[1], 2) == 0);
			if (fully_connected || !diagonal_connection) {
				adj.add(PointIndex.make(_ix[0], rc[0] + offset[0], rc[1] + offset[1]));
			}
		}
		long[] adjix = new long[adj.size()];
		for (int i = 0; i < adjix.length; i++) {
			adjix[i] = adj.get(i);
		}
		return adjix;
	}

	
}
