package promviz;

import java.util.List;

import promviz.dem.DEMFile;

public class TopologyBuilder {

	static final int CHUNK_SIZE_EXP = 13;
	
	static void buildTopology(List<DEMFile> DEMs) {
		// gen coverage
		// group all prefixes into chunks
		// start process pool and farm out chunks
		// handle output (including multiple phases)
	}
	
	static void buildChunk(Prefix chunkPrefix) {
		/*
		 * 
		 * 
		 * 
		 * 
output:
saddle->summit pairs for all saddles within chunk boundary. summit may be inside/outside/null
list of checkpoints created inside chunk boundary
list of checkpoints terminated at outside chunk boundary

bulk load all pages in chunk + one page fringe around border
for all points in chunk, identify saddles
for any saddles, for each lead (up+down) trace until local max/min. checkpointing rules in effect.
if hit edge of loaded area, pend
pend is uniq by lead (multiple pending saddles for same lead, potentially)
		 * 
		 */
	}
	
}
