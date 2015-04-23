package promviz;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import old.promviz.DEMManager;
import old.promviz.DualTopologyNetwork;
import old.promviz.PagedTopologyNetwork;
import old.promviz.Point;
import old.promviz.Prefix;
import old.promviz.PreprocessNetwork;
import old.promviz.TopologyNetwork;
import old.promviz.DEMManager.PartitionCounter;
import old.promviz.PreprocessNetwork.EdgeIterator;
import old.promviz.util.Logging;
import promviz.dem.DEMFile;
import promviz.util.DefaultMap;

public class TopologyBuilder {

	static final int CHUNK_SIZE_EXP = 13;
	
	static void buildTopology(List<DEMFile> DEMs) {
		// gen coverage
		// group all prefixes into chunks
		// start process pool and farm out chunks
		// handle output (including multiple phases)
	}
	
	class ChunkOutput {
		Prefix chunkPrefix;
		
	}
	
	static ChunkOutput buildChunk(Prefix chunkPrefix) {
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
	
	
	
	
	
	
	
	DualTopologyNetwork buildAll() {
		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
		Set<Prefix> allPrefixes = coverage.keySet();
		Logging.log("partitioning complete");
		Set<Prefix> yetToProcess = new HashSet<Prefix>(coverage.keySet()); //mutable!

		PagedElevGrid m = new PagedElevGrid(coverage, MESH_MAX_POINTS);
		DualTopologyNetwork tn = new DualTopologyNetwork(this, true);
		while (!tn.complete(allPrefixes, yetToProcess)) {
			Prefix nextPrefix = getNextPrefix(allPrefixes, yetToProcess, tn, m);
			if (m.isLoaded(nextPrefix)) {
				Logging.log("prefix already loaded!");
				continue;
			}
			
			Iterable<DEMFile.Sample> newData = m.loadPage(nextPrefix);
			tn.buildPartial(m, yetToProcess.contains(nextPrefix) ? newData : null);
			yetToProcess.remove(nextPrefix);
			Logging.log(yetToProcess.size() + " ytp");
		}
		return tn;
	}
		
	Prefix getNextPrefix(Set<Prefix> allPrefixes, Set<Prefix> yetToProcess, TopologyNetwork tn, PagedElevGrid m) {
		Map<Set<Prefix>, Integer> frontierTotals = tn.tallyPending(allPrefixes);
		
		Map<Prefix, Set<Set<Prefix>>> pendingPrefixes = new DefaultMap<Prefix, Set<Set<Prefix>>>() {
			@Override
			public Set<Set<Prefix>> defaultValue(Prefix _) {
				return new HashSet<Set<Prefix>>();
			}			
		};
		for (Set<Prefix> prefixGroup : frontierTotals.keySet()) {
			for (Prefix p : prefixGroup) {
				pendingPrefixes.get(p).add(prefixGroup);
			}
		}
		
		Prefix mostInDemand = null;
		int bestScore = 0;
		for (Entry<Prefix, Set<Set<Prefix>>> e : pendingPrefixes.entrySet()) {
			Prefix p = e.getKey();
			Set<Set<Prefix>> cohorts = e.getValue();

			Logging.log(String.format("pending> %s...", e.getKey())); // more?

			if (m.isLoaded(p)) {
				continue;
			}
			
			int score = 0;
			for (Set<Prefix> cohort : cohorts) {
				int numLoaded = 0;
				for (Prefix coprefix : cohort) {
					if (coprefix != p && m.isLoaded(coprefix)) {
						numLoaded++;
					}
				}
				int cohortScore = 1000000 * numLoaded + frontierTotals.get(cohort);
				score = Math.max(score, cohortScore);
			}
			
			if (score > bestScore) {
				bestScore = score;
				mostInDemand = p;
			}
		}
		
		if (mostInDemand == null) {
			mostInDemand = yetToProcess.iterator().next();
		}
		return mostInDemand;
	}
		
	class PartitionCounter {
		int count;
		Set<DEMFile> coverage;
		
		public PartitionCounter() {
			count = 0;
			coverage = new HashSet<DEMFile>();
		}
		
		public void addSample(DEMFile dem) {
			count++;
			coverage.add(dem);
		}
		
		public void combine(PartitionCounter pc) {
			count += pc.count;
			coverage.addAll(pc.coverage);
		}
	}
		
	// HACKY
	boolean inScope(long ix) {
		// FIXME bug lurking here: nodata areas within loaded DEM extents
		int[] _ix = PointIndex.split(ix);
		int[] xy = {_ix[1], _ix[2]};
		for (DEMFile dem : DEMs) {
			if (xy[0] >= dem.x0 && xy[1] >= dem.y0 &&
					xy[0] < (dem.x0 + dem.height) &&
					xy[1] < (dem.y0 + dem.width)) {
				return true;
			}
		}
		return false;
	}

	
	static void buildTopologyNetwork(DEMManager dm, String region) {
		loadDEMs(dm, region);
		
		DualTopologyNetwork dtn;
		MESH_MAX_POINTS = Long.parseLong(props.getProperty("memory")) / 8;
		dtn = dm.buildAll();
		dtn.up.cleanup();
		dtn.down.cleanup();
		
		System.err.println("edges in network (up), " + dtn.up.numEdges);
		System.err.println("edges in network (down), " + dtn.down.numEdges);
	}
	
	static void preprocessNetwork(DEMManager dm, String region) {
		if (region != null) {
			loadDEMs(dm, region);
		}

		File folder = new File(DEMManager.props.getProperty("dir_net"));
		if (folder.listFiles().length != 0) {
			throw new RuntimeException("/net not empty!");
		}
		
		MESH_MAX_POINTS = (1 << 26);
		PreprocessNetwork.preprocess(dm, true);
		PreprocessNetwork.preprocess(dm, false);
	}
	
	static void verifyNetwork() {
		DualTopologyNetwork dtn;
		MESH_MAX_POINTS = (1 << 26);
		dtn = DualTopologyNetwork.load(null);

		_verifyNetwork(dtn.up);
		_verifyNetwork(dtn.down);
	}
	
	static void _verifyNetwork(TopologyNetwork tn) {
		int peakClass = (tn.up ? Point.CLASS_SUMMIT : Point.CLASS_PIT);
		int saddleClass = (tn.up ? Point.CLASS_PIT : Point.CLASS_SADDLE);
		
		for (Point p : tn.allPoints()) {
			boolean refersToSelf = false;
			for (long adj : p.adjIx()) {
				if (adj == p.ix) {
					refersToSelf = true;
					break;
				}
			}
			if (refersToSelf) {
				Logging.log("verify: " + p + " refers to self");
				continue;
			}
			
			int pClass = p.classify(tn);
			if (pClass != Point.CLASS_SUMMIT && pClass != Point.CLASS_PIT) {
				if (pClass == Point.CLASS_OTHER && tn.pendingSaddles.contains(p)) {
					// fringe saddle whose leads are all pending -- not connected to rest of network
					// note: these are now filtered out by pagedtoponetwork.load()
					Logging.log("verify: fringe saddle (ok, but should have been pruned)");
					continue;
				}
				
				Logging.log("verify: " + p + " unexpected class " + pClass);
				continue;
			}
			boolean topologyViolation = false;
			for (Point adj : p.adjacent(tn)) {
				if (p == adj) {
					continue;
				}
				
				int adjClass = adj.classify(tn);
				if (adjClass != Point.CLASS_SUMMIT && adjClass != Point.CLASS_PIT) {
					Logging.log("verify: " + p + " adj " + adj + " unexpected class " + adjClass);
					continue;
				}
				if (adjClass == pClass) {
					topologyViolation = true;
					break;
				}
			}
			if (topologyViolation) {
				Logging.log("verify: " + p + " adjacent to same type " + pClass);					
				continue;
			}

			boolean isSaddle = (pClass == saddleClass);
			boolean connected;
			if (isSaddle) {
				int numAdj = (p.adjIx().length + (tn.pendingSaddles.contains(p) ? 1 : 0));
				connected = (numAdj == 2);
			} else {
				// redundant, since if this were false, point would have been flagged above
				connected = (p.adjIx().length >= 1);
			}
			if (!connected) {
				Logging.log("verify: " + p + " [" + pClass + "] insufficiently connected (" + p.adjIx().length + ")");					
				continue;
			}

			// check pending too?
		}
	}

	public class DualTopologyNetwork extends TopologyNetwork {

		TopologyNetwork up;
		TopologyNetwork down;
		
		public DualTopologyNetwork(DEMManager dm, boolean cache) {
			up = new TopologyNetwork(true, dm);
			down = new TopologyNetwork(false, dm);
			if (cache) {
				up.enableCache();
				down.enableCache();
			}
		}
		
		public DualTopologyNetwork() {
			
		}
		
		public static DualTopologyNetwork load(DEMManager dm) {
			DualTopologyNetwork dtn = new DualTopologyNetwork();
			dtn.up = new PagedTopologyNetwork(EdgeIterator.PHASE_RAW, true, dm, null);
			dtn.down = new PagedTopologyNetwork(EdgeIterator.PHASE_RAW, false, dm, null);
			return dtn;
		}
		
		public void buildPartial(PagedElevGrid m, Iterable<DEMFile.Sample> newPage) {
			long start = System.currentTimeMillis();
			
			// seems a bit questionable to use an iterable twice like this...
			up.buildPartial(m, newPage);
			down.buildPartial(m, newPage);

			Logging.log("@buildPartial: " + (System.currentTimeMillis() - start));
		}

		public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
			long start = System.currentTimeMillis();
					
			Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
				@Override
				public Integer defaultValue(Set<Prefix> _) {
					return 0;
				}
			};
			up.tallyPending(allPrefixes, frontierTotals);
			down.tallyPending(allPrefixes, frontierTotals);

			Logging.log("@tallyPending: " + (System.currentTimeMillis() - start));
			
			return frontierTotals;
		}


	}

	
}
