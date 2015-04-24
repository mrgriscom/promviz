package promviz;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import old.promviz.util.Logging;
import promviz.MeshPoint.Lead;
import promviz.dem.DEMFile;
import promviz.dem.SRTMDEM;
import promviz.util.WorkerPool;
import promviz.util.WorkerPoolDebug;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class TopologyBuilder {

	static final int CHUNK_SIZE_EXP = 13;
	static final int CHECKPOINT_FREQ = (1 << 11);
	static final int CHECKPOINT_LEN = CHECKPOINT_FREQ;
	
	static void buildTopology(List<DEMFile> DEMs) {
		final Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		
		Set<Prefix> chunks = new HashSet<Prefix>();
		for (Prefix p : coverage.keySet()) {
			chunks.add(new Prefix(p, CHUNK_SIZE_EXP));
		}

		WorkerPool<ChunkInput, ChunkOutput> wp = new WorkerPoolDebug<ChunkInput, ChunkOutput>(3) {
//		WorkerPool<ChunkInput, ChunkOutput> wp = new WorkerPool<ChunkInput, ChunkOutput>(3) {
			public ChunkOutput process(ChunkInput input) {
				return new ChunkProcessor(input).build();
			}

			int i = 1;
			public void postprocess(ChunkOutput output) {
				Logging.log((i++) + " " + output.chunkPrefix.toString() + " " + output.numPoints);
			}
		};
		wp.launch(Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
			public ChunkInput apply(Prefix p) {
				ChunkInput ci = new ChunkInput();
				ci.chunkPrefix = p;
				ci.coverage = (HashMap)((HashMap)coverage).clone();
				return ci;
			}
		}));
		
		// handle output (including multiple phases)
	}
	
	static class ChunkInput {
		Prefix chunkPrefix;
		Map<Prefix, Set<DEMFile>> coverage;
	}
	
	static class ChunkOutput {
		Prefix chunkPrefix;
		int numPoints;
	}
	
	static class ChunkProcessor {
		Prefix prefix;
		Map<Prefix, Set<DEMFile>> coverage;
		
		PagedElevGrid mesh;
		List<ChaseResult> processed;
		List<ChaseResult> pending;
		Set<Long> checkpoints;
		
		public ChunkProcessor(ChunkInput input) {
			this.prefix = input.chunkPrefix;
			this.coverage = input.coverage;
			
			processed = new ArrayList<ChaseResult>();
			pending = new ArrayList<ChaseResult>();
			checkpoints = new HashSet<Long>();
		}
		
		public ChunkOutput build() {
			mesh = new PagedElevGrid(coverage, (int)(1.5 * Math.pow(2, 2 * CHUNK_SIZE_EXP)));
			Iterable<DEMFile.Sample> points = mesh.loadForPrefix(prefix, 1);
			
			for (DEMFile.Sample s : points) {
				if (!prefix.isParent(s.ix)) {
					// don't process the fringe
					continue;
				}
				
				MeshPoint p = new GridPoint(s);
				int pointClass = p.classify(mesh);
				if (pointClass != MeshPoint.CLASS_SADDLE) {
					continue;
				}
				
				processSaddle(p);
			}
			
			Logging.log(prefix + " " + processed.size() + " " + pending.size());
			
			ChunkOutput output = new ChunkOutput();
			output.chunkPrefix = prefix;
			return output;
		}
		
		void processSaddle(MeshPoint saddle) {
			for (Lead[] dirLeads : saddle.leads(mesh)) {				
				for (Lead lead : dirLeads) {
					processLead(lead);
				}
			}
		}
		
		void processLead(Lead lead) {
			ChaseResult result = chase(lead);
			if (result.status != ChaseResult.STATUS_PENDING) {
				processed.add(result);
				if (result.status == ChaseResult.STATUS_INTERIM) {
					MeshPoint chk = result.lead.p;
					if (!checkpointExists(chk)) {
						checkpoints.add(chk.ix);
						processLead(result.lead.follow(mesh));
					}
				}
			} else {
				// pending
				pending.add(result);
			}
		}
		
		class ChaseResult {
			static final int STATUS_FINAL = 1;          // successfully found a local max(min)ima
			static final int STATUS_INDETERMINATE = 2;  // reached edge of data area
			static final int STATUS_PENDING = 3;        // reached edge of *loaded* data area; further processing once adjacent area is loaded
			static final int STATUS_INTERIM = 4;        // reached a 'checkpoint' due to excessive chase length
			
			Lead lead;
			int status;
			
			public ChaseResult(Lead lead, int status) {
				this.lead = lead;
				this.status = status;
			}
		}
		
		ChaseResult chase(Lead lead) {
			int status;
			int loopFailsafe = 0;
			while (true) {
				status = chaseStatus(lead);
				if (status != 0) {
					break;
				}
				
				lead.p = lead.follow(mesh).p;
				lead.len++;

				if (loopFailsafe++ > 2 * CHECKPOINT_LEN) {
					throw new RuntimeException("infinite loop");
				}
			}
			return new ChaseResult(lead, status);	
		}
		
		int chaseStatus(Lead lead) {
			int pClass;
			try {
				pClass = lead.p.classify(mesh);
			} catch (IndexOutOfBoundsException e) {
				return ChaseResult.STATUS_PENDING;
			}
			if (pClass == (lead.up ? MeshPoint.CLASS_SUMMIT : MeshPoint.CLASS_PIT)) {
				return ChaseResult.STATUS_FINAL;
			} else if (pClass == MeshPoint.CLASS_INDETERMINATE) {
				return ChaseResult.STATUS_INDETERMINATE;
			} else if (shouldCheckpoint(lead)) {
				if (pClass == MeshPoint.CLASS_SLOPE) { // avoid confusion until we've better thought through saddles leading to saddles
					return ChaseResult.STATUS_INTERIM;
				}
			}
			return 0;
		}
		
		boolean shouldCheckpoint(Lead lead) {
			return potentialCheckpoint(lead.p) && (checkpointExists(lead.p) || lead.len >= CHECKPOINT_LEN);
			// TODO when we start chasing leads using a surface model, only checkpoint at vertices
		}
		
		boolean potentialCheckpoint(Point p) {
			int[] pf = PointIndex.split(p.ix);
			return _chkpointIx(pf[1]) || _chkpointIx(pf[2]);
		}
		
		boolean _chkpointIx(int k) {
			return (k + CHECKPOINT_FREQ / 2) % CHECKPOINT_FREQ == 0;
		}
		
		boolean checkpointExists(Point p) {
			return !prefix.isParent(p.ix) || checkpoints.contains(p.ix);
		}
		
	}
	
	
	public static void main(String[] args) {
		Logging.init();
		buildTopology(loadDEMs(args[0]));
	}

	
	
//
//	public void tallyPending(Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> frontierTotals) {
//		for (Lead lead : pendingLeads) {
//			long term = lead.p.ix;
//			boolean interior = tallyAdjacency(term, allPrefixes, frontierTotals);
//			if (interior) {
//				pendInterior.add(term);
//			} else {
//				writePendingLead(lead);
//			}
//		}
//		
//		List<Long> edgeFringe = new ArrayList<Long>();
//		for (long fringe : unprocessedFringe) {
//			boolean interior = tallyAdjacency(fringe, allPrefixes, frontierTotals);
//			if (interior) {
//				pendInterior.add(fringe);
//			} else {
//				edgeFringe.add(fringe);
//			}
//		}
//		unprocessedFringe.removeAll(edgeFringe);
//
//		// trim pendInterior to only what is still relevant
//		Set<Long> newPendInterior = new HashSet<Long>();
//		for (Lead lead : pendingLeads) {
//			long term = lead.p.ix;
//			if (pendInterior.contains(term)) {
//				newPendInterior.add(term);
//			}
//		}
//		for (long fringe : unprocessedFringe) {
//			if (pendInterior.contains(fringe)) {
//				newPendInterior.add(fringe);
//			}
//		}
//		pendInterior = newPendInterior;
//	}
//
//	Prefix matchPrefix(long ix, Set<Prefix> prefixes) {
//		Prefix p = new Prefix(ix, DEMManager.GRID_TILE_SIZE);
//		return (prefixes.contains(p) ? p : null);
//	}
//	
//	boolean tallyAdjacency(long ix, Set<Prefix> allPrefixes, Map<Set<Prefix>, Integer> totals) {
//		Set<Prefix> frontiers = new HashSet<Prefix>();
//		frontiers.add(matchPrefix(ix, allPrefixes));
//		for (long adj : DEMManager.adjacency(ix)) {
//			// find the parititon the adjacent point lies in
//			Prefix partition = matchPrefix(adj, allPrefixes);
//
//			if (!pendInterior.contains(ix)) {
//				if (partition == null) {
//					// point is adjacent to a point that will never be loaded, i.e., on the edge of the
//					// entire region of interest; it will be indeterminate forever
//					return false;
//				}
//				if (!dm.inScope(adj)) {
//				//i think this is the only required check; the prefix-matching above can be discarded
//					return false;
//				}
//			}
//				
//			frontiers.add(partition);
//		}
//		
//		totals.put(frontiers, totals.get(frontiers) + 1);
//		return true;
//	}
//	
//
//		
//		
//		Set<Lead> oldPending = pendingLeads;
//		pendingLeads = new HashSet<Lead>();
//		pendingSaddles = new HashSet<Point>();
//		for (Lead lead : oldPending) {
//			Point saddle = lead.p0;
//			Point head = m.get(lead.p.ix);
//			if (head == null) {
//				// point not loaded -- effectively indeterminate
//				addPending(lead); // replicate entry in new map
//			} else {
//				processLead(m, lead);
//			}
//		}
//		
//		Logging.log("# pending: " + oldPending.size() + " -> " + pendingLeads.size());
//		Set<Long> fringeNowProcessed = new HashSet<Long>();
//		for (long ix : unprocessedFringe) {
//			// TODO don't reprocess points that were also pending leads?
//			// arises when a saddle leads to another saddle
//			Point p = m.get(ix);
//			int pointClass = (p != null ? p.classify(m) : Point.CLASS_INDETERMINATE);
//			if (pointClass != Point.CLASS_INDETERMINATE) {
//				fringeNowProcessed.add(ix);
//				if (pointClass == Point.CLASS_SADDLE) {
//					processSaddle(m, p);
//				}
//			}
//		}
//		unprocessedFringe.removeAll(fringeNowProcessed);
//		Logging.log("# fringe: " + (unprocessedFringe.size() + fringeNowProcessed.size()) + " -> " + unprocessedFringe.size());
//
//		if (newPage != null) {
//			build(m, newPage);
//		}
//		
//		trimNetwork();
//	}
//	
//	void trimNetwork() {
//		List<Point> toRemove = new ArrayList<Point>();
//		for (Point p : points.values()) {
//			if (!pendingSaddles.contains(p)) {
//				toRemove.add(p);
//			}
//		}
//		for (Point p : toRemove) {
//			points.remove(p);
//		}
//
//		
//		
//		
//		
//	
//	
//	
//	
	public static List<DEMFile> loadDEMs(String region) {
		List<DEMFile> dems = new ArrayList<DEMFile>();
		try {
	        Process proc = new ProcessBuilder(new String[] {"python", "/home/drew/dev/pv2/script/demregion.py", region}).start();
	        BufferedReader stdin = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	        
	        String s = null;
	        while ((s = stdin.readLine()) != null) {
	        	String[] parts = s.split(",");
	        	String type = parts[0];
	        	String path = parts[1];
	        	int w = Integer.parseInt(parts[2]);
	        	int h = Integer.parseInt(parts[3]);
	        	double lat = Double.parseDouble(parts[4]);
	        	double lon = Double.parseDouble(parts[5]);
	        	int res = Integer.parseInt(parts[6]);
	        	
	        	DEMFile dem = new SRTMDEM(path, w, h, lat, lon, res);
	        	dems.add(dem);
	        }
	    } catch (IOException e) {
	        throw new RuntimeException();
	    }		
		return dems;
	}
	
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

we only bulk process initially loaded area
once pages are being loaded on-demand, it is to only chase down pending leads

		 * 
		 * 
		 * 
	 */
}
	
	
	
	
	
//	
//	
//	DualTopologyNetwork buildAll() {
//		Map<Prefix, Set<DEMFile>> coverage = this.partitionDEM();
//		Set<Prefix> allPrefixes = coverage.keySet();
//		Logging.log("partitioning complete");
//		Set<Prefix> yetToProcess = new HashSet<Prefix>(coverage.keySet()); //mutable!
//
//		PagedElevGrid m = new PagedElevGrid(coverage, MESH_MAX_POINTS);
//		DualTopologyNetwork tn = new DualTopologyNetwork(this, true);
//		while (!tn.complete(allPrefixes, yetToProcess)) {
//			Prefix nextPrefix = getNextPrefix(allPrefixes, yetToProcess, tn, m);
//			if (m.isLoaded(nextPrefix)) {
//				Logging.log("prefix already loaded!");
//				continue;
//			}
//			
//			Iterable<DEMFile.Sample> newData = m.loadPage(nextPrefix);
//			tn.buildPartial(m, yetToProcess.contains(nextPrefix) ? newData : null);
//			yetToProcess.remove(nextPrefix);
//			Logging.log(yetToProcess.size() + " ytp");
//		}
//		return tn;
//	}
//		
//	Prefix getNextPrefix(Set<Prefix> allPrefixes, Set<Prefix> yetToProcess, TopologyNetwork tn, PagedElevGrid m) {
//		Map<Set<Prefix>, Integer> frontierTotals = tn.tallyPending(allPrefixes);
//		
//		Map<Prefix, Set<Set<Prefix>>> pendingPrefixes = new DefaultMap<Prefix, Set<Set<Prefix>>>() {
//			@Override
//			public Set<Set<Prefix>> defaultValue(Prefix _) {
//				return new HashSet<Set<Prefix>>();
//			}			
//		};
//		for (Set<Prefix> prefixGroup : frontierTotals.keySet()) {
//			for (Prefix p : prefixGroup) {
//				pendingPrefixes.get(p).add(prefixGroup);
//			}
//		}
//		
//		Prefix mostInDemand = null;
//		int bestScore = 0;
//		for (Entry<Prefix, Set<Set<Prefix>>> e : pendingPrefixes.entrySet()) {
//			Prefix p = e.getKey();
//			Set<Set<Prefix>> cohorts = e.getValue();
//
//			Logging.log(String.format("pending> %s...", e.getKey())); // more?
//
//			if (m.isLoaded(p)) {
//				continue;
//			}
//			
//			int score = 0;
//			for (Set<Prefix> cohort : cohorts) {
//				int numLoaded = 0;
//				for (Prefix coprefix : cohort) {
//					if (coprefix != p && m.isLoaded(coprefix)) {
//						numLoaded++;
//					}
//				}
//				int cohortScore = 1000000 * numLoaded + frontierTotals.get(cohort);
//				score = Math.max(score, cohortScore);
//			}
//			
//			if (score > bestScore) {
//				bestScore = score;
//				mostInDemand = p;
//			}
//		}
//		
//		if (mostInDemand == null) {
//			mostInDemand = yetToProcess.iterator().next();
//		}
//		return mostInDemand;
//	}
//		
//	class PartitionCounter {
//		int count;
//		Set<DEMFile> coverage;
//		
//		public PartitionCounter() {
//			count = 0;
//			coverage = new HashSet<DEMFile>();
//		}
//		
//		public void addSample(DEMFile dem) {
//			count++;
//			coverage.add(dem);
//		}
//		
//		public void combine(PartitionCounter pc) {
//			count += pc.count;
//			coverage.addAll(pc.coverage);
//		}
//	}
//		
//	// HACKY
//	boolean inScope(long ix) {
//		// FIXME bug lurking here: nodata areas within loaded DEM extents
//		int[] _ix = PointIndex.split(ix);
//		int[] xy = {_ix[1], _ix[2]};
//		for (DEMFile dem : DEMs) {
//			if (xy[0] >= dem.x0 && xy[1] >= dem.y0 &&
//					xy[0] < (dem.x0 + dem.height) &&
//					xy[1] < (dem.y0 + dem.width)) {
//				return true;
//			}
//		}
//		return false;
//	}
//
//	
//	static void buildTopologyNetwork(DEMManager dm, String region) {
//		loadDEMs(dm, region);
//		
//		DualTopologyNetwork dtn;
//		MESH_MAX_POINTS = Long.parseLong(props.getProperty("memory")) / 8;
//		dtn = dm.buildAll();
//		dtn.up.cleanup();
//		dtn.down.cleanup();
//		
//		System.err.println("edges in network (up), " + dtn.up.numEdges);
//		System.err.println("edges in network (down), " + dtn.down.numEdges);
//	}
//	
//	static void preprocessNetwork(DEMManager dm, String region) {
//		if (region != null) {
//			loadDEMs(dm, region);
//		}
//
//		File folder = new File(DEMManager.props.getProperty("dir_net"));
//		if (folder.listFiles().length != 0) {
//			throw new RuntimeException("/net not empty!");
//		}
//		
//		MESH_MAX_POINTS = (1 << 26);
//		PreprocessNetwork.preprocess(dm, true);
//		PreprocessNetwork.preprocess(dm, false);
//	}
//	
//	static void verifyNetwork() {
//		DualTopologyNetwork dtn;
//		MESH_MAX_POINTS = (1 << 26);
//		dtn = DualTopologyNetwork.load(null);
//
//		_verifyNetwork(dtn.up);
//		_verifyNetwork(dtn.down);
//	}
//	
//	static void _verifyNetwork(TopologyNetwork tn) {
//		int peakClass = (tn.up ? Point.CLASS_SUMMIT : Point.CLASS_PIT);
//		int saddleClass = (tn.up ? Point.CLASS_PIT : Point.CLASS_SADDLE);
//		
//		for (Point p : tn.allPoints()) {
//			boolean refersToSelf = false;
//			for (long adj : p.adjIx()) {
//				if (adj == p.ix) {
//					refersToSelf = true;
//					break;
//				}
//			}
//			if (refersToSelf) {
//				Logging.log("verify: " + p + " refers to self");
//				continue;
//			}
//			
//			int pClass = p.classify(tn);
//			if (pClass != Point.CLASS_SUMMIT && pClass != Point.CLASS_PIT) {
//				if (pClass == Point.CLASS_OTHER && tn.pendingSaddles.contains(p)) {
//					// fringe saddle whose leads are all pending -- not connected to rest of network
//					// note: these are now filtered out by pagedtoponetwork.load()
//					Logging.log("verify: fringe saddle (ok, but should have been pruned)");
//					continue;
//				}
//				
//				Logging.log("verify: " + p + " unexpected class " + pClass);
//				continue;
//			}
//			boolean topologyViolation = false;
//			for (Point adj : p.adjacent(tn)) {
//				if (p == adj) {
//					continue;
//				}
//				
//				int adjClass = adj.classify(tn);
//				if (adjClass != Point.CLASS_SUMMIT && adjClass != Point.CLASS_PIT) {
//					Logging.log("verify: " + p + " adj " + adj + " unexpected class " + adjClass);
//					continue;
//				}
//				if (adjClass == pClass) {
//					topologyViolation = true;
//					break;
//				}
//			}
//			if (topologyViolation) {
//				Logging.log("verify: " + p + " adjacent to same type " + pClass);					
//				continue;
//			}
//
//			boolean isSaddle = (pClass == saddleClass);
//			boolean connected;
//			if (isSaddle) {
//				int numAdj = (p.adjIx().length + (tn.pendingSaddles.contains(p) ? 1 : 0));
//				connected = (numAdj == 2);
//			} else {
//				// redundant, since if this were false, point would have been flagged above
//				connected = (p.adjIx().length >= 1);
//			}
//			if (!connected) {
//				Logging.log("verify: " + p + " [" + pClass + "] insufficiently connected (" + p.adjIx().length + ")");					
//				continue;
//			}
//
//			// check pending too?
//		}
//	}
//
//	public class DualTopologyNetwork extends TopologyNetwork {
//
//		TopologyNetwork up;
//		TopologyNetwork down;
//		
//		public DualTopologyNetwork(DEMManager dm, boolean cache) {
//			up = new TopologyNetwork(true, dm);
//			down = new TopologyNetwork(false, dm);
//			if (cache) {
//				up.enableCache();
//				down.enableCache();
//			}
//		}
//		
//		public DualTopologyNetwork() {
//			
//		}
//		
//		public static DualTopologyNetwork load(DEMManager dm) {
//			DualTopologyNetwork dtn = new DualTopologyNetwork();
//			dtn.up = new PagedTopologyNetwork(EdgeIterator.PHASE_RAW, true, dm, null);
//			dtn.down = new PagedTopologyNetwork(EdgeIterator.PHASE_RAW, false, dm, null);
//			return dtn;
//		}
//		
//		public void buildPartial(PagedElevGrid m, Iterable<DEMFile.Sample> newPage) {
//			long start = System.currentTimeMillis();
//			
//			// seems a bit questionable to use an iterable twice like this...
//			up.buildPartial(m, newPage);
//			down.buildPartial(m, newPage);
//
//			Logging.log("@buildPartial: " + (System.currentTimeMillis() - start));
//		}
//
//		public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
//			long start = System.currentTimeMillis();
//					
//			Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
//				@Override
//				public Integer defaultValue(Set<Prefix> _) {
//					return 0;
//				}
//			};
//			up.tallyPending(allPrefixes, frontierTotals);
//			down.tallyPending(allPrefixes, frontierTotals);
//
//			Logging.log("@tallyPending: " + (System.currentTimeMillis() - start));
//			
//			return frontierTotals;
//		}
//
//
//	}
//
