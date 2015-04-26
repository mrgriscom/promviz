package promviz;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.MeshPoint.Lead;
import promviz.dem.DEMFile;
import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.Util;
import promviz.util.WorkerPool;
import promviz.util.WorkerPoolDebug;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class TopologyBuilder {

	static final int NUM_WORKERS = 3;
	
	static final int CHUNK_SIZE_EXP = 13;
	static final int CHECKPOINT_FREQ = (1 << 11);
	static final int CHECKPOINT_LEN = CHECKPOINT_FREQ;
	
	static Prefix _chunk(long ix) {
		return new Prefix(ix, CHUNK_SIZE_EXP);
	}
	
	static void buildTopology(List<DEMFile> DEMs) {
		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		
		FileUtil.ensureEmpty(FileUtil.PHASE_RAW);
		
		Builder builder = new Builder(NUM_WORKERS, coverage);
		builder.firstRound();
		
		int round = 0;
		while (builder.needsAnotherRound()) {
			round++;
			Logging.log("round " + round);
			
			builder.nextRound();
		}
	}
	
	static class ChunkInput {
		Prefix chunkPrefix;
		Map<Prefix, Set<DEMFile>> coverage;
		boolean fullMode;
		ChunkInputForDir[] byDir;
	}
	
	static class ChunkInputForDir {
		Set<Long> internalCheckpoints; // known checkpoints inside the current chunk
		List<Long> pendingCheckpoints; // unresolved checkpoints in this chunk that we must process
	}
	
	static class ChunkOutput {
		Prefix chunkPrefix;
		ChunkOutputForDir[] byDir;
	}
	
	static class ChunkOutputForDir {
		boolean up;
		List<Edge> network;
		Set<Long> internalCheckpoints; // known checkpoints inside the current chunk
		Set<Long> pendingCheckpoints;  // pending checkpoints outside this chunk to be processed in the next round
	}
	
	static class ChunkProcessor {
		Prefix prefix;
		Map<Prefix, Set<DEMFile>> coverage;
		ChunkInput input;
		
		PagedElevGrid mesh;
		Map<SaddleAndDir, Set<SummitAndTag>> processedBySaddle;
		List<Lead> pending; // TODO group by common (term, up)?

		Set<Long> internalCheckpointsUp;
		Set<Long> internalCheckpointsDown;
		Set<Long> pendingCheckpointsUp;
		Set<Long> pendingCheckpointsDown;
		
		public ChunkProcessor(ChunkInput input) {
			this.input = input;
			this.prefix = input.chunkPrefix;
			this.coverage = input.coverage;
			
			processedBySaddle = new DefaultMap<SaddleAndDir, Set<SummitAndTag>>() {
				public Set<SummitAndTag> defaultValue(SaddleAndDir key) {
					return new HashSet<SummitAndTag>();
				}
			};
			pending = new ArrayList<Lead>();
			internalCheckpointsUp = input.byDir[0].internalCheckpoints;
			internalCheckpointsDown = input.byDir[1].internalCheckpoints;
			pendingCheckpointsUp = new HashSet<Long>();
			pendingCheckpointsDown = new HashSet<Long>();
		}
		
		boolean inChunk(long ix) {
			return prefix.isParent(ix);	
		}
		
		Set<Long> internalCheckpoints(boolean up) {
			return (up ? internalCheckpointsUp : internalCheckpointsDown);
		}
		Set<Long> pendingCheckpoints(boolean up) {
			return (up ? pendingCheckpointsUp : pendingCheckpointsDown);
		}
		
		public ChunkOutput build() {
			mesh = new PagedElevGrid(coverage, (int)(1.5 * Math.pow(2, 2 * CHUNK_SIZE_EXP)));
			Iterable<DEMFile.Sample> points = mesh.loadForPrefix(prefix, 1);
			
			if (input.fullMode) {
				// process entire chunk				
				for (DEMFile.Sample s : points) {
					if (!inChunk(s.ix)) {
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
			} else {
				// only process pending leads from previous round
				for (boolean up : new boolean[] {true, false}) {
					for (long ix : input.byDir[up ? 0 : 1].pendingCheckpoints) {
						resumeFromCheckpoint(ix, up);
					}
				}
			}
			
			while (pending.size() > 0) {
				processPending();
			}
			
			Map<Boolean, List<Edge>> topology = normalizeTopology();
			
			ChunkOutput output = new ChunkOutput();
			output.chunkPrefix = prefix;
			output.byDir = new ChunkOutputForDir[2];
			for (int i = 0; i < 2; i++) {
				ChunkOutputForDir o = new ChunkOutputForDir();
				output.byDir[i] = o;
				o.up = (i == 0);
				o.network = topology.get(o.up);
				o.internalCheckpoints = internalCheckpoints(o.up);
				o.pendingCheckpoints = pendingCheckpoints(o.up);
			}
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
				processResult(result);
				if (result.status == ChaseResult.STATUS_INTERIM) {
					long chk = result.lead.p.ix;
					if (!checkpointExists(result.lead)) {
						internalCheckpoints(lead.up).add(chk);
						resumeFromCheckpoint(result.lead);
					} else if (!inChunk(chk)) {
						pendingCheckpoints(lead.up).add(chk);
					}
				}
			} else {
				pending.add(result.lead);
			}
		}

		void resumeFromCheckpoint(long ix, boolean up) {
			resumeFromCheckpoint(new Lead(up, null, mesh.get(ix), -1));
		}
		
		void resumeFromCheckpoint(Lead lead) {
			Lead resume = lead.follow(mesh);
			resume.fromCheckpoint = true;
			processLead(resume);			
		}
		
		void processPending() {
			mesh.loadPage(bestNextPage());
			
			List<Lead> pending_ = pending;
			pending = new ArrayList<Lead>();
			// doing this iteratively for each loaded page could be inefficient; maintaining the
			// necessary indexes seems like a pain though
			for (Lead pend : pending_) {
				processLead(pend);
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
			return potentialCheckpoint(lead.p) && (checkpointExists(lead) || lead.len >= CHECKPOINT_LEN);
			// TODO when we start chasing leads using a surface model, only checkpoint at vertices
		}
		
		boolean potentialCheckpoint(Point p) {
			int[] pf = PointIndex.split(p.ix);
			return _chkpointIx(pf[1]) || _chkpointIx(pf[2]);
		}
		
		boolean _chkpointIx(int k) {
			// checkpoint lines should be offset from chunk boundaries (fails if
			// CHECKPOINT_FREQ >= chunk size)
			return (k + CHECKPOINT_FREQ / 2) % CHECKPOINT_FREQ == 0;
		}
		
		boolean checkpointExists(Lead lead) {
			return checkpointExists(lead.p.ix, lead.up);
		}
		
		boolean checkpointExists(long ix, boolean up) {
			return !inChunk(ix) // assume checkpointing outside current chunk area is handled externally 
					|| internalCheckpoints(up).contains(ix);
		}
		
		Prefix bestNextPage() {
			final Map<Prefix, Double> opportunity = new DefaultMap<Prefix, Double>() {
				public Double defaultValue(Prefix key) {
					return 0.;
				}
			};
			final Map<Prefix, Integer> demand = new DefaultMap<Prefix, Integer>() {
				public Integer defaultValue(Prefix key) {
					return 0;
				}				
			};
			
			// this could be sped up a bit (but worth the code complexity?)
			for (Lead pend : pending) {
				MeshPoint p = pend.p;
				Set<Prefix> frontiers = new HashSet<Prefix>();
				frontiers.add(PagedElevGrid.segmentPrefix(p.ix));
				for (long adj : p.adjIx()) {
					frontiers.add(PagedElevGrid.segmentPrefix(adj));
				}
				
				int numLoaded = 0;
				for (Prefix page : frontiers) {
					if (mesh.isLoaded(page)) {
						numLoaded++;
					}
				}
				int numNeeded = frontiers.size() - numLoaded;

				double opp = (double)frontiers.size() / numNeeded;
				for (Prefix page : frontiers) {
					if (!mesh.isLoaded(page)) {
						opportunity.put(page, Math.max(opportunity.get(page), opp));
						demand.put(page, demand.get(page) + 1);
					}
				}
			}
			
			return Collections.max(demand.keySet(), new Comparator<Prefix>() {
				public int compare(Prefix p1, Prefix p2) {
					double opp1 = opportunity.get(p1);
					double opp2 = opportunity.get(p2);
					int demand1 = demand.get(p1);
					int demand2 = demand.get(p2);
					
					if (opp1 != opp2) {
						return Double.valueOf(opp1).compareTo(opp2);
					} else {
						return Integer.valueOf(demand1).compareTo(demand2);
					}
				}
			});
		}
		
		void processResult(ChaseResult cr) {
			Lead lead = cr.lead;
			SaddleAndDir k = new SaddleAndDir(lead.p0.ix, lead.up);
			SummitAndTag v = new SummitAndTag(lead.p.ix, lead.i);
			if (cr.status == ChaseResult.STATUS_INDETERMINATE) {
				v.summit = PointIndex.NULL;
			} else if (cr.status == ChaseResult.STATUS_INTERIM) {
				v.summit = PointIndex.clone(v.summit, UD(lead.up));
			}
			processedBySaddle.get(k).add(v);
	
			if (cr.lead.fromCheckpoint) {
				long checkpoint = PointIndex.clone(k.saddle, UD(lead.up));
				processedBySaddle.get(k).add(new SummitAndTag(checkpoint, Edge.TAG_NULL));
			}
		}
		
		static class SaddleAndDir {
			long saddle;
			boolean up;
			
			SaddleAndDir(long saddle, boolean up) {
				this.saddle = saddle;
				this.up = up;
			}
			
			public boolean equals(Object o) {
				SaddleAndDir sd = (SaddleAndDir)o;
				return this.saddle == sd.saddle && this.up == sd.up;
			}
			
			public int hashCode() {
				return Long.valueOf(saddle).hashCode() ^ Boolean.valueOf(up).hashCode();
			}
		}

		static class SummitAndTag {
			long summit;
			int tag;
			
			SummitAndTag(long summit, int tag) {
				this.summit = summit;
				this.tag = tag;
			}
			
			public boolean equals(Object o) {
				SummitAndTag st = (SummitAndTag)o;
				return this.summit == st.summit && this.tag == st.tag;
			}
			
			public int hashCode() {
				return Long.valueOf(summit).hashCode() ^ Integer.valueOf(tag).hashCode();
			}
		}

		static class PseudoEdge {
			SaddleAndDir k;
			SummitAndTag v;
			
			public PseudoEdge(SaddleAndDir k, SummitAndTag v) {
				this.k = k;
				this.v = v;
			}

			public PseudoEdge(long saddle, long summit, boolean dir, int tag) {
				this(new SaddleAndDir(saddle, dir), new SummitAndTag(summit, tag));
			}

			public boolean equals(Object o) { throw new UnsupportedOperationException(); }
			public int hashCode() { throw new UnsupportedOperationException(); }
		}
		
		interface TopologyCleaner {
			// add modifications to 'edges', return true to remove the original entry (which
			// you almost certainly want to do if you add anything)
			boolean clean(List<PseudoEdge> additions, SaddleAndDir k, Set<SummitAndTag> v);
		}
		
		public Map<Boolean, List<Edge>> normalizeTopology() {
			// note: we try to release memory from the internal data structures as their objects
			// are consumed, but the memory taken by the container itself does not shrink
						
			// define several cleaning operations
			TopologyCleaner unconnected = new TopologyCleaner() {
				public boolean clean(List<PseudoEdge> additions, SaddleAndDir k, Set<SummitAndTag> v) {
					// remove saddles that are unconnected to the rest of the network
					boolean allPending = true;
					for (SummitAndTag st : v) {
						if (st.summit != PointIndex.NULL) {
							allPending = false;
							break;
						}
					}
					return allPending;
				}
			};
			TopologyCleaner multisaddle = new TopologyCleaner() {
				public boolean clean(List<PseudoEdge> additions, SaddleAndDir k, Set<SummitAndTag> vset) {
					// break up multisaddles
					if (vset.size() <= 2) {
						return false;
					}
					
					List<SummitAndTag> v = new ArrayList<SummitAndTag>(vset);
					Collections.sort(v, new Comparator<SummitAndTag>() {
						public int compare(SummitAndTag a, SummitAndTag b) {
							return Integer.compare(a.tag, b.tag);
						}
					});
		
					long vPeak = PointIndex.clone(k.saddle, 1);
					for (int i = 0; i < v.size(); i++) {
						SummitAndTag st = v.get(i);
						long vSaddle = PointIndex.clone(k.saddle, -i); // would be nice to randomize the saddle ordering
						
						// the disambiguation pattern is not symmetrical between the 'up' and 'down' networks;
						// this is the cost of making them consistent with each other
						if (k.up) {
							additions.add(new PseudoEdge(vSaddle, st.summit, k.up, st.tag));
							additions.add(new PseudoEdge(vSaddle, vPeak, k.up, Edge.TAG_NULL));
						} else {
							SummitAndTag st_prev = v.get(Util.mod(i - 1, v.size()));
							additions.add(new PseudoEdge(vSaddle, st.summit, k.up, st.tag));
							additions.add(new PseudoEdge(vSaddle, st_prev.summit, k.up, st_prev.tag));
						}
					}
					return true;
				}
			};
			TopologyCleaner ring = new TopologyCleaner() {
				public boolean clean(List<PseudoEdge> additions, SaddleAndDir k, Set<SummitAndTag> vset) {
					// split rings (both saddle edges lead to same point)
					SummitAndTag[] v = unpackForSaddle(vset, 2);
					if (v[0].summit != v[1].summit) {
						return false;
					}

					long summit = v[0].summit;
					if (summit == PointIndex.NULL) {
						// this situation was caught by the 'unconnected' cleaner, but it can crop up
						// again in the 'down' network for multisaddles with >1 indeterminate lead
						return true; // return empty to remove this entry
					}
					
					int seq = PointIndex.split(summit)[3]; // checkpoints may already have a seq# set
					long altSummit = PointIndex.clone(summit, seq + UD(k.up));
					long altSaddle = PointIndex.clone(summit, -UD(k.up));
					
					additions.add(new PseudoEdge(k.saddle, summit, k.up, v[0].tag));
					additions.add(new PseudoEdge(k.saddle, altSummit, k.up, v[1].tag));
					additions.add(new PseudoEdge(altSaddle, summit, k.up, Edge.TAG_NULL));
					additions.add(new PseudoEdge(altSaddle, altSummit, k.up, Edge.TAG_NULL));
					return true;
				}
			};

			// perform each cleaning operation in sequence (one full pass each); ordering is important!
			for (TopologyCleaner cleaner : new TopologyCleaner[] {unconnected, multisaddle, ring}) {
				List<PseudoEdge> additions = new ArrayList<PseudoEdge>();
				for (Iterator<Map.Entry<SaddleAndDir, Set<SummitAndTag>>> it = processedBySaddle.entrySet().iterator(); it.hasNext(); ) {
					Map.Entry<SaddleAndDir, Set<SummitAndTag>> e = it.next();
					if (cleaner.clean(additions, e.getKey(), e.getValue())) {
						it.remove();
					}
				}
				for (PseudoEdge change : additions) {
					processedBySaddle.get(change.k).add(change.v);					
				}
			}
			
			List<Edge> upEdges = new ArrayList<Edge>();
			List<Edge> downEdges = new ArrayList<Edge>();
			for (Iterator<Map.Entry<SaddleAndDir, Set<SummitAndTag>>> it = processedBySaddle.entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<SaddleAndDir, Set<SummitAndTag>> e = it.next();
				it.remove();
				
				SaddleAndDir k = e.getKey();
				SummitAndTag[] v = unpackForSaddle(e.getValue(), 2);
				(k.up ? upEdges : downEdges).add(new Edge(v[0].summit, v[1].summit, k.saddle, v[0].tag, v[1].tag));
			}
			Map<Boolean, List<Edge>> allEdges = new HashMap<Boolean, List<Edge>>();
			allEdges.put(true, upEdges);
			allEdges.put(false, downEdges);
			return allEdges;
		}
		
		SummitAndTag[] unpackForSaddle(Set<SummitAndTag> sts, int n) {
			assert sts.size() == n;
			return sts.toArray(new SummitAndTag[n]);
		}
	}

	static class Builder extends WorkerPool<ChunkInput, ChunkOutput> {
//	static class Builder extends WorkerPoolDebug<ChunkInput, ChunkOutput> {

		int numWorkers;
		Map<Prefix, Set<DEMFile>> coverage;
		Map<Boolean, Map<Prefix, Set<Long>>> internalCheckpoints; // warning: we keep this always in memory and never purge
		Map<Boolean, Set<Long>> pendingCheckpoints;
		
		public Builder(int numWorkers, Map<Prefix, Set<DEMFile>> coverage) {
			this.numWorkers = numWorkers;
			this.coverage = coverage;
			
			pendingCheckpoints = new HashMap<Boolean, Set<Long>>();
			internalCheckpoints = new HashMap<Boolean, Map<Prefix, Set<Long>>>();
			for (boolean up : new boolean[] {true, false}) {
				pendingCheckpoints.put(up, new HashSet<Long>());
				internalCheckpoints.put(up, new HashMap<Prefix, Set<Long>>());
			}
		}

		public void firstRound() {
			Set<Prefix> chunks = new HashSet<Prefix>();
			for (Prefix p : coverage.keySet()) {
				chunks.add(new Prefix(p, CHUNK_SIZE_EXP));
			}
			
			for (Prefix chunk : chunks) {
				internalCheckpoints.get(true).put(chunk, new HashSet<Long>());
				internalCheckpoints.get(false).put(chunk, new HashSet<Long>());
			}
			
			launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, null, null);
				}
			}));
		}
		
		public ChunkOutput process(ChunkInput input) {
			return new ChunkProcessor(input).build();
		}

		public void postprocess(int i, ChunkOutput output) {
			postprocessChunk(i, output.chunkPrefix, output.byDir[0]);
			postprocessChunk(i, output.chunkPrefix, output.byDir[1]);
		}

		public void postprocessChunk(int i, Prefix prefix, ChunkOutputForDir output) {
			writeEdges(output.network, output.up);
			
			internalCheckpoints.get(output.up).put(prefix, output.internalCheckpoints);
			pendingCheckpoints.get(output.up).addAll(output.pendingCheckpoints);

			Logging.log(String.format("%d %s %s %d %d %d", i+1, prefix, output.up ? "U" : "D", output.network.size(),
					output.internalCheckpoints.size(), output.pendingCheckpoints.size()));
		}
		
		public static void writeEdges(List<Edge> edges, final boolean up) {
			Map<Prefix, DataOutputStream> f = new DefaultMap<Prefix, DataOutputStream>() {
				public DataOutputStream defaultValue(Prefix prefix) {
					try {
						return new DataOutputStream(new FileOutputStream(FileUtil.segmentPath(up, prefix, FileUtil.PHASE_RAW), true));
					} catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				}
			};
		
			for (Edge e : edges) {
				Prefix bucket1 = _chunk(e.a);
				Prefix bucket2 = (e.pending() ? null : _chunk(e.b));
				
				e.write(f.get(bucket1));
				if (!bucket1.equals(bucket2) && bucket2 != null) {
					e.write(f.get(bucket2));
				}
			}
		
			for (OutputStream o : f.values()) {
				try {
					o.close();
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}				
			}
		}
		
		public boolean needsAnotherRound() {
			Logging.log("before " + pendingCheckpoints.get(true).size() + " " + pendingCheckpoints.get(false).size());
			for (boolean up : new boolean[] {true, false}) {
				// think i could use set.removeAll(), but not sure if less efficient when set being
				// removed from is much smaller than set being removed
				for (Iterator<Long> it = pendingCheckpoints.get(up).iterator(); it.hasNext(); ) {
					long chk = it.next();
					if (internalCheckpoints.get(up).get(_chunk(chk)).contains(chk)) {
						it.remove();
					}
				}
			}
			Logging.log("after " + pendingCheckpoints.get(true).size() + " " + pendingCheckpoints.get(false).size());
			return (pendingCheckpoints.get(true).size() +
					pendingCheckpoints.get(false).size() > 0);
		}
		
		public void nextRound() {
			int checkpointsInMemory = 0;
			for (Map<Prefix, Set<Long>> m : internalCheckpoints.values()) {
				for (Set<Long> ms : m.values()) {
					checkpointsInMemory += ms.size();
				}
			}
			Logging.log("INFO: " + checkpointsInMemory + " internal checkpoints cached");
			
			final Map<Prefix, List<Long>> upChunks = partitionPending(true);
			final Map<Prefix, List<Long>> downChunks = partitionPending(false);

			Set<Prefix> chunks = new HashSet<Prefix>();
			chunks.addAll(upChunks.keySet());
			chunks.addAll(downChunks.keySet());

			this.launch(NUM_WORKERS, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, upChunks, downChunks);
				}
			}));
		}
		
		ChunkInput makeInput(Prefix p, Map<Prefix, List<Long>> pendingUp, Map<Prefix, List<Long>> pendingDown) {
			ChunkInput ci = new ChunkInput();
			ci.chunkPrefix = p;
			ci.fullMode = (pendingUp == null && pendingDown == null);
			ci.coverage = (HashMap)((HashMap)coverage).clone();
			ci.byDir = new ChunkInputForDir[] {
				makeInputDir(p, true, pendingUp),	
				makeInputDir(p, false, pendingDown)
			};
			return ci;
		}
		
		ChunkInputForDir makeInputDir(Prefix p, boolean up, Map<Prefix, List<Long>> pending) {
			ChunkInputForDir cid = new ChunkInputForDir();
			cid.internalCheckpoints = internalCheckpoints.get(up).get(p);
			cid.pendingCheckpoints = (pending != null ? pending.get(p) : null);
			if (cid.pendingCheckpoints == null) {
				cid.pendingCheckpoints = new ArrayList<Long>();
			}
			return cid;
		}
		
		// note: wipes out pending list
		Map<Prefix, List<Long>> partitionPending(boolean up) {
			Map<Prefix, List<Long>> byChunk = new DefaultMap<Prefix, List<Long>>() {
				public List<Long> defaultValue(Prefix key) {
					return new ArrayList<Long>();
				}
			};			
			for (Long ix : pendingCheckpoints.get(up)) {
				byChunk.get(_chunk(ix)).add(ix);
			}
			pendingCheckpoints.put(up, new HashSet<Long>());
			return byChunk;
		}
	}
	
	static int UD(boolean up) {
		return (up ? 1 : -1);
	}
}
	
// verifications:	
// does not refer to self (handled by assert in Edge)
// only saddles can connect to null (handled by assert in Edge)
// every saddle listed must be unique
// peak must be higher than all connecting saddles
// saddle must be lower than two connecting peaks
