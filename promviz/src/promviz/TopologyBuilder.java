package promviz;

import java.io.ByteArrayOutputStream;
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

			public void postprocess(ChunkOutput output) {
				postprocessChunk(output);
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
		List<Edge> upNetwork;
		List<Edge> downNetwork;
		Set<Long> upCheckpoint;
		Set<Long> downCheckpoint;
	}
	
	static class ChunkProcessor {
		Prefix prefix;
		Map<Prefix, Set<DEMFile>> coverage;
		
		PagedElevGrid mesh;
		Map<SaddleAndDir, List<SummitAndTag>> processedBySaddle;
		List<Lead> pending; // TODO group by common (term, up)?
		Set<Long> upCheckpoints;
		Set<Long> downCheckpoints;
		
		public ChunkProcessor(ChunkInput input) {
			this.prefix = input.chunkPrefix;
			this.coverage = input.coverage;
			
			processedBySaddle = new DefaultMap<SaddleAndDir, List<SummitAndTag>>() {
				public List<SummitAndTag> defaultValue(SaddleAndDir key) {
					return new ArrayList<SummitAndTag>();
				}
			};
			pending = new ArrayList<Lead>();
			upCheckpoints = new HashSet<Long>();
			downCheckpoints = new HashSet<Long>();
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
			
			while (pending.size() > 0) {
				processPending();
			}
			
			Map<Boolean, List<Edge>> topology = normalizeTopology();
			
			ChunkOutput output = new ChunkOutput();
			output.chunkPrefix = prefix;
			output.upNetwork = topology.get(true);
			output.downNetwork = topology.get(false);
			output.upCheckpoint = upCheckpoints;
			output.downCheckpoint = downCheckpoints;
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
				if (result.status == ChaseResult.STATUS_INTERIM && !checkpointExists(result.lead)) {
					(lead.up ? upCheckpoints : downCheckpoints).add(result.lead.p.ix);
					Lead resume = result.lead.follow(mesh);
					resume.fromCheckpoint = true;
					processLead(resume);
				}
			} else {
				pending.add(result.lead);
			}
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
			return !prefix.isParent(ix) // assume checkpointing outside current chunk area is handled externally 
					|| (up ? upCheckpoints : downCheckpoints).contains(ix);
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
				v.summit = PointIndex.clone(v.summit, lead.up ? 1 : -1);
			}
			processedBySaddle.get(k).add(v);
	
			if (cr.lead.fromCheckpoint) {
				long checkpoint = PointIndex.clone(k.saddle, lead.up ? 1 : -1);
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
			
			public boolean equals(Object o) { throw new UnsupportedOperationException(); }
			public int hashCode() { throw new UnsupportedOperationException(); }
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
			List<PseudoEdge> clean(SaddleAndDir k, List<SummitAndTag> v);
		}
		
		public Map<Boolean, List<Edge>> normalizeTopology() {
			// note: we try to release memory from the internal data structures as their objects
			// are consumed, but the memory taken by the container itself does not shrink
						
			// define several cleaning operations
			TopologyCleaner unconnected = new TopologyCleaner() {
				public List<PseudoEdge> clean(SaddleAndDir k, List<SummitAndTag> v) {
					// remove saddles that are unconnected to the rest of the network
					boolean allPending = true;
					for (SummitAndTag st : v) {
						if (st.summit != PointIndex.NULL) {
							allPending = false;
							break;
						}
					}
					return (allPending ? new ArrayList<PseudoEdge>() : null);
				}
			};
			TopologyCleaner multisaddle = new TopologyCleaner() {
				public List<PseudoEdge> clean(SaddleAndDir k, List<SummitAndTag> v) {
					// break up multisaddles
					if (v.size() <= 2) {
						return null;
					}
					
					Collections.sort(v, new Comparator<SummitAndTag>() {
						public int compare(SummitAndTag a, SummitAndTag b) {
							return Integer.compare(a.tag, b.tag);
						}
					});
		
					List<PseudoEdge> edges = new ArrayList<PseudoEdge>();
					long vPeak = PointIndex.clone(k.saddle, 1);
					for (int i = 0; i < v.size(); i++) {
						SummitAndTag st = v.get(i);
						long vSaddle = PointIndex.clone(k.saddle, -i); // would be nice to randomize the saddle ordering
						
						// the disambiguation pattern is not symmetrical between the 'up' and 'down' networks;
						// this is the cost of making them consistent with each other
						if (k.up) {
							edges.add(new PseudoEdge(vSaddle, st.summit, k.up, st.tag));
							edges.add(new PseudoEdge(vSaddle, vPeak, k.up, Edge.TAG_NULL));
						} else {
							SummitAndTag st_prev = v.get(Util.mod(i - 1, v.size()));
							edges.add(new PseudoEdge(vSaddle, st.summit, k.up, st.tag));
							edges.add(new PseudoEdge(vSaddle, st_prev.summit, k.up, st_prev.tag));
						}
					}
					return edges;
				}
			};
			TopologyCleaner ring = new TopologyCleaner() {
				public List<PseudoEdge> clean(SaddleAndDir k, List<SummitAndTag> v) {
					// split rings (both saddle edges lead to same point)
					assert v.size() == 2;
					if (v.get(0).summit != v.get(1).summit) {
						return null;
					}
					List<PseudoEdge> edges = new ArrayList<PseudoEdge>();

					long summit = v.get(0).summit;
					if (summit == PointIndex.NULL) {
						// although the first cleaner checked this, it can crop up again
						// in the 'down' network for multisaddles with 2 leads indeterminate
						return edges; // return empty to remove this entry
					}
					
					long altSummit = PointIndex.clone(summit, k.up ? 1 : -1);
					long altSaddle = PointIndex.clone(summit, k.up ? -1 : 1);
					
					edges.add(new PseudoEdge(k.saddle, summit, k.up, v.get(0).tag));
					edges.add(new PseudoEdge(k.saddle, altSummit, k.up, v.get(1).tag));
					edges.add(new PseudoEdge(altSaddle, summit, k.up, Edge.TAG_NULL));
					edges.add(new PseudoEdge(altSaddle, altSummit, k.up, Edge.TAG_NULL));
					return edges;
				}
			};

			// perform each cleaning operation in sequence (one full pass each); ordering is important!
			for (TopologyCleaner cleaner : new TopologyCleaner[] {unconnected, multisaddle, ring}) {
				List<PseudoEdge> additions = new ArrayList<PseudoEdge>();
				for (Iterator<Map.Entry<SaddleAndDir, List<SummitAndTag>>> it = processedBySaddle.entrySet().iterator(); it.hasNext(); ) {
					Map.Entry<SaddleAndDir, List<SummitAndTag>> e = it.next();
					List<PseudoEdge> changes = cleaner.clean(e.getKey(), e.getValue());
					
					if (changes != null) {
						additions.addAll(changes);
						it.remove();
					}
				}
				for (PseudoEdge change : additions) {
					processedBySaddle.get(change.k).add(change.v);					
				}
			}
			
			List<Edge> upEdges = new ArrayList<Edge>();
			List<Edge> downEdges = new ArrayList<Edge>();
			for (Iterator<Map.Entry<SaddleAndDir, List<SummitAndTag>>> it = processedBySaddle.entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<SaddleAndDir, List<SummitAndTag>> e = it.next();
				it.remove();
				
				SaddleAndDir k = e.getKey();
				List<SummitAndTag> v = e.getValue();
				assert v.size() == 2;
				SummitAndTag a = v.get(0);
				SummitAndTag b = v.get(1);
				(k.up ? upEdges : downEdges).add(new Edge(a.summit, b.summit, k.saddle, a.tag, b.tag));
			}
			Map<Boolean, List<Edge>> allEdges = new HashMap<Boolean, List<Edge>>();
			allEdges.put(true, upEdges);
			allEdges.put(false, downEdges);
			return allEdges;
		}
	}

	static int i = 1;
	public static void postprocessChunk(ChunkOutput output) {
		writeEdges(output.upNetwork, true);
		writeEdges(output.downNetwork, false);
		
		Logging.log((i++) + " " + output.chunkPrefix.toString() + " " + output.upNetwork.size() + " " + output.downNetwork.size());
		
		// for each edge in up/down networks
		// write each edge to chunkfile for each summit (only once if in same chunk)
		
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
			Prefix bucket1 = new Prefix(e.a, CHUNK_SIZE_EXP);
			Prefix bucket2 = (e.pending() ? null : new Prefix(e.b, CHUNK_SIZE_EXP));
			
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

}
	
// verifications:	
// does not refer to self (handled by assert in Edge)
// only saddles can connect to null (handled by assert in Edge)
// every saddle listed must be unique
// peak must be higher than all connecting saddles
// saddle must be lower than two connecting peaks
