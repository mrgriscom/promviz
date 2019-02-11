package com.mrgris.prominence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.MeshPoint.Lead;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.DefaultMap;
import com.mrgris.prominence.util.Util;
import com.mrgris.prominence.util.WorkerUtils;

public class TopologyBuilder extends DoFn<Prefix, Edge> {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
	
    PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput;
    TupleTag<Edge> sideOutput;
    
    public TopologyBuilder(PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput, TupleTag<Edge> sideOutput) {
    	this.coverageSideInput = coverageSideInput;
    	this.sideOutput = sideOutput;
    }
	        
    @Setup
    public void setup() {
    	WorkerUtils.initializeGDAL();
    }
    
    public static abstract class Builder {
    	Prefix prefix;
    	PagedElevGrid mesh;
    	Map<SaddleAndDir, SummitAndTag> halfEdges;
    	List<Lead> pending; // TODO group by common (term, up)?
    	
    	public Builder(Prefix prefix, Map<Prefix, Iterable<DEMFile>> coverage) {
    		this.prefix = prefix;
    		this.mesh = _createMesh(coverage); // TODO implement LRU for mesh since we don't checkpoint for now
    		this.halfEdges = new HashMap<>();
    		this.pending = new ArrayList<>();
    	}
		
    	abstract void emitEdge(boolean up, Edge edge);

    	void build() {
    		Iterable<DEMFile.Sample> points = mesh.loadForPrefix(prefix, 1);
    		
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
    		
    		while (pending.size() > 0) {
    			processPending();
    		}
    					
			this.mesh.destroy();
       	}
    	
		void processSaddle(MeshPoint saddle) {
			Lead[][] leads = saddle.leads(mesh);
			if (leads[0].length > 2) {
				// multi-saddle
				
				Map<Integer, Lead> byTraceNum = new HashMap<>();
				for (Lead[] dirLeads : leads) {
					for (Lead l : dirLeads) {
						byTraceNum.put(l.i, l);
					}
				}
				
				List<Lead> newLeadsUp = new ArrayList<>();
				List<Lead> newLeadsDown = new ArrayList<>();
				
				// TODO: randomize the connections
				for (int i = 0; i < leads[0].length - 1; i++) {
					int traceUpA = 2*i;
					int traceUpB = 2*(i+1);
					int traceDownA = 2*i + 1;
					int traceDownB = 2*leads[0].length - 1;
					
					long vSaddle = PointIndex.clone(saddle.ix, -i);
					MeshPoint vSaddlePt = new MeshPoint(vSaddle, saddle.elev, saddle.isodist);
					
					newLeadsUp.add(new Lead(true, vSaddlePt, byTraceNum.get(traceUpA).p, traceUpA));
					newLeadsUp.add(new Lead(true, vSaddlePt, byTraceNum.get(traceUpB).p, traceUpB));
					newLeadsDown.add(new Lead(false, vSaddlePt, byTraceNum.get(traceDownA).p, traceDownA));
					newLeadsDown.add(new Lead(false, vSaddlePt, byTraceNum.get(traceDownB).p, traceDownB));
				}
				
				leads[0] = newLeadsUp.toArray(new Lead[0]);
				leads[1] = newLeadsDown.toArray(new Lead[0]);
			}
			
			for (Lead[] dirLeads : leads) {				
				for (Lead lead : dirLeads) {
					processLead(lead);
				}
			}
		}
		
		void processLead(Lead lead) {
			ChaseResult result = chase(lead);
			if (result.status == ChaseResult.STATUS_PENDING) {
				pending.add(result.lead);
			} else {				
				processResult(result);
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
			//static final int STATUS_INTERIM = 4;        // reached a 'checkpoint' due to excessive chase length
			
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
				if (status > 0) {
					break;
				}
				
				lead.p = lead.follow(mesh).p;
				lead.len++;

				if (loopFailsafe++ > 1e6) {
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
			}
			return 0;
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
			}
			
			if (halfEdges.containsKey(k)) {
				SummitAndTag otherHalf = halfEdges.remove(k);
				if (v.summit == PointIndex.NULL && otherHalf.summit == PointIndex.NULL) {
					// saddle not connected to anything; drop it (just for cleanliness -- the rest of the pipeline would handle it ok)
				} else {
					Edge e = new Edge(v.summit, otherHalf.summit, k.saddle, v.tag, otherHalf.tag);
					emitEdge(k.up, e);
				}
			} else {
				halfEdges.put(k, v);
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
				return Objects.hash(saddle, up);
			}
		}

		static class SummitAndTag {
			long summit;
			int tag;
			
			SummitAndTag(long summit, int tag) {
				this.summit = summit;
				this.tag = tag;
				if (tag == Edge.TAG_NULL) {
					throw new RuntimeException();
				}
			}
			
			public boolean equals(Object o) {
				SummitAndTag st = (SummitAndTag)o;
				return this.summit == st.summit && this.tag == st.tag;
			}
			
			public int hashCode() {
				return Objects.hash(summit, tag);
			}
		}

		static class HalfEdge {
			SaddleAndDir k;
			SummitAndTag v;
			
			public HalfEdge(SaddleAndDir k, SummitAndTag v) {
				this.k = k;
				this.v = v;
			}

			public HalfEdge(long saddle, long summit, boolean dir, int tag) {
				this(new SaddleAndDir(saddle, dir), new SummitAndTag(summit, tag));
			}

			public boolean equals(Object o) { throw new UnsupportedOperationException(); }
			public int hashCode() { throw new UnsupportedOperationException(); }
		}

   		boolean inChunk(long ix) {
			return prefix.isParent(ix);	
		}
    }
    
    @ProcessElement
    public void processElement(ProcessContext c) {
    	new Builder(c.element(), c.sideInput(coverageSideInput)) {
    		void emitEdge(boolean up, Edge edge) {
    			if (up) {
    				c.output(edge);
    			} else {
    				c.output(sideOutput, edge);
    			}
    		}
    	}.build();
    }    	

	static PagedElevGrid _createMesh(Map<Prefix, Iterable<DEMFile>> coverage) {
		int maxSize = 1000 * Util.pow2(2*TopologyNetworkPipeline.PAGE_SIZE_EXP); /*(int)Math.max(
				1.5 * Util.pow2(2 * StarterPipeline.CHUNK_SIZE_EXP),
				1.25 * Math.pow(Util.pow2(StarterPipeline.CHUNK_SIZE_EXP) + 2 * PagedElevGrid.pageDim(), 2)
			);*/
		return new PagedElevGrid(coverage, maxSize);
	}
	
}
	
// verifications:	
// every saddle listed must be unique
// peak must be higher than all connecting saddles
// saddle must be lower than two connecting peaks
