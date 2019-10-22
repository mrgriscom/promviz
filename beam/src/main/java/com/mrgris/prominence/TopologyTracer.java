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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mrgris.prominence.MeshPoint.Lead;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.DefaultMap;
import com.mrgris.prominence.util.Util;
import com.mrgris.prominence.util.WorkerUtils;

// TODO - merge with TopologyBuilder

public class TopologyTracer extends DoFn<KV<Prefix, Iterable<Long>>, KV<Long, KV<Integer, List<Long>>>> {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyTracer.class);

    boolean up;
    PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput;
    //TupleTag<Edge> sideOutput;
        
    public TopologyTracer(boolean up, PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput) {//, TupleTag<Edge> sideOutput) {
    	this.up = up;
    	this.coverageSideInput = coverageSideInput;
    	//this.sideOutput = sideOutput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
    	Prefix pf = c.element().getKey();
    	Set<Long> saddles = Sets.newHashSet(c.element().getValue());
    	new Builder(c.sideInput(coverageSideInput), up) {
    		void emitLead(boolean up, Lead lead) {
    			List<Long> path = new ArrayList<>();
    			for (Point p : lead.trace) {
    				path.add(p != null ? p.ix : PointIndex.NULL);
    			}
    			c.output(KV.of(lead.p0.ix, KV.of(lead.i, path)));
//    			if (up) {
//    				c.output(edge);
//    			} else {
//    				c.output(sideOutput, edge);
//    			}
    		}
    	}.build(pf, saddles);
    }    	
    
    @Setup
    public void setup() {
    	WorkerUtils.initializeGDAL();
    }
    
    public static abstract class Builder {
    	PagedElevGrid mesh;
    	List<Lead> pending; // TODO group by common (term, up)?
    	boolean up;
    	
    	public Builder(Map<Prefix, Iterable<DEMFile>> coverage, boolean up) {
    		this.up = up; // TODO both dirs should be processed simultaneously
    		this.mesh = _createMesh(coverage); // TODO implement LRU for mesh since we don't checkpoint for now
    		this.pending = new ArrayList<>();
    	}
		
    	abstract void emitLead(boolean up, Lead edge);

    	void build(Prefix pf, Set<Long> saddles) {
    		// TODO switch to just needed pages (need to add buffer though)
//			final Set<Prefix> pages = new HashSet<Prefix>();
//    		for (long ix : saddles) {
//				pages.add(PagedElevGrid.segmentPrefix(ix));
//			}
//			mesh.bulkLoadPage(pages);
    		mesh.loadForPrefix(pf, 1);
    		
			Set<Long> pureSaddles = new HashSet<>();
			for (long ix : saddles) {
				pureSaddles.add(PointIndex.clone(ix, 0));
			}
    		for (long ix : pureSaddles) {
    			MeshPoint p = mesh.get(ix);
    			int pointClass = p.classify(mesh);
    			if (pointClass != MeshPoint.CLASS_SADDLE) {
    				throw new RuntimeException(""+ix);
    			}    			
    			processSaddle(p, saddles);
    		}
    		
    		while (pending.size() > 0) {
    			processPending();
    		}
    					
			this.mesh.destroy();
       	}
    	
		void processSaddle(MeshPoint saddle, Set<Long> saddles) {
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
					if (lead.up != up || !saddles.contains(lead.p0.ix)) {
						continue;
					}
					lead.enableTracing();
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
				
				lead.setNextP(lead.follow(mesh).p);

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
			if (cr.status == ChaseResult.STATUS_INDETERMINATE) {
				lead.setNextP(null);
			}
			
			emitLead(up, lead);
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
