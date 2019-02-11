package com.mrgris.prominence;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import com.mrgris.prominence.Prominence.Front;
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.Searcher;

public class PromFinalize extends DoFn<Iterable<AvroFront>, PromFact> {
	
	boolean up;
	double cutoff;
	TupleTag<Edge> mstTag;

	public PromFinalize(boolean up, double cutoff, TupleTag<Edge> mstTag) {
		this.up = up;
		this.cutoff = cutoff;
		this.mstTag = mstTag;
	}
	
    @ProcessElement
    public void processElement(ProcessContext c) {
    	Iterable<AvroFront> fronts = c.element();
    	
    	new Searcher(up, cutoff, fronts) {
    		public void emitFact(PromFact pf) {
    			c.output(pf);
    		}
    		
    		public void emitPendingFront(Front f) {
    			throw new RuntimeException();
    		}
    		
    		public void emitBacktraceEdge(Edge e) {
    			c.output(mstTag, e);
    		}
    	}.finalizePending();
    }    	
}
