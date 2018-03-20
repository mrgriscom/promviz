package com.mrgris.prominence;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.mrgris.prominence.Prominence.Front;
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.Searcher;

public class Prominence2 extends DoFn<KV<Prefix, Iterable<AvroFront>>, PromFact> {
	
	boolean up;
	double cutoff;
	TupleTag<AvroFront> pendingFrontsTag;
	TupleTag<Edge> mstTag;
	
	public Prominence2(boolean up, double cutoff, TupleTag<AvroFront> pendingFrontsTag, TupleTag<Edge> mstTag) {
		this.up = up;
		this.cutoff = cutoff;
		this.pendingFrontsTag = pendingFrontsTag;
		this.mstTag = mstTag;
	}
	
    @ProcessElement
    public void processElement(ProcessContext c) {
    	Prefix prefix = c.element().getKey();
    	Iterable<AvroFront> fronts = c.element().getValue();
    	
    	new Searcher(up, cutoff, fronts) {
    		public void emitFact(PromFact pf) {
    			c.output(pf);
    		}
    		
    		public void emitPendingFront(Front f) {
    			c.output(pendingFrontsTag, new AvroFront(f));
    		}
    		
    		public void emitBacktraceEdge(Edge e) {
    			c.output(mstTag, e);
    		}
    	}.search();
    }    	
	

}
