package com.mrgris.prominence;

import org.apache.beam.sdk.transforms.DoFn;

import com.mrgris.prominence.Prominence.Front;
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.Searcher;

public class PromFinalize extends DoFn<Iterable<AvroFront>, PromFact> {
	
	boolean up;
	double cutoff;
	
	public PromFinalize(boolean up, double cutoff) {
		this.up = up;
		this.cutoff = cutoff;
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
    	}.finalizePending();
    }    	
}
