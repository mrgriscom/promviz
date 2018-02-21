/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mrgris.prominence;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.dem.DEMFile;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class ProminencePipeline {
  private static final Logger LOG = LoggerFactory.getLogger(ProminencePipeline.class);
      
  public static void main(String[] args) {
	
	// TODO: custom options and validation
	// --output=gs://mrgris-dataflow-test/output-file-prefix
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).create());
    
    PCollection<KV<Prefix, DEMFile>> pageFileMapping = TopologyNetworkPipeline.makePageFileMapping(p);
    final PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage = pageFileMapping.apply(View.asMultimap());

    // TODO separate but identical sub-pipelines for the up and down networks

    PCollection<Edge> network = p.apply(AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/network-up-*"));
    PCollection<KV<Long, Iterable<Long>>> minimalFronts = network.apply(ParDo.of(new DoFn<Edge, KV<Long, Long>>() {
	      @ProcessElement
	      public void processElement(ProcessContext c) {
	    	  Edge e = c.element();
	    	  c.output(KV.of(e.a, e.saddle));
	    	  if (!e.pending()) {
		    	  c.output(KV.of(e.b, e.saddle));
	    	  }
	      }
    })).apply(GroupByKey.create());
    PCollection<KV<Prefix, Iterable<KV<Long, Iterable<Long>>>>> initialChunks = minimalFronts.apply(
    		MapElements.into(new TypeDescriptor<KV<Prefix, KV<Long, Iterable<Long>>>>() {}).via(
    				front -> KV.of(new Prefix(front.getKey(), TopologyNetworkPipeline.CHUNK_SIZE_EXP), front)))
    		.apply(GroupByKey.create());
    final TupleTag<Edge> promFactsTag = new TupleTag<Edge>(){};
    final TupleTag<Edge> pendingFrontsTag = new TupleTag<Edge>(){};    
    PCollection<PromFact> pfacts = initialChunks.apply(ParDo.of(new Prominence(true, 20., pageCoverage /*, promFactsTag, pendingFrontsTag*/))
    		.withSideInputs(pageCoverage)
    		/*.withOutputTags(promFactsTag, TupleTagList.of(pendingFrontsTag))*/);
        
    pfacts.apply("dumprawfacts",
    	    AvroIO.write(PromFact.class).to("gs://mrgris-dataflow-test/factstest"));

    
    
    // read edges
    // turn edges into fronts
    // parition fronts by chunks
    // -> promfacts, pending fronts, mst network + overrides
    
    
    
    // coalesce steps -- must pre-populated all the way to global, even if many are no-ops
    // what if chunk size is such that there is only one processing level (extreme edge case but try to handle it)
    // actually 'final level' is when there is only one remaining chunk, so useful to catch this early and avoid redundant work
    
    p.run();
    
    
    
    

/*
		public void coalesce() {
			this.level += COALESCE_STEP;
			this.chunks = TopologyBuilder.clusterPrefixes(this.chunks, this.level);
			Logging.log(chunks.size() + " chunks @ L" + level);
			
			launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, up, cutoff);
				}
			}));
		}
	*/
    
    
    
    
    
    
    
    
    
    
    ////////////////////////////
    /*
     * output format
     * 
     * 
     * 
     * primary key = s2 code for summit

type - summit or sink
Point p         // summit
Point saddle    // key saddle
Point thresh    // next highest ground - if null, represents pending prominence only; will be an interpolated point, but in interim stages will be the next highest gridpoint
Point pthresh   // first higher point of notable prominence
Point parent    // prominence parent - if null, no parent found yet

(should punt on these for the moment)
path_thresh     // path to threshold point; if pending, path is to eow
path_parent     // path to parent; if none found, path is to eow

int child_ix    // order number among this parent's children
	
height_subsaddles*
domain_subsaddles*

subsaddle {
  Point saddle
  Point forPeak
  type {DOMAIN, HEIGHT}
}
// the same subsaddle point can be both types

point {
  latlon
  s2
  elevation
  prominence (opt)
  // include isodist?
}

// TOOD runoff
     */
  }
}
