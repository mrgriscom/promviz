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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.beam.runners.direct.repackaged.runners.core.java.repackaged.com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.mrgris.prominence.Edge.HalfEdge;
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.PromPair;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.ReverseComparator;

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
      
  static final TupleTag<PromFact> promFactsTag = new TupleTag<PromFact>(){};
  static final TupleTag<Edge> mstTag = new TupleTag<Edge>(){};
  
  static PCollection<PromFact> consolidatePromFacts(PCollectionList<PromFact> promFacts) {
	  return promFacts.apply(Flatten.pCollections()).apply(MapElements.into(new TypeDescriptor<KV<Long, PromFact>>() {}).via(pf -> KV.of(pf.p.ix, pf)))
	    	    .apply(Combine.perKey(new SerializableFunction<Iterable<PromFact>, PromFact>() {
	    		  	  @Override
	    		  	  public PromFact apply(Iterable<PromFact> input) {
	    		  	    PromFact combined = new PromFact();
	    		  	    for (PromFact pf : input) {
	    		  	    	if (combined.p == null) {
	    		  	    		combined.p = pf.p;
	    		  	    	}
	    		  	    	// debug -- what facts are emitting elev 0 for p?
	    		  	    	if (combined.p.elev == 0 && pf.p.elev != 0) {
	    		  	    		combined.p = pf.p;
	    		  	    	}
	    		  	    	if (pf.saddle != null) {
	    		  	    		combined.saddle = pf.saddle;
	    		  	    	}
	    		  	    	if (pf.thresh != null) {
	    		  	    		combined.thresh = pf.thresh;
	    		  	    	}
	    		  	    	if (pf.pthresh != null) {
	    		  	    		combined.pthresh = pf.pthresh;
	    		  	    	}
	    		  	    	if (pf.parent != null) {
	    		  	    		combined.parent = pf.parent;
	    		  	    	}
	    		  	    	if (pf.promRank != null) {
	    		  	    		combined.promRank = pf.promRank;
	    		  	    	}
	    		  	    	combined.elevSubsaddles.addAll(pf.elevSubsaddles);
	    		  	    	combined.promSubsaddles.addAll(pf.promSubsaddles);
	    		  	    	if (pf.threshPath != null) {
	    		  	    		combined.threshPath = pf.threshPath;
	    		  	    	}
	    		  	    	if (pf.parentPath != null) {
	    		  	    		combined.parentPath = pf.parentPath;
	    		  	    	}
	    		  	    	if (pf.domainBoundary != null) {
	    		  	    		combined.domainBoundary = pf.domainBoundary;
	    		  	    	}
	    		  	    }
	    		  	    return combined;
	    		  	  }
	    	  	  })).apply(Values.create());
  }
  
  static Prefix chunkingPrefix(long ix, int chunkLevel) {
	  int offset = 0;  // in base half-chunks
	  int level = TopologyNetworkPipeline.CHUNK_SIZE_EXP;
	  while (level < chunkLevel) {
		  offset = (1 << Prominence.COALESCE_STEP) * offset + 1;
		  level += Prominence.COALESCE_STEP;
	  }
	  offset *= (1 << (TopologyNetworkPipeline.CHUNK_SIZE_EXP - 1));
	  int[] pcs = PointIndex.split(ix);
	  // eventually have to think about overflow here -- offset approaches 1/6 of chunk size
	  pcs[1] -= offset;
	  pcs[2] -= offset;	  
	  return new Prefix(PointIndex.make(pcs[0], pcs[1], pcs[2]), chunkLevel);
  }
  
  public static PCollectionTuple dirPipeline(Pipeline p, boolean up, String networkPath, PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage) {
	    // TODO verify edges since read from outside source
	  PCollection<Edge> network = p.apply(AvroIO.read(Edge.class).from(networkPath));
	  PCollection<KV<Long, Iterable<HalfEdge>>> minimalFronts = network.apply(ParDo.of(new DoFn<Edge, KV<Long, HalfEdge>>() {
		      @ProcessElement
		      public void processElement(ProcessContext c) {
		    	  Edge e = c.element();
		    	  for (HalfEdge he : e.split()) {
		    		  if (he != null) {
				    	  c.output(KV.of(he.p, he));		    			  
		    		  }
		    	  }
		      }
	    })).apply(GroupByKey.create());
	    // TODO insert stage that generates the fronts before invoking searcher? or too much overhead?
	    PCollection<Iterable<KV<Long, Iterable<HalfEdge>>>> initialChunks = minimalFronts.apply(
	    		MapElements.into(new TypeDescriptor<KV<Prefix, KV<Long, Iterable<HalfEdge>>>>() {}).via(
	    				front -> KV.of(chunkingPrefix(front.getKey(), TopologyNetworkPipeline.CHUNK_SIZE_EXP), front)))
	    		.apply(GroupByKey.create()).apply(Values.create());
	    final TupleTag<AvroFront> pendingFrontsTag = new TupleTag<AvroFront>(){};   
	    PCollectionTuple searchOutput = initialChunks.apply(ParDo.of(new Prominence(up, 20., pageCoverage, pendingFrontsTag, mstTag))
	    		.withSideInputs(pageCoverage)
	    		.withOutputTags(promFactsTag, TupleTagList.of(pendingFrontsTag).and(mstTag)));

	    PCollectionList<PromFact> promFacts = PCollectionList.of(searchOutput.get(promFactsTag)); 
	    PCollectionList<KV<Long, KV<Integer, Edge>>> mstEdges = PCollectionList.of(searchOutput.get(mstTag).apply(
	    				MapElements.into(new TypeDescriptor<KV<Long, KV<Integer, Edge>>>() {})
	    	    		.via(e -> KV.of(e.saddle, KV.of(TopologyNetworkPipeline.CHUNK_SIZE_EXP, e)))));
	    
	    // TODO add offset during coalescing to avoid overlapping boundaries across multiple steps
	    int chunkSize = TopologyNetworkPipeline.CHUNK_SIZE_EXP;
	    while (chunkSize < 20) { // NOT GLOBAL!!!   TODO check this later
	      chunkSize += Prominence.COALESCE_STEP;

	      final int cs = chunkSize;
	      PCollection<Iterable<AvroFront>> coalescedChunks = searchOutput.get(pendingFrontsTag).apply(
	      		MapElements.into(new TypeDescriptor<KV<Prefix, AvroFront>>() {}).via(
	      				front -> KV.of(chunkingPrefix(front.peakIx, cs), front)))
	      		.apply(GroupByKey.create()).apply(Values.create());
	      searchOutput = coalescedChunks.apply(ParDo.of(new Prominence2(up, 20., pendingFrontsTag, mstTag))
	      		.withOutputTags(promFactsTag, TupleTagList.of(pendingFrontsTag).and(mstTag)));

	      promFacts = promFacts.and(searchOutput.get(promFactsTag));
	      mstEdges = mstEdges.and(searchOutput.get(mstTag).apply(
    				MapElements.into(new TypeDescriptor<KV<Long, KV<Integer, Edge>>>() {})
    	    		.via(e -> KV.of(e.saddle, KV.of(cs, e)))));
	    }
	    // coalesce steps -- must pre-populated all the way to global, even if many are no-ops
	    // what if chunk size is such that there is only one processing level (extreme edge case but try to handle it)
	        
	    PCollection<Iterable<AvroFront>> finalChunk = searchOutput.get(pendingFrontsTag).apply(MapElements.into(new TypeDescriptor<KV<Integer, AvroFront>>() {})
	    		.via(front -> KV.of(0, front))).apply(GroupByKey.create()).apply(Values.create());
	    
	    searchOutput = finalChunk.apply(ParDo.of(new PromFinalize(up, 20., mstTag))
	    		.withOutputTags(promFactsTag, TupleTagList.of(mstTag)));
	    promFacts = promFacts.and(searchOutput.get(promFactsTag));
        mstEdges = mstEdges.and(searchOutput.get(mstTag).apply(
  				MapElements.into(new TypeDescriptor<KV<Long, KV<Integer, Edge>>>() {})
  	    		.via(e -> KV.of(e.saddle, KV.of(9999, e)))));

        PCollection<Edge> mst = mstEdges.apply(Flatten.pCollections()).apply(GroupByKey.create()).apply(Values.create())
        		.apply(MapElements.into(new TypeDescriptor<Edge>() {}).via(kvs ->
        				new Ordering<KV<Integer, Edge>>() {
        					@Override
        					public int compare(KV<Integer, Edge> a, KV<Integer, Edge> b) {
        						return Integer.compare(a.getKey(), b.getKey());
        					}
        		  		  }.max(kvs).getValue()
        				));
        		
	    PCollection<PromFact> promInfo = consolidatePromFacts(promFacts);
	    
	    PCollection<PromFact> promRank = promInfo.apply(MapElements.into(new TypeDescriptor<KV<Integer, KV<Point, Point>>>() {})
	    		.via(pf -> KV.of(0, KV.of(pf.p, pf.saddle.s)))).apply(GroupByKey.create()).apply(Values.create())
	    		.apply(ParDo.of(new DoFn<Iterable<KV<Point, Point>>, PromFact>() {
	    		      @ProcessElement
	    		      public void processElement(ProcessContext c) {
	    		    	  Iterable<KV<Point, Point>> allProms = c.element();
	    		    	  List<KV<Point, Point>> proms = Lists.newArrayList(allProms);
	    		    	  proms.sort(new ReverseComparator<KV<Point, Point>>(new Comparator<KV<Point, Point>>() {
	    						@Override
	    						public int compare(KV<Point, Point> a, KV<Point, Point> b) {
	    							return new PromPair(a.getKey(), a.getValue()).compareTo(new PromPair(b.getKey(), b.getValue()));
	    						}	    		  
	    		    	  }));
	    		    	  ListIterator<KV<Point, Point>> it = proms.listIterator();
	    		    	  while (it.hasNext()) {
	    		    		  int i = it.nextIndex();
	    		    		  KV<Point, Point> pp = it.next();
	    		    		  
	    		    		  PromFact rank = new PromFact();
	    		    		  rank.p = new Point(pp.getKey());
	    		    		  rank.promRank = i;
	    		    		  c.output(rank);
	    		    	  }
	    		      }		  
	    			
	    		}));
	    
	    promInfo = consolidatePromFacts(PCollectionList.of(promInfo).and(promRank));
	    
	    return PCollectionTuple.of(promFactsTag, promInfo).and(mstTag, mst);
  }
  
  public static void main(String[] args) {
	
	// TODO: custom options and validation
	// --output=gs://mrgris-dataflow-test/output-file-prefix
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).create());
    
    PCollection<KV<Prefix, DEMFile>> pageFileMapping = TopologyNetworkPipeline.makePageFileMapping(p);
    final PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage = pageFileMapping.apply(View.asMultimap());

    PCollectionTuple promSearchUp = dirPipeline(p, true, "gs://mrgris-dataflow-test/network-up-*", pageCoverage);
    PCollectionTuple promSearchDown = dirPipeline(p, false, "gs://mrgris-dataflow-test/network-down-*", pageCoverage);
    
    PCollectionList.of(promSearchUp.get(promFactsTag)).and(promSearchDown.get(promFactsTag)).apply(Flatten.pCollections())
    	.apply("dumpfacts", AvroIO.write(PromFact.class).to("gs://mrgris-dataflow-test/factstest").withoutSharding());
    
    promSearchUp.get(mstTag).apply(AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/mst-up").withoutSharding());
    promSearchDown.get(mstTag).apply(AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/mst-down").withoutSharding());
    
    p.run();
    
  }
}
