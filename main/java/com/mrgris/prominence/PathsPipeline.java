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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.org.apache.commons.compress.utils.Lists;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.Prominence.PromFact;

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

public class PathsPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(PathsPipeline.class);
  
  static abstract class PathSearcher {
	  Map<Long, Long> backtrace;
	  
	  public PathSearcher(Iterable<Edge> mst) {
		  backtrace = new HashMap<>();
		  for (Edge e : mst) {
			  if (e.a == PointIndex.NULL) {
				  throw new RuntimeException();
			  }
			  backtrace.put(e.a, e.saddle);
			  if (e.b != PointIndex.NULL) {
				  backtrace.put(e.saddle, e.b);
			  }
		  }
	  }

	  public long get(long ix) {
		  if (backtrace.containsKey(ix)) {
			  return backtrace.get(ix);
		  } else {
			  return PointIndex.NULL;
		  }
	  }
	  
		public List<Long> getAtoB(long start, long end) {
			List<Long> fromA = new ArrayList<>();
			List<Long> fromB = new ArrayList<>();
			Set<Long> inFromA = new HashSet<>();
			Set<Long> inFromB = new HashSet<>();

			long intersection = PointIndex.NULL;
			long curA = start;
			long curB = end;
			while (curA != PointIndex.NULL || curB != PointIndex.NULL) {
				if (curA != PointIndex.NULL && curB != PointIndex.NULL && curA == curB) {
					intersection = curA;
					break;
				}
				
				if (curA != PointIndex.NULL) {
					fromA.add(curA);
					inFromA.add(curA);
					curA = this.get(curA);
				}
				if (curB != PointIndex.NULL) {
					fromB.add(curB);
					inFromB.add(curB);
					curB = this.get(curB);
				}
					
				if (inFromA.contains(curB)) {
					intersection = curB;
					break;
				} else if (inFromB.contains(curA)) {
					intersection = curA;
					break;
				}
			}

			List<Long> path = new ArrayList<>();
			int i = fromA.indexOf(intersection);
			path.addAll(i != -1 ? fromA.subList(0, i) : fromA);
			path.add(intersection);
			List<Long> path2 = new ArrayList<>();
			i = fromB.indexOf(intersection);
			path2 = (i != -1 ? fromB.subList(0, i) : fromB);
			Collections.reverse(path2);
			path.addAll(path2);
			return path;
		}
	  
	  void search(PromFact p) {
		  PromFact threshPath = new PromFact();
		  threshPath.p = p.p;
		  threshPath.threshPath = getAtoB(p.p.ix, p.thresh != null ? p.thresh.ix : PointIndex.NULL);
		  emitPath(threshPath);
		  
		  PromFact parentPath = new PromFact();
		  parentPath.p = p.p;
		  parentPath.parentPath = getAtoB(p.p.ix, p.parent != null ? p.parent.ix : PointIndex.NULL);
		  emitPath(parentPath);
	  }
	  
	  public abstract void emitPath(PromFact pf);
  }
  
  public static PCollection<PromFact> searchPaths(PCollection<PromFact> promInfo, PCollection<Edge> mst) {
	  PCollection<KV<Integer, Iterable<PromFact>>> promSingleton =
			  promInfo.apply(MapElements.into(new TypeDescriptor<KV<Integer, PromFact>>() {}).via(pf -> KV.of(0, pf)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Edge>>> mstSingleton =
			  mst.apply(MapElements.into(new TypeDescriptor<KV<Integer, Edge>>() {}).via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());

	  final TupleTag<Iterable<PromFact>> promInfoTag = new TupleTag<>();
	  final TupleTag<Iterable<Edge>> mstTag = new TupleTag<>();
	  
      return KeyedPCollectionTuple
			    .of(promInfoTag, promSingleton)
			    .and(mstTag, mstSingleton)
			    .apply(CoGroupByKey.create())
			    .apply(ParDo.of(
		  new DoFn<KV<Integer, CoGbkResult>, PromFact>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Integer, CoGbkResult> e = c.element();
		      Iterable<PromFact> promInfo = e.getValue().getAll(promInfoTag).iterator().next();
		      Iterable<Edge> mst = e.getValue().getAll(mstTag).iterator().next();

		      PathSearcher searcher = new PathSearcher(mst) {
		    	  public void emitPath(PromFact pf) {
		    		  c.output(pf);
		    	  }
		      };
		      for (PromFact pf : promInfo) {
		    	  searcher.search(pf);
		      }
		    }
		  }
		));
	  
  }
  
  public static void main(String[] args) {
	 
	// TODO: custom options and validation
	// --output=gs://mrgris-dataflow-test/output-file-prefix
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).create());
    
    PCollection<PromFact> promInfo = p.apply(AvroIO.read(PromFact.class).from("gs://mrgris-dataflow-test/factstest"));
    PCollection<Edge> mstUp = p.apply(AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/mst-up"));
    PCollection<Edge> mstDown = p.apply(AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/mst-down"));

    PCollectionList<PromFact> promByDir = promInfo.apply(Partition.of(2, new PartitionFn<PromFact>() {
		@Override
		public int partitionFor(PromFact e, int numPartitions) {
			return Point.compareElev(e.p, e.saddle.s) > 0 ? 0 : 1;
		}
    }));
    PCollection<PromFact> promInfoUp = promByDir.get(0);
    PCollection<PromFact> promInfoDown = promByDir.get(1);

    PCollection<PromFact> pathsUp = searchPaths(promInfoUp, mstUp);
    PCollection<PromFact> pathsDown = searchPaths(promInfoDown, mstDown);
    
    promInfo = ProminencePipeline.consolidatePromFacts(PCollectionList.of(promInfo).and(pathsUp).and(pathsDown));
    promInfo.apply(AvroIO.write(PromFact.class).to("gs://mrgris-dataflow-test/factstestwithpaths").withoutSharding());
    
    p.run();
    
  }
}
