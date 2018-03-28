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

import org.apache.avro.reflect.Nullable;
import org.apache.beam.runners.dataflow.repackaged.org.apache.beam.runners.core.construction.java.repackaged.com.google.common.collect.Sets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.Edge.HalfEdge;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.PromFact.Saddle;
import com.mrgris.prominence.util.DefaultMap;

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
	  Map<Long, List<Edge>> anchors;
	  
	  /*
	  static class IterStart {
		  long ix;
		  int option;
		  
		  public IterStart(long ix, int option) {
			  this.ix = ix;
			  this.option = option;
		  }
	  }
	  */
	  
	  public PathSearcher(Iterable<Edge> mst, Iterable<Long> keyPointsIt) {
		  anchors = new DefaultMap<Long, List<Edge>>() {
			@Override
			public List<Edge> defaultValue(Long key) {
				return new ArrayList<>();
			}
		  };
		  backtrace = new HashMap<>();
		  
		  for (Edge e : mst) {
			  if (e.a == PointIndex.NULL) {
				  anchors.get(e.saddle).add(e);
			  } else {
				  backtrace.put(e.a, e.saddle);
				  if (e.b != PointIndex.NULL) {
					  backtrace.put(e.saddle, e.b);
				  }
			  }
		  }
		  // awefowijfisjfiwjef
		  for (long ix : backtrace.keySet()) {
			  anchors.remove(ix);
		  }
		  for (long ix : backtrace.values()) {
			  anchors.remove(ix);
		  }
		  
		  Set<Long> keyPoints = Sets.newHashSet(keyPointsIt);
		  Set<Long> junctions = new HashSet<>();
		  Set<Long> seen = new HashSet<>(keyPoints);
		  for (long start : keyPoints) {
			  for (int opt = 0; opt < numOptions(start); opt++) {
				  long ix = start;
				  int i = opt;
				  
				  while (true) {
					  ix = get(ix, i);
					  i = 0;
					  
					  if (ix == PointIndex.NULL) {
						  break;
					  }
					  if (seen.contains(ix)) {
						  if (!keyPoints.contains(ix)) {
							  junctions.add(ix);
						  }
						  break;
					  } else {
						  seen.add(ix);
					  }
				  }
			  }
		  }
		  System.out.println("backtrace " + backtrace.size());
		  System.out.println("seen " + seen.size());
		  System.out.println("keypoints " + keyPoints.size());
		  System.out.println("junctions " + junctions.size());
		  		  
		  keyPoints.addAll(junctions);

		  Map<Long, List<Edge>> rAnchors = new DefaultMap<Long, List<Edge>>() {
			@Override
			public List<Edge> defaultValue(Long key) {
				return new ArrayList<>();
			}
		  };
		  Map<Long, Long> rBacktrace = new HashMap<>();

		  for (long start : keyPoints) {
			  for (int opt = 0; opt < numOptions(start); opt++) {
				  LOG.debug("kp " + start + " " + opt);
				  
				  long ix = start;
				  int i = opt;
				  
				  LOG.debug("tr " + ix + " " + i);
				  while (true) {
					  ix = get(ix, i);
					  i = 0;
					  LOG.debug("tr " + ix + " " + i);
					  
					  if (ix == PointIndex.NULL) {
						  break;
					  }
					  if (keyPoints.contains(ix)) {
						  break;
					  }
				  }
				  LOG.debug("end " + start + " " + ix);
				  
				  if (anchors.containsKey(start)) {
					  if (ix == start) {
						  // workaround EOW MST crossing bug
						  ix = PointIndex.NULL;
					  }
					  
					  rAnchors.get(start).add(new Edge(PointIndex.NULL, ix, start, Edge.TAG_NULL,
							  anchors.get(start).get(opt).tagB));
				  } else if (ix != PointIndex.NULL) {
					  rBacktrace.put(start, ix);
					  LOG.debug("here!!!" + rBacktrace.size());
				  }
			  }
		  }
		  System.out.println("rbt3 " + rBacktrace.size());
		  // need edge for terminal keypoint to EOW?
		  this.anchors = rAnchors;
		  this.backtrace = rBacktrace;
		  System.out.println("rbt2 " + this.backtrace.size());
		  
		  int c = 0;
		  for (long ix : backtrace.keySet()) {
			  long next = get(ix);
			  if (next == PointIndex.NULL) {
				  c++;
			  } else if (backtrace.containsKey(next)) {
				  c++;
			  }
		  }
		  System.out.println("rbt " + this.backtrace.size() + " " + c);
		  
	  }

	  public int numOptions(long ix) {
		  if (anchors.containsKey(ix)) {
			  return anchors.get(ix).size();
		  } else {
			  return 1;
		  }
	  }
	  
	  public long get(long ix) {
		  //if (anchors.containsKey(ix) && !backtrace.containsKey(ix)) {
		 //	  throw new RuntimeException("" + ix);
		  //}
		  return get(ix, 0);
	  }
	  
	  public long get(long ix, int option) {
		  if (anchors.containsKey(ix) && !backtrace.containsKey(ix)) {
			  return anchors.get(ix).get(option).b;
		  } else if (backtrace.containsKey(ix)) {
			  if (option != 0) {
				  throw new RuntimeException();
			  }
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
	  
	  void search(PathTask task) {
		  PromFact fact = new PromFact();
		  fact.p = task.p;
		  if (task.type == PathTask.TYPE_THRESH) {
			  fact.threshPath = getAtoB(task.p.ix, task.thresh);
		  } else if (task.type == PathTask.TYPE_PARENT) {		  
		  	  fact.parentPath = getAtoB(task.p.ix, task.parent);
		  } else if (task.type == PathTask.TYPE_DOMAIN) {
			  fact.domainBoundary = Runoff.runoff(task.saddles, this);
		  }
		  emitPath(fact);
	  }
	  
	  public abstract void emitPath(PromFact pf);
  }
  
  @DefaultCoder(AvroCoder.class)
  static class PathTask {
	  static final int TYPE_THRESH = 1;
	  static final int TYPE_PARENT = 2;
	  static final int TYPE_DOMAIN = 3;
	  
	  Point p;
	  int type;
	  long thresh;
	  long parent;
	  @Nullable
	  ArrayList<Saddle> saddles;
  }
  
  public static PCollection<PromFact> searchPaths(PCollection<PromFact> prom, PCollection<PromFact> promOppo,
		  PCollection<Edge> mst, PCollection<Edge> rawNetwork) {
	  PCollection<PathTask> tasks = PCollectionList.of(
			  prom.apply(ParDo.of(new DoFn<PromFact, PathTask>() {
				  @ProcessElement
				  public void processElement(ProcessContext c) {
					  PromFact pf = c.element();

					  PathTask thresh = new PathTask();
					  thresh.type = PathTask.TYPE_THRESH;
					  thresh.p = pf.p;
					  thresh.thresh = pf.thresh != null ? pf.thresh.ix : PointIndex.NULL;
					  c.output(thresh);

					  PathTask parent = new PathTask();
					  parent.type = PathTask.TYPE_PARENT;
					  parent.p = pf.p;
					  parent.parent = pf.parent != null ? pf.parent.ix : PointIndex.NULL;
					  c.output(parent);
				  }
			  })))
			  .and(
					  promOppo.apply(ParDo.of(new DoFn<PromFact, PathTask>() {
						  @ProcessElement
						  public void processElement(ProcessContext c) {
							  PromFact pf = c.element();

							  PathTask domain = new PathTask();
							  domain.type = PathTask.TYPE_DOMAIN;
							  domain.p = pf.p;
							  domain.saddles = new ArrayList<>(pf.promSubsaddles);
							  domain.saddles.add(pf.saddle);
							  c.output(domain);
						  }
					  })))			  
			  .apply(Flatten.pCollections());
	  
	  PCollection<Long> promBasinSaddles = promOppo.apply(MapElements.into(new TypeDescriptor<Long>() {}).via(pf -> pf.saddle.s.ix));
      // in theory this could get too big for a side input, but estimate <30M points globally for P20m
	  final PCollectionView<Map<Long, Void>> saddleLookup = promBasinSaddles.apply(MapElements
    		  .into(new TypeDescriptor<KV<Long, Void>>() {}).via(ix -> KV.of(ix, null))).apply(View.asMap());
	  PCollection<Edge> mstSaddleAnchors = rawNetwork.apply(ParDo.of(new DoFn<Edge, Edge>() {
		  @ProcessElement
		  public void processElement(ProcessContext c) {
			  Map<Long, Void> relevantSaddles = c.sideInput(saddleLookup);
			  Edge e = c.element();
			  if (relevantSaddles.containsKey(e.saddle)) {
				  for (HalfEdge he : e.split()) {
					  // still output null half-edges because we need both tag #s to determine orientation
					  c.output(new Edge(PointIndex.NULL, he.p, he.saddle, Edge.TAG_NULL, he.tag));
				  }
			  }
		  }		  
	  }).withSideInputs(saddleLookup));

	  PCollection<Long> keyPoints = PCollectionList.of(
			  prom.apply(ParDo.of(new DoFn<PromFact, Long>(){
				  @ProcessElement
				  public void processElement(ProcessContext c) {
					  PromFact pf = c.element();
					  c.output(pf.p.ix);
					  c.output(pf.saddle.s.ix);
					  if (pf.thresh != null) {
						  c.output(pf.thresh.ix);
					  }
				  }
			  }))
			  ).and(promBasinSaddles).apply(Flatten.pCollections()).apply(Distinct.create());
	  	  
	  PCollection<KV<Integer, Iterable<PathTask>>> taskSingleton =
			  tasks.apply(MapElements.into(new TypeDescriptor<KV<Integer, PathTask>>() {}).via(task -> KV.of(0, task)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Edge>>> mstSingleton =
			  PCollectionList.of(mst).and(mstSaddleAnchors).apply(Flatten.pCollections())
			  .apply(MapElements.into(new TypeDescriptor<KV<Integer, Edge>>() {}).via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Long>>> keyPointsSingleton =
			  keyPoints.apply(MapElements.into(new TypeDescriptor<KV<Integer, Long>>() {}).via(ix -> KV.of(0, ix)))
	  		.apply(GroupByKey.create());

	  final TupleTag<Iterable<PathTask>> taskTag = new TupleTag<>();
	  final TupleTag<Iterable<Edge>> mstTag = new TupleTag<>();	  
	  final TupleTag<Iterable<Long>> keyPointsTag = new TupleTag<>();	  
      return KeyedPCollectionTuple
			    .of(taskTag, taskSingleton)
			    .and(mstTag, mstSingleton)
			    .and(keyPointsTag, keyPointsSingleton)
			    .apply(CoGroupByKey.create())
			    .apply(ParDo.of(
		  new DoFn<KV<Integer, CoGbkResult>, PromFact>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Integer, CoGbkResult> e = c.element();
		      Iterable<PathTask> tasks = e.getValue().getAll(taskTag).iterator().next();
		      Iterable<Edge> mst = e.getValue().getAll(mstTag).iterator().next();
		      Iterable<Long> keyPoints = e.getValue().getAll(keyPointsTag).iterator().next();
		      
		      PathSearcher searcher = new PathSearcher(mst, keyPoints) {
		    	  public void emitPath(PromFact pf) {
		    		  c.output(pf);
		    	  }
		      };
		      for (PathTask task : tasks) {
		    	  searcher.search(task);
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
    PCollection<Edge> rawNetworkUp = p.apply(AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/network-up-*"));
    PCollection<Edge> rawNetworkDown = p.apply(AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/network-down-*"));

    PCollectionList<PromFact> promByDir = promInfo.apply(Partition.of(2, new PartitionFn<PromFact>() {
		@Override
		public int partitionFor(PromFact e, int numPartitions) {
			return Point.compareElev(e.p, e.saddle.s) > 0 ? 0 : 1;
		}
    }));
    PCollection<PromFact> promInfoUp = promByDir.get(0);
    PCollection<PromFact> promInfoDown = promByDir.get(1);

    PCollection<PromFact> pathsUp = searchPaths(promInfoUp, promInfoDown, mstUp, rawNetworkUp);
    PCollection<PromFact> pathsDown = searchPaths(promInfoDown, promInfoUp, mstDown, rawNetworkDown);
    
    promInfo = ProminencePipeline.consolidatePromFacts(PCollectionList.of(promInfo).and(pathsUp).and(pathsDown));
    promInfo.apply(AvroIO.write(PromFact.class).to("gs://mrgris-dataflow-test/factstestwithpaths").withoutSharding());
    
    p.run();
    
  }
}
