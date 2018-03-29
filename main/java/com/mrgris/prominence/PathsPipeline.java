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
import java.util.Objects;
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
  
  @DefaultCoder(AvroCoder.class)
  static class TrimmedEdge {
	  List<Long> breadcrumbs;
	  boolean startsSaddle;
	  int traceNum;
	  
	  public TrimmedEdge() {
		  breadcrumbs = new ArrayList<>();
	  }
  }
  
  static abstract class PathSearcher {
	  Map<Object, Long> backtrace;
	  Map<Long, List<Integer>> anchors;
	  
	  static class BasinSaddleEdge {
		  long ix;
		  int trace;
		  
		  public BasinSaddleEdge(long ix, int trace) {
			  this.ix = ix;
			  this.trace = trace;
		  }
		  
			@Override
			public boolean equals(Object o) {
				if (o instanceof BasinSaddleEdge) {
					BasinSaddleEdge bse = (BasinSaddleEdge)o;
					return this.ix == bse.ix && this.trace == bse.trace;
				} else {
					return false;
				}
			}
			
			@Override
			public int hashCode() {
				return Objects.hash(ix, trace);
			}
	  }
	  
	  public PathSearcher(Iterable<Edge> mst, Iterable<KV<Long, Integer>> keyPointsIt) {
		  anchors = new DefaultMap<Long, List<Integer>>() {
			@Override
			public List<Integer> defaultValue(Long key) {
				return new ArrayList<>();
			}
		  };
		  backtrace = new HashMap<>();
		  
		  List<Edge> basinSaddleEdges = new ArrayList<>();
		  Set<Long> terminalSaddles = new HashSet<>();
		  for (Edge e : mst) {
			  if (e.a == PointIndex.NULL) {
				  // process only after main mst has been built
				  basinSaddleEdges.add(e);
			  } else {
				  backtrace.put(e.a, e.saddle);
				  if (e.b != PointIndex.NULL) {
					  backtrace.put(e.saddle, e.b);
				  } else {
					  terminalSaddles.add(e.saddle);
				  }
			  }
		  }
		  for (Edge e : basinSaddleEdges) {
			  if (backtrace.containsKey(e.saddle) || terminalSaddles.contains(e.saddle)) {
				  // through various quirks, the two MSTs can sometimes share saddles, particularly
				  // near the edge of the data region, though strictly speaking this isn't supposed
				  // to happen. give precedence to the mst over the basin saddles in this case.
				  continue;
			  }
			  
			  anchors.get(e.saddle).add(e.tagB);
			  if (e.b != PointIndex.NULL) {
				  backtrace.put(new BasinSaddleEdge(e.saddle, e.tagB), e.b);
			  }
		  }
		  
		  Map<Long, Integer> keyPoints = new HashMap<>();
		  for (KV<Long, Integer> kp : keyPointsIt) {
			  keyPoints.put(kp.getKey(), kp.getValue());
		  }
		  Set<Long> junctions = new HashSet<>();
		  Set<Long> seen = new HashSet<>();
		  Set<Object> traceStart = new HashSet<>();
		  for (long ix : keyPoints.keySet()) {
			  if (anchors.containsKey(ix)) {
				  for (int traceNum : anchors.get(ix)) {
					  traceStart.add(new BasinSaddleEdge(ix, traceNum));
				  }
			  } else {
				  traceStart.add(ix);
			  }
		  }
		  for (Object cur : traceStart) {
			  while (true) {
				  cur = get(cur);
				  long ix = (long)cur;
				  if (ix == PointIndex.NULL || keyPoints.containsKey(ix)) {
					  break;
				  } else if (seen.contains(ix)) {
					  junctions.add(ix);
					  break;
				  } else {
					  seen.add(ix);
				  }
			  }
		  }		  		  
		  for (long j : junctions) {
			  keyPoints.put(j, MeshPoint.CLASS_SUMMIT); // junctions can never be saddles
		  }
		  traceStart.addAll(junctions);
		  
		  Map<Object, Long> trimmedBacktrace = new HashMap<>();
		  for (Object start : traceStart) {
			  TrimmedEdge seg = new TrimmedEdge();
			  if (start instanceof BasinSaddleEdge) {
				  BasinSaddleEdge bse = (BasinSaddleEdge)start;
				  seg.breadcrumbs.add(bse.ix);
				  seg.traceNum = bse.trace;
				  seg.startsSaddle = true;
			  } else {
				  if (keyPoints.get(start) == MeshPoint.CLASS_SADDLE) {
					  seg.startsSaddle = true;
				  }
				  seg.breadcrumbs.add((long)start);
			  }
			  
			  Object cur = start;
			  long ix;
			  while (true) {
				  cur = get(cur);
				  ix = (long)cur; // how to tell if peak or saddle?
				  if (ix != PointIndex.NULL) {
					  seg.breadcrumbs.add(ix);
				  }
				  if (ix == PointIndex.NULL || keyPoints.containsKey(ix)) {
					  break;
				  }
			  }
			  if (ix != PointIndex.NULL) {
				  trimmedBacktrace.put(start, ix);
			  }
			  if (seg.breadcrumbs.size() > 1) {
				  emitEdge(seg);
			  }
		  }		  		  
		  System.out.println("backtrace size before " + backtrace.size() + " after " + trimmedBacktrace.size());
		  backtrace = trimmedBacktrace;
	  }

	  public long get(Object cur) {
		  if (backtrace.containsKey(cur)) {
			  return backtrace.get(cur);
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
	  public abstract void emitEdge(TrimmedEdge seg);
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

	  PCollection<KV<Long, Integer>> keyPoints = PCollectionList.of(
			  prom.apply(ParDo.of(new DoFn<PromFact, KV<Long, Integer>>(){
				  @ProcessElement
				  public void processElement(ProcessContext c) {
					  PromFact pf = c.element();
					  c.output(KV.of(pf.p.ix, MeshPoint.CLASS_SUMMIT));
					  c.output(KV.of(pf.saddle.s.ix, MeshPoint.CLASS_SADDLE));
					  if (pf.thresh != null) {
						  c.output(KV.of(pf.thresh.ix, MeshPoint.CLASS_SUMMIT));
					  }
				  }
			  }))
			  ).and(promBasinSaddles.apply(MapElements.into(new TypeDescriptor<KV<Long, Integer>>(){})
					  .via(ix -> KV.of(ix, MeshPoint.CLASS_SADDLE)))).apply(Flatten.pCollections()).apply(Distinct.create());
	  	  
	  PCollection<KV<Integer, Iterable<PathTask>>> taskSingleton =
			  tasks.apply(MapElements.into(new TypeDescriptor<KV<Integer, PathTask>>() {}).via(task -> KV.of(0, task)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Edge>>> mstSingleton =
			  PCollectionList.of(mst).and(mstSaddleAnchors).apply(Flatten.pCollections())
			  .apply(MapElements.into(new TypeDescriptor<KV<Integer, Edge>>() {}).via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<KV<Long, Integer>>>> keyPointsSingleton =
			  keyPoints.apply(MapElements.into(new TypeDescriptor<KV<Integer, KV<Long, Integer>>>() {}).via(kp -> KV.of(0, kp)))
	  		.apply(GroupByKey.create());

	  final TupleTag<Iterable<PathTask>> taskTag = new TupleTag<>();
	  final TupleTag<Iterable<Edge>> mstTag = new TupleTag<>();	  
	  final TupleTag<Iterable<KV<Long, Integer>>> keyPointsTag = new TupleTag<>();	  
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
		      Iterable<KV<Long, Integer>> keyPoints = e.getValue().getAll(keyPointsTag).iterator().next();
		      
		      PathSearcher searcher = new PathSearcher(mst, keyPoints) {
		    	  public void emitPath(PromFact pf) {
		    		  c.output(pf);
		    	  }
		    	  public void emitEdge(TrimmedEdge seg) {
		    		  // TODO
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
