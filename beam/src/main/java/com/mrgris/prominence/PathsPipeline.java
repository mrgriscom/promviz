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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mrgris.prominence.AvroToDb.MSTDebugSink;
import com.mrgris.prominence.AvroToDb.PointsDebugSink;
import com.mrgris.prominence.AvroToDb.SpatialiteSink;
import com.mrgris.prominence.Edge.HalfEdge;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.Prominence.PromFact.Saddle;
import com.mrgris.prominence.ProminencePipeline.PromPipeline;
import com.mrgris.prominence.TopologyNetworkPipeline.TopoPipeline;
import com.mrgris.prominence.dem.DEMFile;
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
  static class PrunedEdge {
	  long srcIx;
	  long dstIx;
	  int saddleTraceNum = -1;
	  ArrayList<Long> interimIxs;
	  
	  public PrunedEdge() {
		  interimIxs = new ArrayList<>();
	  }
	  
		@Override
		public boolean equals(Object o) {
			if (o instanceof PrunedEdge) {
				PrunedEdge pe = (PrunedEdge)o;
				return this.srcIx == pe.srcIx && this.dstIx == pe.dstIx && this.saddleTraceNum == pe.saddleTraceNum;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(srcIx, dstIx, saddleTraceNum);
		}

  }
  
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
  
  static class MST {
	  Map<Object, Long> backtrace;  // key is either long or BasinSaddleEdge
	  Map<Long, List<Integer>> basinSaddles;
	  Map<Object, List<Long>> trimmedSegments;
	  
	  public MST() {
		  backtrace = new HashMap<>();
		  trimmedSegments = new HashMap<>();
		  basinSaddles = new DefaultMap<Long, List<Integer>>() {
				@Override
				public List<Integer> defaultValue(Long key) {
					return new ArrayList<>();
				}
			  };
	  }
	  	  
	  // helpful in various situations, as the terminal point doesn't always have an explicit null chained after
	  public long getDeadendAsNull(Object cur) {
		  if (backtrace.containsKey(cur)) {
			  return backtrace.get(cur);
		  } else {
			  return PointIndex.NULL;
		  }
	  }
  }
  
  static abstract class PathSearcher {
	  
	  // more memory efficient than list
	  // (doesn't seem to matter)
	  static class SaddleDirs {
		  public int a = -1;
		  public int b = -1;
		  
		  public void add(int n) {
			  if (n < 0) {
				  throw new RuntimeException();
			  }
			  if (a == -1) {
				  a = n;
			  } else if (b == -1) {
				  b = n;
			  } else {
				  throw new RuntimeException();
			  }
		  }
		  
		  public int[] toArr() {
			  if (a == -1) {
				  return new int[] {};
			  } else if (b == -1) {
				  return new int[] {a};				  
			  } else {
				  return new int[] {a, b};
			  }
		  }
	  }
	  
	  Map<Object, Long> backtrace;
	  Map<Long, SaddleDirs> anchors;
	  
	  public PathSearcher(Iterable<PrunedEdge> mst) {
		  anchors = new DefaultMap<Long, SaddleDirs>() {
			@Override
			public SaddleDirs defaultValue(Long key) {
				return new SaddleDirs();
			}
		  };
		  backtrace = new HashMap<>();
		  
		  for (PrunedEdge e : mst) {
			  if (e.saddleTraceNum == -1) {
				  if (e.dstIx != PointIndex.NULL) {
					  backtrace.put(e.srcIx, e.dstIx);
				  }
			  } else {
				  anchors.get(e.srcIx).add(e.saddleTraceNum);
				  if (e.dstIx != PointIndex.NULL) {
					  backtrace.put(new BasinSaddleEdge(e.srcIx, e.saddleTraceNum), e.dstIx);

				  }
			  }
		  }
	  }
		  
	  public long get(Object cur) {
		  if (backtrace.containsKey(cur)) {
			  return backtrace.get(cur);
		  } else {
			  return PointIndex.NULL;
		  }
	  }
	  
		public ArrayList<ArrayList<Long>> getAtoB(long start, long end) {
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

			ArrayList<Long> Aseg = new ArrayList<>();
			ArrayList<Long> Bseg = new ArrayList<>();
			int i = fromA.indexOf(intersection);
			if (i != -1) {
				Aseg.addAll(fromA.subList(0, i + 1)); // incl intersection
			} else {
				Aseg.addAll(fromA);
				Aseg.add(PointIndex.NULL);
			}
			i = fromB.indexOf(intersection);
			if (i != -1) {
				Bseg.addAll(fromB.subList(0, i + 1)); // incl intersection
			} else {
				Bseg.addAll(fromB);
				Bseg.add(PointIndex.NULL);
			}
			return Lists.newArrayList(Aseg, Bseg);
			
//			List<Long> path = new ArrayList<>();
//			int i = fromA.indexOf(intersection);
//			path.addAll(i != -1 ? fromA.subList(0, i) : fromA);
//			path.add(intersection);
//			List<Long> path2 = new ArrayList<>();
//			i = fromB.indexOf(intersection);
//			path2 = (i != -1 ? fromB.subList(0, i) : fromB);
//			Collections.reverse(path2);
//			path.addAll(path2);
//			return path;
		}
	  
	  void search(PathTask task) {
		  PathFragments path = new PathFragments();
		  path.p = task.p;
		  path.type = task.type;
		  if (task.type == PathTask.TYPE_THRESH) {
			  path.fragments = getAtoB(task.p.ix, task.target);
		  } else if (task.type == PathTask.TYPE_PARENT) {		  
			  path.fragments = getAtoB(task.p.ix, task.target);
		  } else if (task.type == PathTask.TYPE_DOMAIN) {
			  path.fragments = (ArrayList)Runoff.runoff(task.saddles, this);
		  }
		  emitPath(path);
	  }
	  
	  public abstract void emitPath(PathFragments pf);
	  //public abstract void emitEdge(TrimmedEdge seg);
  }
  
  @DefaultCoder(AvroCoder.class)
  static class PathTask {
	  static final int TYPE_THRESH = 1;
	  static final int TYPE_PARENT = 2;
	  static final int TYPE_DOMAIN = 3;
	  
	  Point p;
	  int type;
	  long target;
	  @Nullable
	  ArrayList<Saddle> saddles;
  }
  
  @DefaultCoder(AvroCoder.class)
  static class PathFragments {
	  Point p;
	  int type;
	  ArrayList<ArrayList<Long>> fragments;
	  
	  public PathFragments() {
		  fragments = new ArrayList<>();
	  }
  }

  @DefaultCoder(AvroCoder.class)
  static class PathTaskId {
	  long ix;
	  int type;
	  
	  public PathTaskId() {}
	  
	  public PathTaskId(long ix, int type) {
		  this.ix = ix;
		  this.type = type;
	  }
	  
		@Override
		public boolean equals(Object o) {
			if (o instanceof PathTaskId) {
				PathTaskId pe = (PathTaskId)o;
				return this.ix == pe.ix && this.type == pe.type;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(ix, type);
		}

  }

  @DefaultCoder(AvroCoder.class)
  static class EdgeId {
	  long ix;
	  int dir = -1;
	  boolean up;   // not needed to disambiguate -- should be distinct based on (ix, dir), but makes bookkeeping easier
	  
	  public EdgeId() {}
	  
	  public EdgeId(long ix, int dir, boolean up) {
		  this.ix = ix;
		  this.dir = dir;
		  this.up = up;
	  }
	  
		@Override
		public boolean equals(Object o) {
			if (o instanceof EdgeId) {
				EdgeId pe = (EdgeId)o;
				return this.ix == pe.ix && this.dir == pe.dir && this.up == pe.up;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(ix, dir, up);
		}

  }

  
  static String ud(boolean up) { return up ? "-Up" : "-Down"; }
  
  static TupleTag<PathFragments> pathFragsTag = new TupleTag<>();
  static TupleTag<PrunedEdge> fullPmstTag = new TupleTag<>();
  
  public static PCollectionTuple searchPaths(boolean up, String debugDst, PCollection<PromFact> prom, PCollection<PromFact> promOppo,
		  PCollection<Edge> mst, PCollection<Edge> rawNetwork) {
	  PCollection<PathTask> tasks = PCollectionList.of(
			  prom.apply("PathTasks"+ud(up), ParDo.of(new DoFn<PromFact, PathTask>() {
				  @ProcessElement
				  public void processElement(ProcessContext c) {
					  PromFact pf = c.element();

					  PathTask thresh = new PathTask();
					  thresh.type = PathTask.TYPE_THRESH;
					  thresh.p = pf.p;
					  thresh.target = pf.thresh != null ? pf.thresh.ix : PointIndex.NULL;
					  c.output(thresh);

					  PathTask parent = new PathTask();
					  parent.type = PathTask.TYPE_PARENT;
					  parent.p = pf.p;
					  parent.target = pf.parent != null ? pf.parent.ix : PointIndex.NULL;
					  c.output(parent);
				  }
			  })))
			  .and(
					  promOppo.apply("DomainTasks"+ud(up), ParDo.of(new DoFn<PromFact, PathTask>() {
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
	  
	  PCollection<Long> promBasinSaddles = promOppo.apply("GetBasinSaddles"+ud(up), MapElements.into(new TypeDescriptor<Long>() {}).via(pf -> pf.saddle.s.ix));
	  
	  // remove basin saddles that already exist in mst. this is contradictory and shouldn't happen but does due to
	  // some quirks. might go away with support for EOW saddles?
	  PCollection<Long> promSaddles = mst.apply(MapElements.into(new TypeDescriptor<Long>() {}).via(e -> e.saddle));
	  final TupleTag<Iterable<Void>> main = new TupleTag<Iterable<Void>>() {};	  
	  final TupleTag<Iterable<Void>> subtract = new TupleTag<Iterable<Void>>() {};
	  promBasinSaddles = KeyedPCollectionTuple
			  .of(main, promBasinSaddles.apply(MapElements.into(new TypeDescriptor<KV<Long, Void>>() {})
					  .via(ix -> KV.of(ix, null))).apply(GroupByKey.create()))
			  .and(subtract, promSaddles.apply(MapElements.into(new TypeDescriptor<KV<Long, Void>>() {})
					  .via(ix -> KV.of(ix, null))).apply(GroupByKey.create()))
			  .apply(CoGroupByKey.create())
			  .apply("RemoveBasinSaddlesAppearingInMst"+ud(up), ParDo.of(new DoFn<KV<Long, CoGbkResult>, Long>() {
				  @ProcessElement
				  public void processElement(ProcessContext c, MultiOutputReceiver out) {
					  KV<Long, CoGbkResult> elem = c.element();
					  long ix = elem.getKey();
					  boolean mainMatch = (elem.getValue().getOnly(main, null) != null);
					  boolean subtrMatch = (elem.getValue().getOnly(subtract, null) != null);
					  if (mainMatch && !subtrMatch) {
						  c.output(ix);
					  }
				  }
			  }));
	  	  
      // in theory this could get too big for a side input, but estimate <30M points globally for P20m
	  final PCollectionView<Map<Long, Void>> saddleLookup = promBasinSaddles.apply(MapElements
    		  .into(new TypeDescriptor<KV<Long, Void>>() {}).via(ix -> KV.of(ix, null))).apply(View.asMap());
	  PCollection<Edge> mstSaddleAnchors = rawNetwork.apply("GetBasinSaddleAnchors"+ud(up), ParDo.of(new DoFn<Edge, Edge>() {
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
			  prom.apply("MstKeyPoints"+ud(up), ParDo.of(new DoFn<PromFact, Long>(){
				  @ProcessElement
				  public void processElement(ProcessContext c) {
					  PromFact pf = c.element();
					  c.output(pf.p.ix);
					  //c.output(KV.of(pf.saddle.s.ix, MeshPoint.CLASS_SADDLE)); // included implicitly; makes pmst smaller
					  if (pf.thresh != null) {
						  c.output(pf.thresh.ix);
					  }
				  }
			  }))
			  ).and(promBasinSaddles).apply(Flatten.pCollections()).apply(Distinct.create());

	    keyPoints.apply(FileIO.<Long>write()
	            .via(new PointsDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "keypoints.spatialite";
					}
	            }).withNumShards(1));

	  
	    mst.apply(MapElements.into(new TypeDescriptor<KV<Long,Long>>() {}).via(e -> KV.of(e.a, e.b)))
	    .apply(FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "fullmst.spatialite";
					}
	            }).withNumShards(1));

	  
	  PCollection<KV<Prefix, Iterable<Edge>>> mstChunked =
			  PCollectionList.of(mst).and(mstSaddleAnchors).apply(Flatten.pCollections()).apply("ChunkMst"+ud(up), 
					  ParDo.of(new DoFn<Edge, KV<Prefix, Edge>>(){
						  @ProcessElement
						  public void processElement(ProcessContext c) {
							  Edge e = c.element();
							  long srcIx = (e.a != PointIndex.NULL ? e.a : e.saddle);  // handles basin saddle anchors
							  Prefix srcPrefix = ProminencePipeline.chunkingPrefix(srcIx, TopologyNetworkPipeline.CHUNK_SIZE_EXP);
							  c.output(KV.of(srcPrefix, e));
						  }
					  })).apply(GroupByKey.create());
	  PCollection<KV<Prefix, Long>> mstChunkInflows =
			  PCollectionList.of(mst).and(mstSaddleAnchors).apply(Flatten.pCollections()).apply("ChunkMstInflows"+ud(up), 
					  ParDo.of(new DoFn<Edge, KV<Prefix, Long>>(){
						  @ProcessElement
						  public void processElement(ProcessContext c) {
							  Edge e = c.element();
							  long srcIx = (e.a != PointIndex.NULL ? e.a : e.saddle);  // handles basin saddle anchors
							  long dstIx = e.b;
							  if (dstIx != PointIndex.NULL) {
								  Prefix srcPrefix = ProminencePipeline.chunkingPrefix(srcIx, TopologyNetworkPipeline.CHUNK_SIZE_EXP);
								  Prefix dstPrefix = ProminencePipeline.chunkingPrefix(dstIx, TopologyNetworkPipeline.CHUNK_SIZE_EXP);
								  if (!dstPrefix.equals(srcPrefix)) {
									  c.output(KV.of(dstPrefix, dstIx));
								  }
							  }
						  }
					  }));
	  
	  PCollection<KV<Prefix, Iterable<Long>>> keyPointsChunked =
			  keyPoints.apply("ChunkKeyPoints"+ud(up), MapElements.into(new TypeDescriptor<KV<Prefix, Long>>() {}).via(kp -> 
			  KV.of(ProminencePipeline.chunkingPrefix(kp, TopologyNetworkPipeline.CHUNK_SIZE_EXP), kp)))
	  		.apply(GroupByKey.create());

	  final TupleTag<Iterable<Edge>> mstTag = new TupleTag<Iterable<Edge>>() {};	  
	  final TupleTag<Long> mstChunkInflowsTag = new TupleTag<Long>() {};	  
	  final TupleTag<Iterable<Long>> keyPointsTag = new TupleTag<Iterable<Long>>() {};
      final TupleTag<KV<Long, Long>> outPatchPanel = new TupleTag<KV<Long, Long>>(){};
      final TupleTag<Long> outRelevantInflows = new TupleTag<Long>(){};
      PCollectionTuple mstTrace = KeyedPCollectionTuple
			    .of(mstTag, mstChunked)
			    .and(mstChunkInflowsTag, mstChunkInflows)
			    .and(keyPointsTag, keyPointsChunked)
			    .apply(CoGroupByKey.create())
			    .apply("TraceMstChunks"+ud(up), ParDo.of(
		  new DoFn<KV<Prefix, CoGbkResult>, KV<Long, Long>>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Prefix, CoGbkResult> elem = c.element();
		      Prefix pf = elem.getKey();
		      Iterable<Edge> mst = elem.getValue().getOnly(mstTag, Lists.newArrayList());
		      Iterable<Long> keyPoints = elem.getValue().getOnly(keyPointsTag, Lists.newArrayList());
		      Set<Long> inflows = Sets.newHashSet(elem.getValue().getAll(mstChunkInflowsTag));
		      
		      MST chunkMst = new MST();
		      for (Edge e : mst) {
		    	  if (e.a != PointIndex.NULL) {
		    		  chunkMst.backtrace.put(e.a, e.b);
		    	  } else {
		    		  chunkMst.backtrace.put(new BasinSaddleEdge(e.saddle, e.tagB), e.b);
					  chunkMst.basinSaddles.get(e.saddle).add(e.tagB);
		    	  }
		      }

		      // this might be made more efficient bc it involves a lot of redundant re-tracing
		      for (long inflow : inflows) {
		    	  long cur = inflow;
		    	  while (true) {
		    		  cur = chunkMst.getDeadendAsNull(cur);
		    		  if (cur == PointIndex.NULL) {
		    			  break;
		    		  } else if (!pf.isParent(cur)) {
		    			  c.output(outPatchPanel, KV.of(inflow, cur));
		    			  break;
		    		  }
		    	  }
		      }
			  Set<Object> traceStart = new HashSet<>();
			  for (long ix : keyPoints) {
				  if (chunkMst.basinSaddles.containsKey(ix)) {
					  for (int traceNum : chunkMst.basinSaddles.get(ix)) {
						  traceStart.add(new BasinSaddleEdge(ix, traceNum));
					  }
				  } else {
					  traceStart.add(ix);
				  }
			  }
			  for (Object start : traceStart) {
				  long cur = chunkMst.getDeadendAsNull(start);
				  while (true) {
		    		  if (cur == PointIndex.NULL) {
		    			  break;
		    		  } else if (!pf.isParent(cur)) {
		    			  c.output(outRelevantInflows, cur);
		    			  break;
		    		  }
		    		  cur = chunkMst.getDeadendAsNull(cur);
				  }
			  }		  		  
		    }
		  }
		).withOutputTags(outPatchPanel, TupleTagList.of(outRelevantInflows))
	);

	    mstTrace.get(outPatchPanel)
	    .apply("writepp", FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "patchpanel.spatialite";
					}
	            }).withNumShards(1));

	    mstTrace.get(outRelevantInflows).apply(FileIO.<Long>write()
	            .via(new PointsDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "keyinflows.spatialite";
					}
	            }).withNumShards(1));

      
	  PCollection<KV<Integer, Iterable<KV<Long, Long>>>> patchPanelSingleton =
			  mstTrace.get(outPatchPanel).apply("PatchPanelSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, KV<Long, Long>>>() {})
					  .via(e -> KV.of(0, e))).apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Long>>> relevantInflowsSingleton =
			  mstTrace.get(outRelevantInflows).apply(Distinct.create()).apply("RelevantInflowsSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, Long>>() {})
					  .via(e -> KV.of(0, e))).apply(GroupByKey.create());

	  final TupleTag<Iterable<KV<Long, Long>>> ppTag = new TupleTag<Iterable<KV<Long, Long>>>(){};
	  final TupleTag<Iterable<Long>> riTag = new TupleTag<Iterable<Long>>() {};
      PCollection<Long> allRelevantInflows = KeyedPCollectionTuple
			    .of(ppTag, patchPanelSingleton)
			    .and(riTag, relevantInflowsSingleton)
			    .apply(CoGroupByKey.create())
			    .apply("TracePatchPanel"+ud(up), ParDo.of(
		  new DoFn<KV<Integer, CoGbkResult>, Long>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Integer, CoGbkResult> e = c.element();
		      Iterable<KV<Long, Long>> patchPanel = e.getValue().getOnly(ppTag, Lists.newArrayList());
		      Iterable<Long> relevantInflows = e.getValue().getOnly(riTag, Lists.newArrayList());
		      
		      MST chunkMst = new MST();
		      for (KV<Long, Long> edge : patchPanel) {
	    		  chunkMst.backtrace.put(edge.getKey(), edge.getValue());
		      }
		      Set<Long> seen = new HashSet<>();
		      for (long start : relevantInflows) {
		    	  long cur = start;
		    	  while (true) {
		    		  c.output(cur);
		    		  seen.add(cur);
		    		  cur = chunkMst.getDeadendAsNull(cur);
		    		  if (cur == PointIndex.NULL || seen.contains(cur)) {
		    			  break;
		    		  }
		    	  }
		      }
		    }
		  }
		));
	    allRelevantInflows.apply(FileIO.<Long>write()
	            .via(new PointsDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "cascadedinflows.spatialite";
					}
	            }).withNumShards(1));

      PCollection<KV<Prefix, Iterable<Long>>> inflowsByChunk = 
			  allRelevantInflows.apply("RechunkInflows"+ud(up), MapElements.into(new TypeDescriptor<KV<Prefix, Long>>() {}).via(inflow -> 
			  KV.of(ProminencePipeline.chunkingPrefix(inflow, TopologyNetworkPipeline.CHUNK_SIZE_EXP), inflow)))
	  		.apply(GroupByKey.create());
      final TupleTag<Iterable<Long>> inflowsTag = new TupleTag<Iterable<Long>>() {};
      final TupleTag<PrunedEdge> outCompleteEdge = new TupleTag<PrunedEdge>() {};
      final TupleTag<PrunedEdge> outIncompleteEdge = new TupleTag<PrunedEdge>() {};
      final TupleTag<Long> outInflowJunctions = new TupleTag<Long>() {};      
      PCollectionTuple edgesOut = KeyedPCollectionTuple
			    .of(mstTag, mstChunked)
			    .and(keyPointsTag, keyPointsChunked)
			    .and(inflowsTag, inflowsByChunk)
			    .apply(CoGroupByKey.create())
			    .apply("ChunkedMstToEdges"+ud(up), ParDo.of(
		  new DoFn<KV<Prefix, CoGbkResult>, PrunedEdge>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Prefix, CoGbkResult> elem = c.element();
		      Prefix pf = elem.getKey();
		      Iterable<Edge> mst = elem.getValue().getOnly(mstTag, Lists.newArrayList());
		      Iterable<Long> keyPointsIt = elem.getValue().getOnly(keyPointsTag, Lists.newArrayList());
		      Iterable<Long> inflowsIt = elem.getValue().getOnly(inflowsTag, Lists.newArrayList());
		      
		      MST chunkMst = new MST();
		      Map<Long, Long> saddles = new HashMap<>();
		      for (Edge e : mst) {
		    	  if (e.a != PointIndex.NULL) {
		    		  chunkMst.backtrace.put(e.a, e.b);
		    		  saddles.put(e.a, e.saddle);
		    	  } else {
		    		  chunkMst.backtrace.put(new BasinSaddleEdge(e.saddle, e.tagB), e.b);
					  chunkMst.basinSaddles.get(e.saddle).add(e.tagB);
		    	  }
		      }
			  
			  Set<Long> keyPoints = Sets.newHashSet(keyPointsIt);
			  Set<Long> inflows = Sets.newHashSet(inflowsIt);
			  Set<Long> junctions = new HashSet<>();
			  Set<Long> seen = new HashSet<>();
			  
			  Set<Object> traceStart = new HashSet<>();
			  for (long ix : keyPoints) {
				  if (chunkMst.basinSaddles.containsKey(ix)) {
					  for (int traceNum : chunkMst.basinSaddles.get(ix)) {
						  traceStart.add(new BasinSaddleEdge(ix, traceNum));
					  }
				  } else {
					  traceStart.add(ix);
				  }
			  }
			  traceStart.addAll(inflows);
			  for (Object start : traceStart) {
				  long cur;
				  if (start instanceof BasinSaddleEdge) {
					  cur = chunkMst.backtrace.get(start);
				  } else {
					  cur = (long)start;
				  }
				  while (true) {
					  if (seen.contains(cur)) {
						  junctions.add(cur);
						  break;
					  }
					  seen.add(cur);
					  cur = chunkMst.getDeadendAsNull(cur);
					  if (cur == PointIndex.NULL) {
						  break;
					  }
				  }
			  }		  		  
			  traceStart.addAll(junctions);
			  for (Object start : traceStart) {
				  PrunedEdge seg = new PrunedEdge();
				  if (start instanceof BasinSaddleEdge) {
					  BasinSaddleEdge bse = (BasinSaddleEdge)start;
					  seg.srcIx = bse.ix;
					  seg.saddleTraceNum = bse.trace;
				  } else {
					  seg.srcIx = (long)start;
					  seg.interimIxs.add(saddles.get(seg.srcIx));
				  }
				  if (!chunkMst.backtrace.containsKey(start)) {
					  // is root, no edge
					  continue;
				  }
				  long cur = chunkMst.backtrace.get(start);
				  while (true) {
					  if (cur != PointIndex.NULL && !pf.isParent(cur)) {
						  // even if also a junction -- must be resolved in coalesce stage
						  // think this special case is irrelevant with bugfix? (OR MAYBE NOT)
						  seg.dstIx = cur;
						  c.output(outIncompleteEdge, seg);
						  break;
					  } else if (junctions.contains(cur) || cur == PointIndex.NULL) {
						  seg.dstIx = cur;
						  boolean continuation = inflows.contains(seg.srcIx) && !keyPoints.contains(seg.srcIx) && !junctions.contains(seg.srcIx);
						  c.output(continuation ? outIncompleteEdge : outCompleteEdge, seg);
						  if (continuation && cur != PointIndex.NULL) {
							  c.output(outInflowJunctions, seg.dstIx);
						  }
						  break;
					  }
					  if (chunkMst.backtrace.containsKey(cur)) {
						  seg.interimIxs.add(cur);
						  seg.interimIxs.add(saddles.get(cur));
						  cur = chunkMst.getDeadendAsNull(cur);
					  } else {
						  seg.dstIx = cur;
						  boolean continuation = inflows.contains(seg.srcIx) && !keyPoints.contains(seg.srcIx) && !junctions.contains(seg.srcIx);
						  c.output(continuation ? outIncompleteEdge : outCompleteEdge, seg);
						  if (continuation && cur != PointIndex.NULL) {
							  c.output(outInflowJunctions, seg.dstIx);
						  }
						  break;
					  }
				  }
				  if (seg.dstIx != PointIndex.NULL && inflows.contains(seg.dstIx)) {
					  c.output(outInflowJunctions, seg.dstIx);
				  }
			  }
		    }}).withOutputTags(outCompleteEdge, TupleTagList.of(outIncompleteEdge).and(outInflowJunctions)));

      // if seg dst is an inflow, mark for next stage
      
	    edgesOut.get(outCompleteEdge).apply(MapElements.into(new TypeDescriptor<KV<Long,Long>>() {}).via(e -> KV.of(e.srcIx, e.dstIx)))
	    .apply(FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "compl.spatialite";
					}
	            }).withNumShards(1));
	    edgesOut.get(outIncompleteEdge).apply(MapElements.into(new TypeDescriptor<KV<Long,Long>>() {}).via(e -> KV.of(e.srcIx, e.dstIx)))
	    .apply(FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "incompl.spatialite";
					}
	            }).withNumShards(1));

      
	  PCollection<KV<Integer, Iterable<PrunedEdge>>> incompleteEdgesSingleton =
			  edgesOut.get(outIncompleteEdge).apply("IncompleteEdgesSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, PrunedEdge>>() {})
					  .via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());
	  PCollection<KV<Integer, Iterable<Long>>> inflowJunctionsSingleton =
			  edgesOut.get(outInflowJunctions).apply("InflowJunctionsSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, Long>>() {})
					  .via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());

      final TupleTag<Iterable<PrunedEdge>> incEdge = new TupleTag<>();
      final TupleTag<Iterable<Long>> inflJct = new TupleTag<>();      
	  PCollection<PrunedEdge> completedEdges = KeyedPCollectionTuple
			    .of(incEdge, incompleteEdgesSingleton)
			    .and(inflJct, inflowJunctionsSingleton)
			    .apply(CoGroupByKey.create())
			    .apply("MergeIncompleteEdges"+ud(up), ParDo.of(
		  new DoFn<KV<Integer, CoGbkResult>, PrunedEdge>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Integer, CoGbkResult> elem = c.element();
		      Iterable<PrunedEdge> edges = elem.getValue().getOnly(incEdge, Lists.newArrayList());
		      Set<Long> inflowJunctions = Sets.newHashSet(elem.getValue().getOnly(inflJct, Lists.newArrayList()));
		      
				      
				      Map<Object, PrunedEdge> edgeMap = new HashMap<>();
				      
				      MST chunkMst = new MST();
				      for (PrunedEdge pe : edges) {
				    	  if (pe.saddleTraceNum == -1) {
				    		  chunkMst.backtrace.put(pe.srcIx, pe.dstIx);
				    		  edgeMap.put(pe.srcIx, pe);
				    	  } else {
				    		  chunkMst.backtrace.put(new BasinSaddleEdge(pe.srcIx, pe.saddleTraceNum), pe.dstIx);
							  chunkMst.basinSaddles.get(pe.srcIx).add(pe.saddleTraceNum);				    		  
				    		  edgeMap.put(new BasinSaddleEdge(pe.srcIx, pe.saddleTraceNum), pe);
				    	  }
				      }
					  
					  Set<Long> junctions = new HashSet<>();
					  Set<Long> seen = new HashSet<>();					  
					  Set<Object> traceStart = new HashSet<>();
					  Set<Long> allDst = new HashSet<>();
					  allDst.addAll(chunkMst.backtrace.values());
					  for (Object o : chunkMst.backtrace.keySet()) {
						  if (o instanceof BasinSaddleEdge) {
							  traceStart.add(o);
						  } else {
							  long ix = (long)o;
							  if (!allDst.contains(ix)) {
								  traceStart.add(ix);
							  }
						  }
					  }
					  traceStart.addAll(inflowJunctions);
					  for (Object start : traceStart) {
						  long cur;
						  if (start instanceof BasinSaddleEdge) {
							  cur = chunkMst.backtrace.get(start);
						  } else {
							  cur = (long)start;
						  }
						  while (true) {
							  if (seen.contains(cur)) {
								  junctions.add(cur);
								  break;
							  }
							  seen.add(cur);
							  cur = chunkMst.getDeadendAsNull(cur);
							  if (cur == PointIndex.NULL) {
								  break;
							  }
						  }
					  }		  		  
					  traceStart.addAll(junctions);
					  for (Object start : traceStart) {
						  PrunedEdge seg = new PrunedEdge();
						  if (start instanceof BasinSaddleEdge) {
							  BasinSaddleEdge bse = (BasinSaddleEdge)start;
							  seg.srcIx = bse.ix;
							  seg.saddleTraceNum = bse.trace;
							  seg.interimIxs.addAll(edgeMap.get(start).interimIxs);
						  } else {
							  seg.srcIx = (long)start;
							  if (edgeMap.containsKey(start)) {
								  // i think this should always be true since junctions should only occur at known edge starts?
								  seg.interimIxs.addAll(edgeMap.get(start).interimIxs);
							  }
						  }
						  if (!chunkMst.backtrace.containsKey(start)) {
							  // is root (terminal junction), no edge
							  continue;
						  }
						  long cur = chunkMst.backtrace.get(start);
						  while (true) {
							  if (junctions.contains(cur) || cur == PointIndex.NULL || !chunkMst.backtrace.containsKey(cur)) {
								  seg.dstIx = cur;
								  break;
							  }
							  // don't add -- just an inflow marker? (but still a legit point right?); not adding breaks tracing
							  seg.interimIxs.add(cur);
							  if (edgeMap.containsKey(cur)) {
								  // should always be true??
								  seg.interimIxs.addAll(edgeMap.get(cur).interimIxs);
							  }
							  cur = chunkMst.getDeadendAsNull(cur);
						  }
						  c.output(seg);
					  }
				    }}));

	    completedEdges.apply(MapElements.into(new TypeDescriptor<KV<Long,Long>>() {}).via(e -> KV.of(e.srcIx, e.dstIx)))
	    .apply(FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "finalcompl.spatialite";
					}
	            }).withNumShards(1));

	  
	  PCollection<PrunedEdge> pmst = PCollectionList.of(edgesOut.get(outCompleteEdge)).and(completedEdges)
			  .apply(Flatten.pCollections());
      // may need to strip out breadcrumbs from edges when reconstituting pmst (for memory reasons)
	  // (but only if all PrunedEdges are kept in memory, rather than processed as iterable
	  PCollection<KV<Integer, Iterable<PrunedEdge>>> pmstSingleton =
			  pmst.apply("PrunedMstSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, PrunedEdge>>() {}).via(e -> KV.of(0, e)))
	  		.apply(GroupByKey.create());
	  
	    pmst.apply(MapElements.into(new TypeDescriptor<KV<Long,Long>>() {}).via(pe -> KV.of(pe.srcIx, pe.dstIx)))
	    .apply(FileIO.<KV<Long, Long>>write()
	            .via(new MSTDebugSink())
	            .to(debugDst).withNaming(new FileNaming() {
					@Override
					public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							Compression compression) {
						return "pmst.spatialite";
					}
	            }).withNumShards(1));

	  
	  PCollection<KV<Integer, Iterable<PathTask>>> taskSingleton =
			  tasks.apply("TasksSingleton"+ud(up), MapElements.into(new TypeDescriptor<KV<Integer, PathTask>>() {}).via(task -> KV.of(0, task)))
	  		.apply(GroupByKey.create());
	  
	  final TupleTag<Iterable<PathTask>> taskTag = new TupleTag<Iterable<PathTask>>() {};
	  final TupleTag<Iterable<PrunedEdge>> pmstTag = new TupleTag<Iterable<PrunedEdge>>() {};
      PCollection<PathFragments> coarsePaths = KeyedPCollectionTuple
			    .of(taskTag, taskSingleton)
			    .and(pmstTag, pmstSingleton)
			    .apply(CoGroupByKey.create())
			    .apply("SearchPaths"+ud(up), ParDo.of(
		  new DoFn<KV<Integer, CoGbkResult>, PathFragments>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Integer, CoGbkResult> e = c.element();
		      Iterable<PathTask> tasks = e.getValue().getOnly(taskTag);
		      Iterable<PrunedEdge> pmst = e.getValue().getOnly(pmstTag);
		      
		      PathSearcher searcher = new PathSearcher(pmst) {
		    	  public void emitPath(PathFragments pf) {
		    		  c.output(pf);
		    	  }
		    	  /*
		    	  public void emitEdge(TrimmedEdge seg) {
		    		  // TODO
		    	  }
		    	  */
		      };
		      for (PathTask task : tasks) {
		    	  searcher.search(task);
		      }
		    }
		  }
		));
	  
      return PCollectionTuple.of(pathFragsTag, coarsePaths).and(fullPmstTag, pmst);
  }
  
  static PCollection<KV<EdgeId, PrunedEdge>> indexPMST(PCollection<PrunedEdge> pmst, boolean up) {
	  return pmst.apply(MapElements.into(new TypeDescriptor<KV<EdgeId, PrunedEdge>>() {})
			  .via(pe -> KV.of(new EdgeId(pe.srcIx, pe.saddleTraceNum, up), pe)));
  }
  
  public static PCollection<PromFact> fillOutPaths(PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage,
		  PCollectionTuple searchUp, PCollectionTuple searchDown) {
      PCollection<KV<EdgeId, PrunedEdge>> indexedEdges = PCollectionList
    		  .of(indexPMST(searchUp.get(fullPmstTag), true))
    		  .and(indexPMST(searchDown.get(fullPmstTag), false))
    		  .apply(Flatten.pCollections());

      PCollection<KV<Long, EdgeId>> saddlesToEdges = indexedEdges.apply("SaddlesPerPMSTEdge",
    		  ParDo.of(new DoFn<KV<EdgeId, PrunedEdge>, KV<Long, EdgeId>>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		    	EdgeId eid = c.element().getKey();
		    	PrunedEdge pe = c.element().getValue();
		    	boolean isSaddle = (pe.saddleTraceNum != -1);
		    	if (isSaddle) {
		    		c.output(KV.of(pe.srcIx, eid));
		    	}
		    	for (int i = (isSaddle ? 1 : 0); i < pe.interimIxs.size(); i += 2) {
		    		c.output(KV.of(pe.interimIxs.get(i), eid));
		    	}
		    }
      }));
      PCollection<KV<Prefix, Iterable<KV<Long, Boolean>>>> chunkedSaddles = 
    		  saddlesToEdges.apply(MapElements.into(new TypeDescriptor<KV<Long, Boolean>>() {})
    				  .via(kv -> KV.of(kv.getKey(), kv.getValue().up)))
    		  .apply(Distinct.create()).apply(MapElements.into(new TypeDescriptor<KV<Prefix, KV<Long, Boolean>>>() {})
           .via(kv -> KV.of(new Prefix(kv.getKey(), TopologyNetworkPipeline.CHUNK_SIZE_EXP), kv)))
	    		.apply("ChunkedSaddles", GroupByKey.create());//.apply(Values.create());*/
      PCollection<KV<Long, KV<Integer, List<Long>>>> saddleTraces = chunkedSaddles.apply("DetailedTraceSaddles",
    		  ParDo.of(new TopologyTracer(pageCoverage)).withSideInputs(pageCoverage));
	  final TupleTag<EdgeId> peTag = new TupleTag<>();
	  final TupleTag<KV<Integer, List<Long>>> traceTag = new TupleTag<>();
	  
      PCollection<KV<EdgeId, KV<Long, KV<Integer, List<Long>>>>> tracesCrossRefed = KeyedPCollectionTuple
			    .of(peTag, saddlesToEdges)
			    .and(traceTag, saddleTraces)
			    .apply(CoGroupByKey.create())
			    .apply("TraceCrossrefToPMSTEdge", ParDo.of(
		  new DoFn<KV<Long, CoGbkResult>, KV<EdgeId, KV<Long, KV<Integer, List<Long>>>>>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Long, CoGbkResult> e = c.element();
		      long saddle = e.getKey();
		      List<EdgeId> edges = Lists.newArrayList(e.getValue().getAll(peTag));
		      for (KV<Integer, List<Long>> xx : e.getValue().getAll(traceTag)) {
		    	  int dir = xx.getKey();
		    	  boolean up = (dir % 2 == 0);
		    	  List<Long> path = xx.getValue();
		    	  for (EdgeId eid : edges) {
		    		  boolean emit = (eid.ix != saddle || eid.dir == dir) && eid.up == up;
		    		  if (emit) {
		    			  c.output(KV.of(eid, KV.of(saddle, KV.of(dir, path))));
		    		  }
		    	  }
		      }
		    }
		  }
		));
	  final TupleTag<PrunedEdge> qqqqq = new TupleTag<>();
	  final TupleTag<KV<Long, KV<Integer, List<Long>>>> wwwww = new TupleTag<>();
      PCollection<PrunedEdge> detailedPMST = KeyedPCollectionTuple
			    .of(wwwww, tracesCrossRefed)
			    .and(qqqqq, indexedEdges)
			    .apply(CoGroupByKey.create())
			    .apply("AssembleDetailedPMST", ParDo.of(
		  new DoFn<KV<EdgeId, CoGbkResult>, PrunedEdge>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<EdgeId, CoGbkResult> e = c.element();
		      PrunedEdge pe = e.getValue().getOnly(qqqqq);
		      
		      Map<KV<Long, Long>, List<Long>> traces = new HashMap<>();
		      for (KV<Long, KV<Integer, List<Long>>> trace : e.getValue().getAll(wwwww)) {
		    	  long saddle = trace.getKey();
		    	  List<Long> path = trace.getValue().getValue();
		    	  long terminus = path.get(path.size() - 1);
		    	  traces.put(KV.of(saddle, terminus), path);
		      }

		      PrunedEdge detail = new PrunedEdge();
		      detail.srcIx = pe.srcIx;
		      detail.dstIx = pe.dstIx;
		      detail.saddleTraceNum = pe.saddleTraceNum;
		      for (int i = 0; i < pe.interimIxs.size() + 1; i++) {
		    	  long start = (i == 0 ? pe.srcIx : pe.interimIxs.get(i - 1));
		    	  long end = (i == pe.interimIxs.size() ? pe.dstIx : pe.interimIxs.get(i));
		    	  boolean startIsSaddle = (i % 2 == (pe.saddleTraceNum == -1 ? 1 : 0));
		    	  long saddle = (startIsSaddle ? start : end);
		    	  long summit = (startIsSaddle ? end : start);
		    	  List<Long> seg = traces.get(KV.of(saddle, summit));
		    	  seg = seg.subList(0, seg.size() - 1); // final point redundant with pe.interimIxs (start point already absent)
		    	  if (!startIsSaddle) {
		    		  Collections.reverse(seg);
		    	  }
		    	  detail.interimIxs.addAll(seg);
		    	  if (i < pe.interimIxs.size()) {
		    		  detail.interimIxs.add(pe.interimIxs.get(i));
		    	  }
		      }
		      c.output(detail);
		    }
		  }
		));
      
      PCollection<PathFragments> coarsePaths = PCollectionList.of(searchUp.get(pathFragsTag)).and(searchDown.get(pathFragsTag)).apply(Flatten.pCollections());
      PCollection<KV<Long, PathTaskId>> mstNodeToPath = coarsePaths.apply(ParDo.of(
    		  new DoFn<PathFragments, KV<Long, PathTaskId>>() {
    			    @ProcessElement
    			    public void processElement(ProcessContext c) {
    			    	PathFragments path = c.element();
    			    	for (List<Long> seg : path.fragments) {
    			    		for (Long ix : seg) {
    			    			// FIXME need special handling for BasinSaddleEdges
    			    			if (ix != PointIndex.NULL) {
    			    				c.output(KV.of(ix, new PathTaskId(path.p.ix, path.type)));
    			    			}
    			    		}
    			    	}
    			    }
    		  }));
	  PCollection<KV<Long, PrunedEdge>> mstNodeToEdge = detailedPMST.apply(MapElements.into(new TypeDescriptor<KV<Long, PrunedEdge>>() {}).via(pe -> KV.of(pe.srcIx, pe)));

	  final TupleTag<PathTaskId> aa = new TupleTag<>();
	  final TupleTag<PrunedEdge> bb = new TupleTag<>();
	  PCollection<KV<PathTaskId, PrunedEdge>> edgesByPath = KeyedPCollectionTuple
	  .of(aa, mstNodeToPath)
	  .and(bb, mstNodeToEdge)
	  .apply(CoGroupByKey.create())
	  .apply("CrossrefPathsToEdges", ParDo.of(
		  new DoFn<KV<Long, CoGbkResult>, KV<PathTaskId, PrunedEdge>>() {
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<Long, CoGbkResult> e = c.element();
		      Iterable<PathTaskId> tasks = e.getValue().getAll(aa);
		      List<PrunedEdge> edges = Lists.newArrayList(e.getValue().getAll(bb)); // should be a getonly except for basin saddle edges
		      
		      for (PathTaskId task : tasks) {
		    	  for (PrunedEdge pe : edges) {
		    		  c.output(KV.of(task, pe));
		    	  }
		      }
		    }
		  }
		));

	  PCollection<KV<PathTaskId, PathFragments>> pathsById = coarsePaths.apply(MapElements.into(new TypeDescriptor<KV<PathTaskId, PathFragments>>() {}).via(pf -> KV.of(new PathTaskId(pf.p.ix, pf.type), pf)));
	  final TupleTag<PathFragments> cc = new TupleTag<>();
	  PCollection<PromFact> finePaths = KeyedPCollectionTuple
	  .of(cc, pathsById)
	  .and(bb, edgesByPath)
	  .apply(CoGroupByKey.create())
	  .apply("CoarseToFinePaths", ParDo.of(
		  new DoFn<KV<PathTaskId, CoGbkResult>, PromFact>() {
		      Map<Long, PrunedEdge> edges = new HashMap<>();
		      Map<BasinSaddleEdge, PrunedEdge> basinSaddleEdges = new HashMap<>();  
			  
		    @ProcessElement
		    public void processElement(ProcessContext c) {
		      KV<PathTaskId, CoGbkResult> e = c.element();
		      PathFragments frags = e.getValue().getOnly(cc);
		      Iterable<PrunedEdge> eIt = e.getValue().getAll(bb);

		      for (PrunedEdge pe : eIt) {
		    	  if (pe.saddleTraceNum == -1) {
		    		  edges.put(pe.srcIx, pe);
		    	  } else {
		    		  basinSaddleEdges.put(new BasinSaddleEdge(pe.srcIx, pe.saddleTraceNum), pe);
		    	  }
		      }

		      PromFact fact = new PromFact();
		    	fact.p = frags.p;
		    	
		    	if (frags.type == PathTask.TYPE_THRESH || frags.type == PathTask.TYPE_PARENT) {
		    		List<Long> path = assemble(frags.fragments.get(0));
		    		List<Long> contra = assemble(frags.fragments.get(1));
		    		Collections.reverse(contra);
		    		path.addAll(contra.subList(1, contra.size()));
		    		if (frags.type == PathTask.TYPE_THRESH) {
		    			fact.threshPath = path;
		    		} else {
		    			fact.parentPath = path;
		    		}
		    	} else if (frags.type == PathTask.TYPE_DOMAIN) {
		    		fact.domainBoundary = new ArrayList<>();
		    		for (List<Long> frag : frags.fragments) {
		    			frag = assemble(frag);
		    			if (frag.get(frag.size() - 1) == PointIndex.NULL) {
		    				frag = frag.subList(0, frag.size() - 1);
		    			}
		    			fact.domainBoundary.add(frag);
		    		}
		    	}
		    	c.output(fact);

		    }
		    
		    List<Long> assemble(List<Long> keys) {
		    	List<Long> seg = new ArrayList<>();
		    	for (int i = 0; i < keys.size(); i++) {
		    		long ix = keys.get(i);
		    		seg.add(ix);
		    		if (i < keys.size() - 1) {
		    			PrunedEdge pe = edges.get(ix);
		    			if (pe == null && i == 0) {
		    				for (PrunedEdge bspe : basinSaddleEdges.values()) {
		    					if (bspe.srcIx == ix && bspe.dstIx == keys.get(1)) {
		    						pe = bspe;
		    						break;
		    					}
		    				}
		    			}
		    			if (pe == null) {
		    				// shouldn't happen but plow forward
		    				continue;
		    			}
		    			seg.addAll(pe.interimIxs);
		    		}
		    	}
		    	return seg;
		    }
		  }
		));
	  return finePaths;
  }
  
  public static class PathPipeline implements Serializable {
	  transient Pipeline p;
	  PromPipeline pp;
	  
	  public PathPipeline (PromPipeline pp) {
		  this.pp = pp;
		  this.p = pp.p;
	  }
	  
	  public void freshRun() {
		  PCollection<PromFact> promInfo = pp.facts;
		    PCollection<Edge> mstUp = pp.mstUp;
		    PCollection<Edge> mstDown = pp.mstDown;
		    PCollection<Edge> rawNetworkUp = pp.tp.networkUp;
		    PCollection<Edge> rawNetworkDown = pp.tp.networkDown;

		    PCollectionList<PromFact> promByDir = promInfo.apply("SplitPromFacts", Partition.of(2, new PartitionFn<PromFact>() {
				@Override
				public int partitionFor(PromFact e, int numPartitions) {
					return Point.compareElev(e.p, e.saddle.s) > 0 ? 0 : 1;
				}
		    }));
		    PCollection<PromFact> promInfoUp = promByDir.get(0);
		    PCollection<PromFact> promInfoDown = promByDir.get(1);

		    PCollectionTuple pathSearchUp = searchPaths(true, pp.tp.outputRoot, promInfoUp, promInfoDown, mstUp, rawNetworkUp);
		    PCollectionTuple pathSearchDown = searchPaths(false, pp.tp.outputRoot, promInfoDown, promInfoUp, mstDown, rawNetworkDown);
		    PCollection<PromFact> paths = fillOutPaths(pp.tp.pageCoverage, pathSearchUp, pathSearchDown);
		    
		    promInfo = ProminencePipeline.consolidatePromFacts(PCollectionList.of(promInfo).and(paths));
		    //promInfo.apply("WritePromFactsWithPaths", AvroIO.write(PromFact.class).to(pp.tp.outputRoot + "promfactswithpaths").withoutSharding());

		    promInfo.apply("WriteSpatialite", FileIO.<PromFact>write()
		            .via(new SpatialiteSink())
		            .to(pp.tp.outputRoot).withNaming(new FileNaming() {
						@Override
						public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
								Compression compression) {
							return "promout.spatialite";
						}
		            }).withNumShards(1));
	  }
	  
  }
  
  public static void main(String[] args) {
	  TopoPipeline tp = new TopoPipeline(args);
	  tp.initDEMs();
	  tp.previousRun();
	  PromPipeline pp = new PromPipeline(tp);
	  pp.previousRun();
	  PathPipeline pthp = new PathPipeline(pp);
	  pthp.freshRun();
	  pthp.p.run();
  }
}
