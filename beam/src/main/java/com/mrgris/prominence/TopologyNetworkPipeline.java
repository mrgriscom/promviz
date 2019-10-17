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
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.dem.DEMIndex;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

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
public class TopologyNetworkPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(TopologyNetworkPipeline.class);

  public static final String DEM_ROOT = "https://storage.googleapis.com/mrgris-dem/";
  
  final static int PAGE_SIZE_EXP = 9;
  final static int CHUNK_SIZE_EXP = 11;
  static Prefix segmentPrefix(long ix) { return new Prefix(ix, PAGE_SIZE_EXP); }
    
  // TODO make avro datastructures first-class citizens
  
  static String global = ":global";
  
  static DEMIndex loadDemIndex() {
	  return DEMIndex.instance();
  }
  
  static PCollection<KV<Prefix, DEMFile>> makePageFileMapping (Pipeline p, String region, String series) {
	  DEMIndex index = loadDemIndex();
	  GeometryFactory gf = new GeometryFactory();
	  WKTReader wkt = new WKTReader(gf);
	  
	  boolean isGlobal = region.startsWith(global); 
	  Geometry regionPoly = null;
	  if (!isGlobal) {
		  try {
			  regionPoly = wkt.read(region);
		  } catch (ParseException e) {
			  throw new RuntimeException("invalid " + region);
		  }
	  }

	  List<DEMFile> DEMs = new ArrayList<>();
	  for (DEMFile dem : index.dems) {
		  if (!dem.path.startsWith(series + "/")) {
			  continue;
		  }
		  boolean include;
		  try {
			  include = isGlobal || regionPoly.intersects(wkt.read(dem.bound));
		  } catch (ParseException e) {
			  throw new RuntimeException("invalid " + dem.bound);
		  }
		  if (include) {
			  LOG.info("DEM " + dem.path);
			  DEMs.add(dem);
		  }
	  }
	  
	    return p.apply("FilterDemsForRegion", Create.of(DEMs)).apply("DemFilesToOverlappingPages", ParDo.of(new DoFn<DEMFile, KV<Prefix, DEMFile>>() {
		      @ProcessElement
		      public void processElement(ProcessContext c) {
		    	DEMFile dem = c.element();
				for (Prefix page : dem.overlappingPages(PAGE_SIZE_EXP)) {
					c.output(KV.of(page, dem));
				}
		      }    	
	    }));
  }
  
  public static class DEMContext implements Serializable {
	  String region;
	  String series;
	  
	  public DEMContext(String region, String series) {
		  this.region = region;
		  this.series = series;
	  }
	  
	  public void init(TopoPipeline tp) {
		  tp.pageFileMapping = makePageFileMapping(tp.p, region, series);
		  tp.pageCoverage = tp.pageFileMapping.apply(View.asMultimap());
	  }
  }
  
  public static class TopoPipeline implements Serializable {
	  transient Pipeline p;
	  transient DEMContext demCtx;
	  
	  transient PCollection<KV<Prefix, DEMFile>> pageFileMapping;
	  transient PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage;
	  
	  transient PCollection<Edge> networkUp;
	  transient PCollection<Edge> networkDown;
		  
	  public interface MyOptions extends PipelineOptions {
		  String getBound();
		  void setBound(String _);
		  String getSeries();
		  void setSeries(String _);
	  }
	  
	  public TopoPipeline(String args[]) {
		  PipelineOptionsFactory.register(MyOptions.class);
		  MyOptions options = PipelineOptionsFactory.fromArgs(args)
				  									.withValidation()
		                                            .as(MyOptions.class);
		  
		    p = Pipeline.create(options);
		    this.demCtx = new DEMContext(options.getBound(), options.getSeries());
	  }
	  
	  public void initDEMs() {
		  if (pageFileMapping == null) {
			  demCtx.init(this);
		  }
	  }
	  
	  public void freshRun (boolean write) {
		  initDEMs();
		  PCollection<KV<Prefix, DEMFile>> pfm = this.pageFileMapping;
		    PCollection<Prefix> chunks = pfm.apply("PagesToChunks",
		    		MapElements.into(new TypeDescriptor<Prefix>() {}).via(kv -> kv.getKey()))
		    		.apply(Distinct.create())
		    		.apply(MapElements.into(new TypeDescriptor<Prefix>() {}).via(pr -> new Prefix(pr, CHUNK_SIZE_EXP)))
		    		.apply(Distinct.create());
		    
		    final TupleTag<Edge> upEdgesTag = new TupleTag<Edge>(){};
		    final TupleTag<Edge> downEdgesTag = new TupleTag<Edge>(){};    
		    PCollectionTuple network = chunks.apply("GenerateNetwork",
		    		ParDo.of(new TopologyBuilder(pageCoverage, downEdgesTag)).withSideInputs(pageCoverage)
		    		.withOutputTags(upEdgesTag, TupleTagList.of(downEdgesTag)));
		    
		    networkUp = network.get(upEdgesTag);
		    networkDown = network.get(downEdgesTag);
		    
		    if (write) {
		    	networkUp.apply("WriteRawNetwork-Up",
		    	    AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/network-up"));
		    	networkDown.apply("WriteRawNetwork-Down",
		    	    AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/network-down"));
		    }
	  }
	  
	  public void previousRun() {
		  networkUp = p.apply("LoadRawNetwork-Up", AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/network-up-*"));
		  networkDown = p.apply("LoadRawNetwork-Down", AvroIO.read(Edge.class).from("gs://mrgris-dataflow-test/network-down-*"));
	  }
  }
  
  public static void main(String[] args) {
	  TopoPipeline p = new TopoPipeline(args);
	  p.freshRun(true);
	  p.p.run();
  }
}
