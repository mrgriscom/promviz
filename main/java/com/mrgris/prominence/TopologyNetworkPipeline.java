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

  final static int PAGE_SIZE_EXP = 9;
  final static int CHUNK_SIZE_EXP = 11;
  static Prefix segmentPrefix(long ix) { return new Prefix(ix, PAGE_SIZE_EXP); }
    
  static PCollection<KV<Prefix, DEMFile>> makePageFileMapping (Pipeline p) {
	    PCollection<DEMFile> DEMs = p.apply(Create.of(
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N38W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 38., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N38W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 38., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N39W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 39., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N39W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 39., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N39W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 39., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N40W078.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 40., -78.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N41W078.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 41., -78.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N42W078.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 42., -78.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N43W078.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 43., -78.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W062.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -62.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W063.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -63.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W064.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -64.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W077.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -77.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N44W078.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 44., -78.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W060.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -60.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W061.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -61.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W062.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -62.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W063.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -63.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W064.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -64.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N45W076.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 45., -76.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W060.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -60.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W061.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -61.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W062.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -62.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W063.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -63.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W064.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -64.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W074.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -74.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N46W075.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 46., -75.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W060.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -60.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W061.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -61.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W062.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -62.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W063.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -63.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W064.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -64.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W072.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -72.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N47W073.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 47., -73.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -69.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W070.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -70.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N48W071.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 48., -71.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W064.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -64.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W065.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -65.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W066.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -66.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W067.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -67.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W068.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -68.),
	    	    new DEMFile("https://storage.googleapis.com/mrgris-dem/ferranti3/N49W069.hgt.gz", DEMFile.GRID_GEO_3AS, DEMFile.FORMAT_SRTM_HGT, 1201, 1201, 49., -69.)
	    ));
	    return DEMs.apply("DemFilesToOverlappingPages", ParDo.of(new DoFn<DEMFile, KV<Prefix, DEMFile>>() {
		      @ProcessElement
		      public void processElement(ProcessContext c) {
		    	DEMFile dem = c.element();
				int[] pmin = PointIndex.split(segmentPrefix(dem.genIx(0, 0)).prefix);
				int[] pmax = PointIndex.split(segmentPrefix(dem.genAbsIx(dem.xmax(), dem.ymax())).prefix);
				int proj = pmin[0], x0 = pmin[1], y0 = pmin[2], x1 = pmax[1], y1 = pmax[2];
				Prefix[] pages = Prefix.tileInclusive(proj, x0, y0, x1, y1, PAGE_SIZE_EXP);
				for (Prefix page : pages) {
					c.output(KV.of(page, dem));
				}
		      }    	
	    }));
  }
  
  public static void main(String[] args) {
	
	// TODO: custom options and validation
	// --output=gs://mrgris-dataflow-test/output-file-prefix
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).create());

    PCollection<KV<Prefix, DEMFile>> pageFileMapping = makePageFileMapping(p);
    final PCollectionView<Map<Prefix, Iterable<DEMFile>>> pageCoverage = pageFileMapping.apply(View.asMultimap());
    PCollection<Prefix> chunks = pageFileMapping.apply(
    		MapElements.into(new TypeDescriptor<Prefix>() {}).via(kv -> kv.getKey()))
    		.apply(Distinct.create())
    		.apply(MapElements.into(new TypeDescriptor<Prefix>() {}).via(pr -> new Prefix(pr, CHUNK_SIZE_EXP)))
    		.apply(Distinct.create());
    
    final TupleTag<Edge> upEdgesTag = new TupleTag<Edge>(){};
    final TupleTag<Edge> downEdgesTag = new TupleTag<Edge>(){};    
    PCollectionTuple network = chunks.apply("GenerateNetwork",
    		ParDo.of(new TopologyBuilder(pageCoverage, downEdgesTag)).withSideInputs(pageCoverage)
    		.withOutputTags(upEdgesTag, TupleTagList.of(downEdgesTag)));
    
    network.get(upEdgesTag).apply("OutputUp",
    	    AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/network-up"));
    network.get(downEdgesTag).apply("OutputDown",
    	    AvroIO.write(Edge.class).to("gs://mrgris-dataflow-test/network-down"));
    
    p.run();
  }
}
