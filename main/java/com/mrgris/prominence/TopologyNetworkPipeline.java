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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
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
import com.mrgris.prominence.dem.DEMIndex;
import com.vividsolutions.jts.geom.Coordinate;
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
  
  static String northAmerica = "34.93999,-74.79492 30.90222,-80.22217 25.60190,-79.36523 24.26700,-80.33203 23.90593,-82.22168 28.57487,-84.77051 27.39128,-95.71289 21.06400,-96.10840 19.82873,-92.85645 22.02455,-90.02197 22.14671,-85.60547 17.39258,-86.48438 16.23577,-81.69434 10.79014,-82.08984 10.01213,-76.68457 7.36247,-76.86035 6.33714,-78.70605 6.79554,-83.32031 11.84585,-90.15381 15.17818,-100.67871 20.13847,-107.60010 24.02640,-113.46680 30.05008,-118.03711 34.36158,-122.27783 39.11301,-125.33203 44.77794,-126.36475 48.26857,-127.08984 52.50953,-133.19824 56.48676,-137.04346 58.12432,-139.65820 58.82512,-148.05176 55.60318,-152.79785 54.25239,-159.87305 52.84259,-166.88232 54.68653,-167.29980 57.51582,-161.41113 58.91599,-168.09082 60.33782,-168.31055 63.29294,-166.42090 65.30265,-168.88184 66.00015,-168.57422 67.12729,-166.46484 68.35870,-167.67334 69.31832,-167.34375 71.20192,-160.29053 71.74643,-156.42334 70.34093,-141.30615 70.16275,-134.14307 70.94536,-128.29834 70.00557,-120.41016 69.11561,-114.85107 68.33438,-109.90723 69.12344,-106.08398 68.21237,-100.30518 68.86352,-97.91016 69.13127,-96.06445 70.64177,-97.51465 71.85623,-96.63574 72.20196,-96.21826 72.19525,-92.98828 69.27171,-87.09961 70.28912,-85.91309 70.09553,-81.56250 62.91523,-79.10156 62.93523,-72.90527 60.73769,-63.98438 55.77657,-57.52441 52.45601,-53.96484 47.42809,-51.67969 45.73686,-53.08594 43.42101,-63.58887 38.95941,-72.90527";
  static String	northeastUS = "43.78696,-77.60742 45.33670,-75.25635 46.98025,-72.55371 48.06340,-70.18066 49.33944,-67.43408 49.53947,-65.14893 48.93693,-63.67676 47.78363,-61.10596 47.24941,-59.85352 46.10371,-59.15039 45.04248,-60.73242 43.27721,-65.45654 41.02964,-69.74121 40.22922,-73.23486 38.66836,-74.72900 38.73695,-75.47607 40.22922,-77.25586";
  static String africa = "38.06539,11.51367 36.77409,-5.88867 27.21556,-15.24902 21.90228,-18.32520 11.43696,-17.70996 2.94304,-7.51465 -37.43997,18.45703 -34.74161,30.05859 -26.74561,48.69141 -13.41099,52.55859 13.75272,53.26172 16.46769,41.04492 32.62087,35.50781";
  static String westernUS = "48.90806,-123.35449 48.89362,-112.93945 45.84411,-108.30322 44.22946,-102.98584 40.81381,-101.16211 37.94420,-101.66748 34.37971,-101.95313 30.69461,-104.52393 31.54109,-108.85254 31.18461,-113.79639 31.24099,-117.39990 37.38762,-123.13477 43.22920,-125.39795";
  static String highAsia = "29.15216,75.84961 26.58853,82.48535 25.56227,88.72559 20.79720,91.31836 15.70766,93.29590 14.85985,97.08618 13.42168,100.26123 13.51784,103.31543 18.41708,106.36963 20.17972,107.40234 25.75042,111.89575 28.98892,116.24634 31.45678,117.31201 34.74161,114.69727 39.30030,116.89453 39.36828,121.02539 44.84029,123.83789 54.34855,128.74878 59.66774,122.95898 61.60640,117.59766 61.27023,107.66602 60.71620,101.60156 59.17593,94.74609 57.23150,83.67188 51.86292,78.66211 52.69636,75.76172 54.67383,68.77441 51.20688,62.27051 47.57653,57.08496 45.85941,49.57031 48.34165,39.11133 47.27923,34.36523 43.48481,37.22168 41.86956,40.38574 42.81152,33.75000 41.24477,25.66406 38.34166,25.40039 35.67515,27.33398 34.37971,33.22266 34.08906,35.04639 32.73184,34.69482 32.26856,35.52979 33.35806,36.60645 34.08906,39.81445 35.40696,41.74805 30.88337,48.36182 26.58853,51.15234 25.56227,54.79980 26.37219,56.46973 25.02588,57.48047 24.12670,61.17188 23.64452,67.76367";
  static String saTest = "-33.39476,17.64954 -34.60156,18.20984 -35.12440,19.91272 -34.37064,23.74695 -34.18909,26.12000 -32.99024,28.29529 -31.01057,30.58594 -29.95018,31.18469 -27.82450,30.21240 -26.98083,26.98242 -30.72295,20.86304 -29.89781,19.11621 -30.43447,17.10571";
  static String test = "42.2,-72.8 42.2,-73.2 41.8,-73";
  
  static DEMIndex loadDemIndex() {
	  return DEMIndex.instance();
  }

  static Polygon makePolygon(GeometryFactory gf, List<Coordinate> coords) {
	  coords.add(coords.get(0));
	  return gf.createPolygon(new CoordinateArraySequence(coords.toArray(new Coordinate[coords.size()])));
  }

  // TODO unused?
  static Polygon makeQuadrant(GeometryFactory gf, double xmin, double xmax, double ymin, double ymax) {
	  List<Coordinate> coords = new ArrayList<>();
	  coords.add(new Coordinate(xmin, ymin));
	  coords.add(new Coordinate(xmax, ymin));
	  coords.add(new Coordinate(xmax, ymax));
	  coords.add(new Coordinate(xmin, ymax));
	  return makePolygon(gf, coords);
  }
  
  static String fmtKey(double flat, double flon) {
	  int lat = (int)Math.round(flat);
	  int lon = (int)Math.round(flon);
	  return String.format("%s%02d%s%03d", lat >= 0 ? "N" : "S", Math.abs(lat), lon >= 0 ? "E" : "W", Math.abs(lon));
  }
  
  static PCollection<KV<Prefix, DEMFile>> makePageFileMapping (Pipeline p) {
	  final String region = test; //northeastUS;
	  final String series = "ferranti3";
	  
	  DEMIndex index = loadDemIndex();
	  GeometryFactory gf = new GeometryFactory();
	  WKTReader wkt = new WKTReader(gf);
	  
	  List<Coordinate> regionCoords = new ArrayList<>();
	  for (String ll : region.split(" ")) {
		  String[] s = ll.split(",");
		  double lat = Double.parseDouble(s[0]);
		  double lon = Double.parseDouble(s[1]);
		  regionCoords.add(new Coordinate(lon, lat));
	  }
	  Polygon regionPoly = makePolygon(gf, regionCoords);

	  List<DEMFile> DEMs = new ArrayList<>();
	  for (DEMFile dem : index.dems) {
		  if (!dem.path.startsWith(series + "/")) {
			  continue;
		  }
		  boolean include = false;
		  try {
			  include = regionPoly.intersects(wkt.read(dem.bound));
		  } catch (ParseException e) {
			  throw new RuntimeException("invalid " + dem.bound);
		  }
		  if (include) {
			  LOG.info("DEM " + dem.path);
			  DEMs.add(dem);
		  }
	  }
	  
	    return p.apply(Create.of(DEMs)).apply("DemFilesToOverlappingPages", ParDo.of(new DoFn<DEMFile, KV<Prefix, DEMFile>>() {
		      @ProcessElement
		      public void processElement(ProcessContext c) {
		    	DEMFile dem = c.element();
				int[] pmin = PointIndex.split(segmentPrefix(dem.genAbsIx(dem.xmin(), dem.ymin())).prefix);
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
