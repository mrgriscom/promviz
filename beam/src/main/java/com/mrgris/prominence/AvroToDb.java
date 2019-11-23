package com.mrgris.prominence;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mrgris.prominence.AvroToDb.PrepDB;
import com.mrgris.prominence.AvroToDb.Record;
import com.mrgris.prominence.AvroToDb.SpatialiteSink;
import com.mrgris.prominence.PathsPipeline.PathPipeline;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.ProminencePipeline.PromPipeline;
import com.mrgris.prominence.TopologyNetworkPipeline.TopoPipeline;
import com.mrgris.prominence.TopologyNetworkPipeline.TopoPipeline.MyOptions;
import com.mrgris.prominence.util.WorkerUtils;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.WKBWriter;

public class AvroToDb {
	
	  private static final Logger LOG = LoggerFactory.getLogger(AvroToDb.class);

	
	static final int TYPE_SUMMIT = 1;
	static final int TYPE_SADDLE = 0;
	static final int TYPE_SINK = -1;

	public static class PrepDB extends DoFn<PromFact, Record> {
		GeometryFactory gf;
		WKBWriter wkb;
		
		@Setup
		public void setup() {
	    	WorkerUtils.initializeGDAL();
			gf = new GeometryFactory();
			wkb = new WKBWriter();
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			PromFact pf = c.element();
			c.output(new Record(pf, this));
		}


	}
	
	// prepare data for insertion into database. all this work is parallelizable (unlike db interaction),
	// so do it upfront
	// if we wanted to get really clever, we could use in-memory spatialite instances here to prep the geometry
	// into the db internal format (passing it just a blob during insertion). but observation shows the db time
	// converting WKB (note: *not* WKT) into internal structure, including compression, to be negligible
	@DefaultCoder(AvroCoder.class)
	public static class Record {
		@DefaultCoder(AvroCoder.class)
		public static class PointRecord {
			long geocode;
			int type;
			byte[] geom;
			Point p;
			
			public PointRecord() {}
			
			public PointRecord(Point p, int type, PrepDB ctx) {
				this.p = p;
				this.geocode = geocode(p);
				this.type = type;
				
				double coords[] = PointIndex.toLatLon(p.ix, true);
				com.vividsolutions.jts.geom.Point pt = ctx.gf.createPoint(new Coordinate(coords[1], coords[0]));
				this.geom = ctx.wkb.write(pt);
			}
			
			public void process(PreparedStatement ps) throws SQLException {
		        ps.setLong(1, geocode);
		        ps.setInt(2, type);
		        ps.setInt(3, (int)(p.elev * 1000.));
		        ps.setInt(4, p.isodist == 0 ? 0 : p.isodist > 0 ? p.isodist - Integer.MAX_VALUE : p.isodist - Integer.MIN_VALUE);
		        ps.setInt(5, -1);
		        ps.setBytes(6, geom);
		        ps.addBatch();
			}
		}

		@DefaultCoder(AvroCoder.class)
		public static class SubsaddleRecord {
			long p;
			long ss;
			boolean elev;
			boolean prom;
			
			public SubsaddleRecord() {}
			
			public SubsaddleRecord(long p, long ss, boolean elev, boolean prom) {
				this.p = p;
				this.ss = ss;
				this.elev = elev;
				this.prom = prom;
			}

			public static ArrayList<SubsaddleRecord> fromPromFact(PromFact pf) {
				// TODO: key this set by geocode, not pointix
				Set<Long> ssElev = new HashSet<>();
				Set<Long> ssProm = new HashSet<>();
				for (PromFact.Saddle ss : pf.elevSubsaddles) {
					ssElev.add(ss.s.ix);
				}
				for (PromFact.Saddle ss : pf.promSubsaddles) {
					ssProm.add(ss.s.ix);
				}
				ArrayList<SubsaddleRecord> subsaddles = new ArrayList<>();
				for (long ss : Sets.union(ssElev, ssProm)) {
					subsaddles.add(new SubsaddleRecord(geocode(pf.p),
							PointIndex.iGeocode(ss),
							ssElev.contains(ss),
							ssProm.contains(ss)));
				}
				return subsaddles;
			}
			
			public void process(PreparedStatement ps) throws SQLException {
				ps.setLong(1, p);
				ps.setLong(2, ss);
				ps.setInt(3, elev ? 1 : 0);
				ps.setInt(4, prom ? 1 : 0);
				ps.addBatch();
			}
		}
		
		PointRecord p;
		PointRecord saddle;
		int promRank;
		boolean minBound;
		@Nullable
		Long parent_geocode;
		@Nullable
		Long pthresh_geocode;
		// avro errors if byte arrays are null -- use zero-length instead
		byte[] thresh_path;
		byte[] parent_path;
		byte[] domain;
		ArrayList<SubsaddleRecord> subsaddles = new ArrayList<>();
		
		public Record() {}
		
		public Record(PromFact pf, PrepDB ctx) {
			p = new PointRecord(pf.p, Point.compareElev(pf.p, pf.saddle.s) > 0 ? TYPE_SUMMIT : TYPE_SINK, ctx);
			saddle = new PointRecord(pf.saddle.s, TYPE_SADDLE, ctx);
			promRank = pf.promRank;
			minBound = (pf.thresh == null);
			parent_geocode = pf.parent != null ? geocode(pf.parent) : null;
			pthresh_geocode = pf.pthresh != null ? geocode(pf.pthresh) : null;
			thresh_path = pf.threshPath != null ? ctx.wkb.write(makePath(ctx.gf, pf.threshPath, pf.threshTrim)) : new byte[0];
			parent_path = pf.parentPath != null ? ctx.wkb.write(makePath(ctx.gf, pf.parentPath)) : new byte[0];
			domain = pf.domainBoundary != null ? ctx.wkb.write(makeDomain(ctx.gf, pf.domainBoundary)) : new byte[0];
			subsaddles = SubsaddleRecord.fromPromFact(pf);
		}
		
		public void process(PreparedStatement ps) throws SQLException {
			ps.setLong(1, p.geocode);
			ps.setLong(2, saddle.geocode);
			ps.setInt(3, (int)(1000. * Math.abs(p.p.elev - saddle.p.elev)));
			ps.setInt(4, promRank);
			ps.setInt(5, minBound ? 1 : 0);
			ps.setObject(6, parent_geocode);
			ps.setObject(7, pthresh_geocode);
			ps.setBytes(8, nullIfEmpty(thresh_path));
			ps.setBytes(9, nullIfEmpty(parent_path));
			ps.setBytes(10, nullIfEmpty(domain));
			ps.addBatch();
		}
		
		static byte[] nullIfEmpty(byte[] arr) {
			return arr.length == 0 ? null : arr;
		}
		
		public void process(PreparedStatement stInsPt, PreparedStatement stInsProm, PreparedStatement stInsSS) throws SQLException {
			p.process(stInsPt);
			saddle.process(stInsPt);
			process(stInsProm);
			for (SubsaddleRecord ss : subsaddles) {
				ss.process(stInsSS);
			}
		}
		
		
		static long geocode(Point p) {
			return PointIndex.iGeocode(p.ix);
		}
		
		static LineString makePath(GeometryFactory gf, List<Long> ixs) {
			return makePath(gf, ixs, 0);
		}
		static LineString makePath(GeometryFactory gf, List<Long> ixs, double trim) {
			List<Coordinate> coords = new ArrayList<>();
			for (long ix : ixs) {
				if (ix == PointIndex.NULL) {
					continue;
				}
				double ll[] = PointIndex.toLatLon(ix, true);
				coords.add(new Coordinate(ll[1], ll[0]));
			}
			// FIXME
			if (coords.size() == 1) {
				coords.add(coords.get(0));
			}
			
			if (trim > 0) {
				Coordinate a = coords.get(coords.size() - 1 - (int)Math.ceil(trim));
				Coordinate b = coords.get(coords.size() - 1 - (int)Math.floor(trim));
				double frac = trim % 1.;
				Coordinate last = new Coordinate(
					(1-frac)*b.x + frac*a.x,
					(1-frac)*b.y + frac*a.y
				);
				coords = new ArrayList<>(coords.subList(0, coords.size() - (int)Math.ceil(trim)));
				coords.add(last);
			}
			
			return gf.createLineString(new CoordinateArraySequence(coords.toArray(new Coordinate[coords.size()])));
		}
		
		static MultiLineString makeDomain(GeometryFactory gf, List<List<Long>> segs) {
			List<LineString> paths = new ArrayList<>();
			for (List<Long> seg : segs) {
				try {
					paths.add(makePath(gf, seg));
				} catch (IllegalArgumentException e) {
					System.out.println("invalid geometry");
				}
			}
			return gf.createMultiLineString(paths.toArray(new LineString[paths.size()]));
		}
		
	}
	

	static class SpatialiteSink implements FileIO.Sink<Record> {
		final int BATCH_SIZE = 5000;
		WritableByteChannel finalDst;

		File dbpath;
		Connection conn;
		int curBatchSize = 0;
		int totalWritten = 0;
		PreparedStatement stInsPt;
        PreparedStatement stInsProm;
        PreparedStatement stInsSS;

        synchronized public static void installdb() {
        	WorkerUtils.initializeGDAL();
        	WorkerUtils.initializeSpatialite();
        }
        
		public void open(WritableByteChannel channel) throws IOException {
			// save channel till end
			finalDst = channel;
						
			// install db
			installdb();			
			try {
				// set up db
	            SQLiteConfig config = new SQLiteConfig();
	            config.enableLoadExtension(true);
	            dbpath = File.createTempFile("promout", "sqlite");
		        conn = DriverManager.getConnection("jdbc:sqlite:" + dbpath.getPath(), config.toProperties());
	            Statement stmt = conn.createStatement();
	            stmt.execute("SELECT load_extension('mod_spatialite')");
	            stmt.execute("SELECT InitSpatialMetadata(1)");

	            // DON'T specify any index-based constraints (primary key, foreign key, etc.) until after data
	            // is populated. much faster to add them at the end.
	            // DON'T override sqlite's internal rowid primary key*, as any of our alternative keys are non-
	            // sequential, which makes bulk insert much, much slower. the extra space used is well worth it.
	            // *either with primary key of literal type "integer", or "without rowid"
	            
	            stmt.execute(
	                "create table points (" +
	            	"  geocode int8 not null," +
	                "  type int not null," +
	            	"  elev_mm int not null," +
	                "  isodist_cm int not null," +
	            	"  elev_rank int not null" +
	            	");"
	            );
	            stmt.execute("SELECT AddGeometryColumn('points', 'loc', 4326, 'POINT', 'XY');");
	            stmt.execute(
	                "create table prom (" +
	                "  point int8 not null," +
	                "  saddle int8 not null," +
	                "  prom_mm int not null," +
	                "  prom_rank int not null," +
	                "  min_bound int not null," +
	                "  prom_parent int8," +
	                "  line_parent int8" +
	                ");"
	            );
	            stmt.execute("SELECT AddGeometryColumn('prom', 'thresh_path', 4326, 'LINESTRING', 'XY');");
	            stmt.execute("SELECT AddGeometryColumn('prom', 'parent_path', 4326, 'LINESTRING', 'XY');");
	            stmt.execute("SELECT AddGeometryColumn('prom', 'domain', 4326, 'MULTILINESTRING', 'XY');");
	            stmt.execute(
	                    "create table subsaddles (" +
	                    "  point int8 not null," +
	                    "  saddle int8 not null," +
	                    "  is_elev int not null," +
	                    "  is_prom int not null" +
	                    ");"
	                );
	            
	            conn.setAutoCommit(false);
	            String insPt = "insert into points values (?,?,?,?,?,GeomFromWKB(?, 4326))";
	            stInsPt = conn.prepareStatement(insPt);
	            String insProm = "insert into prom values (?,?,?,?,?,?,?,CompressGeometry(GeomFromWKB(?, 4326)),CompressGeometry(GeomFromWKB(?, 4326)),CompressGeometry(GeomFromWKB(?, 4326)))";
	            stInsProm = conn.prepareStatement(insProm);
	            String insSS = "insert into subsaddles values (?,?,?,?)";
	            stInsSS = conn.prepareStatement(insSS);
	            
	    		/* TODO known_point table
	    		 * geo + latlon
	    		 * names
	    		 * highpoints of
	    		 * ref ids (gnis, peakbagger, etc.)
	    		 */
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void addConstraintsAndIndexes() throws SQLException {
			Statement stmt = conn.createStatement();

			// since the db is read-only, these aren't really worth enforcing
			// note: sqlite doesn't support adding constraints: primary key/unique can be replicated via
			// CREATE UNIQUE INDEX; foreign key can't be done after the fact, but isn't enforced by default
			// anyway
			/*
            stmt.execute("ALTER TABLE points ADD PRIMARY KEY (geocode)");
            stmt.execute("ALTER TABLE prom ADD PRIMARY KEY (point)");
            stmt.execute("ALTER TABLE subsaddles ADD PRIMARY KEY (point, saddle)");
            stmt.execute("ALTER TABLE points ADD CONSTRAINT UNIQUE (elev_rank)");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT UNIQUE (saddle)");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT UNIQUE (prom_rank)");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT FOREIGN KEY (point) REFERENCES points");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT FOREIGN KEY (saddle) REFERENCES points");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT FOREIGN KEY (prom_parent) REFERENCES points");
            stmt.execute("ALTER TABLE prom ADD CONSTRAINT FOREIGN KEY (line_parent) REFERENCES points");
            stmt.execute("ALTER TABLE subsaddles ADD CONSTRAINT FOREIGN KEY (point) REFERENCES prom");
            stmt.execute("ALTER TABLE subsaddles ADD CONSTRAINT FOREIGN KEY (saddle) REFERENCES prom(saddle)");
            */
			
			createIndex(stmt, "points", "geocode");
			createIndex(stmt, "points", "elev_mm");
			createIndex(stmt, "prom", "point");
			createIndex(stmt, "prom", "saddle");
			createIndex(stmt, "prom", "prom_mm");
			createIndex(stmt, "prom", "prom_parent");
			createIndex(stmt, "prom", "line_parent");
			createIndex(stmt, "subsaddles", "point");
			createIndex(stmt, "subsaddles", "saddle");
			// might want separate summit/sink indexes (where clause on points.type)
			
			// TODO spatial indexes
			
			long start = System.currentTimeMillis();
			conn.commit();
			long end = System.currentTimeMillis();
			LOG.info("committing indexes " + (end - start));

		}
		
		public void createIndex(Statement stmt, String table, String col) throws SQLException {
			long start = System.currentTimeMillis();
			stmt.execute(String.format("CREATE INDEX %s_%s ON %s(%s)", table, col, table, col));
			long end = System.currentTimeMillis();
			LOG.info("created index " + table + " " + col + " " + (end - start));
		}
		
		public void write(Record rec) throws IOException {
			try {
				rec.process(stInsPt, stInsProm, stInsSS);
	
				// potentially flush batch
				curBatchSize += 1;
				if (curBatchSize % BATCH_SIZE == 0) {
					flushBatch();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void flush() throws IOException {
			// flush remaining and finalize db
			try {
				if (curBatchSize > 0) {
					flushBatch();
				}
				
				long start = System.currentTimeMillis();
				conn.commit();
				long end = System.currentTimeMillis();
				LOG.info("commit db time " + (end - start));
				
				addConstraintsAndIndexes();
				
				conn.close();
			} catch (SQLException e) {
				throw new IOException(e);
			}
			
			copyFile(dbpath, finalDst);
		}
		
		static void copyFile(File input, WritableByteChannel dst) throws IOException {
			FileInputStream fis = new FileInputStream(input);
			FileChannel src = fis.getChannel();
			LOG.info("total file size " + src.size());
			long position = 0;
			while (position < src.size()) {
				position += src.transferTo(position, src.size() - position, dst);
				LOG.info("transferred " + position);
			}
			fis.close();
		}
		
		void flushBatch() throws SQLException {
			long start = System.currentTimeMillis();
			stInsPt.executeBatch();
			stInsProm.executeBatch();
			stInsSS.executeBatch();
			// don't commit here; only commit at end
			long end = System.currentTimeMillis();
			
			totalWritten += curBatchSize;
			curBatchSize = 0;
			LOG.info(totalWritten + " written to db; batch db time " + (end - start));
		}
	}
	
	
	
	
	
	static class MSTDebugSink implements FileIO.Sink<KV<Long, Long>> {
		final int BATCH_SIZE = 5000;
		WritableByteChannel finalDst;

		File dbpath;
		Connection conn;
		int curBatchSize = 0;
		PreparedStatement stInsEdge;
		
		GeometryFactory gf = new GeometryFactory();

        synchronized public static void installdb() {
        	WorkerUtils.initializeGDAL();
        	WorkerUtils.initializeSpatialite();
        }
        
		public void open(WritableByteChannel channel) throws IOException {
			// save channel till end
			finalDst = channel;
						
			// install db
			installdb();			
			try {
				// set up db
	            SQLiteConfig config = new SQLiteConfig();
	            config.enableLoadExtension(true);
	            dbpath = File.createTempFile("edgedump", "sqlite");
		        conn = DriverManager.getConnection("jdbc:sqlite:" + dbpath.getPath(), config.toProperties());
	            Statement stmt = conn.createStatement();
	            stmt.execute("SELECT load_extension('mod_spatialite')");
	            stmt.execute("SELECT InitSpatialMetadata(1)");

	            stmt.execute(
	                "create table edge (id int);"
	            );
	            stmt.execute("SELECT AddGeometryColumn('edge', 'e', 4326, 'LINESTRING', 'XY');");
	            
	            conn.setAutoCommit(false);
	            String insEdge = "insert into edge values (0,GeomFromText(?, 4326))";
	            stInsEdge = conn.prepareStatement(insEdge);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void write(KV<Long, Long> edge) throws IOException {
			if (edge.getValue() == PointIndex.NULL) {
				return;
			}
			try {
				stInsEdge.setString(1, Record.makePath(gf, Lists.newArrayList(edge.getKey(), edge.getValue())).toText());
				stInsEdge.addBatch();
	
				// potentially flush batch
				curBatchSize += 1;
				if (curBatchSize % BATCH_SIZE == 0) {
					flushBatch();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void flush() throws IOException {
			// flush remaining and finalize db
			try {
				if (curBatchSize > 0) {
					flushBatch();
				}
				conn.close();
			} catch (SQLException e) {
				throw new IOException(e);
			}
			
			// copy db to cloud
			FileInputStream fis = new FileInputStream(dbpath);
			fis.getChannel().transferTo(0, Long.MAX_VALUE, finalDst);
			fis.close();
		}
		
		void flushBatch() throws SQLException {
			stInsEdge.executeBatch();
			conn.commit();
			curBatchSize = 0;
		}
	}
	static class PointsDebugSink implements FileIO.Sink<Long> {
		final int BATCH_SIZE = 5000;
		WritableByteChannel finalDst;

		File dbpath;
		Connection conn;
		int curBatchSize = 0;
		PreparedStatement stInsEdge;

		GeometryFactory gf = new GeometryFactory();
		
        synchronized public static void installdb() {
        	WorkerUtils.initializeGDAL();
        	WorkerUtils.initializeSpatialite();
        }
        
		public void open(WritableByteChannel channel) throws IOException {
			// save channel till end
			finalDst = channel;
						
			// install db
			installdb();			
			try {
				// set up db
	            SQLiteConfig config = new SQLiteConfig();
	            config.enableLoadExtension(true);
	            dbpath = File.createTempFile("ptdump", "sqlite");
		        conn = DriverManager.getConnection("jdbc:sqlite:" + dbpath.getPath(), config.toProperties());
	            Statement stmt = conn.createStatement();
	            stmt.execute("SELECT load_extension('mod_spatialite')");
	            stmt.execute("SELECT InitSpatialMetadata(1)");

	            stmt.execute(
	                "create table pts (id int);"
	            );
	            stmt.execute("SELECT AddGeometryColumn('pts', 'p', 4326, 'POINT', 'XY');");
	            
	            conn.setAutoCommit(false);
	            String insEdge = "insert into pts values (0,GeomFromText(?, 4326))";
	            stInsEdge = conn.prepareStatement(insEdge);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void write(Long ix) throws IOException {
			if (ix == PointIndex.NULL) {
				return;
			}
			try {
				double coords[] = PointIndex.toLatLon(ix, true);
				com.vividsolutions.jts.geom.Point pt = gf.createPoint(new Coordinate(coords[1], coords[0]));

				stInsEdge.setString(1, pt.toText());
				stInsEdge.addBatch();
	
				// potentially flush batch
				curBatchSize += 1;
				if (curBatchSize % BATCH_SIZE == 0) {
					flushBatch();
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}

		public void flush() throws IOException {
			// flush remaining and finalize db
			try {
				if (curBatchSize > 0) {
					flushBatch();
				}
				conn.close();
			} catch (SQLException e) {
				throw new IOException(e);
			}
			
			// copy db to cloud
			FileInputStream fis = new FileInputStream(dbpath);
			fis.getChannel().transferTo(0, Long.MAX_VALUE, finalDst);
			fis.close();
		}
		
		void flushBatch() throws SQLException {
			stInsEdge.executeBatch();
			conn.commit();
			curBatchSize = 0;
		}
	}
	
	  public static void main(String[] args) {
		  PipelineOptionsFactory.register(MyOptions.class);
		  MyOptions options = PipelineOptionsFactory.fromArgs(args)
				  									.withValidation()
		                                            .as(MyOptions.class);
		  Pipeline p = Pipeline.create(options);
		  String outputRoot = options.getOutputLocation();

		  p.apply("LoadPromFacts", AvroIO.read(Record.class).from(outputRoot + "dbpreprocess*"))
		  .apply("WriteSpatialite", FileIO.<AvroToDb.Record>write()
				  .via(new SpatialiteSink())
				  .to(outputRoot).withNaming(new FileNaming() {
					  @Override
					  public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
							  Compression compression) {
						  return "promout.spatialite";
					  }
				  }).withNumShards(1));
		  
		  p.run();
	  }

}
