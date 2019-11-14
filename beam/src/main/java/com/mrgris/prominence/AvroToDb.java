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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import com.google.common.collect.Sets;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.util.GeoCode;
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

	// prepare data for insertion into database. all this work is parallelizable (unlike db interaction),
	// so do it upfront
	@DefaultCoder(AvroCoder.class)
	public static class Record {
		public static class PointRecord {
			long geocode;
			int type;
			byte[] geom;
			Point p;
			
			public PointRecord() {}
			
			public PointRecord(Point p, int type) {
				this.p = p;
				this.geocode = geocode(p);
				this.type = type;
				
				double coords[] = PointIndex.toLatLon(p.ix);
				com.vividsolutions.jts.geom.Point pt = gf.createPoint(new Coordinate(coords[1], coords[0]));
				this.geom = wkb.write(pt);
			}
			
			public void process(PreparedStatement ps) throws SQLException {
		        ps.setLong(1, geocode);
		        ps.setInt(2, type);
		        ps.setInt(3, (int)(p.elev * 1000.));
		        ps.setInt(4, p.isodist == 0 ? 0 : p.isodist > 0 ? p.isodist - Integer.MAX_VALUE : p.isodist - Integer.MIN_VALUE);
		        ps.setBytes(5, geom);
		        ps.addBatch();
			}
		}

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
		
		PromFact pf;
		PointRecord p;
		PointRecord saddle;
		@Nullable
		Long parent_geocode;
		@Nullable
		Long pthresh_geocode;
		@Nullable
		byte[] thresh_path;
		@Nullable
		byte[] parent_path;
		@Nullable
		byte[] domain;
		ArrayList<SubsaddleRecord> subsaddles = new ArrayList<>();
		
		public Record() {}
		
		public Record(PromFact pf) {
			this.pf = pf;
			p = new PointRecord(pf.p, Point.compareElev(pf.p, pf.saddle.s) > 0 ? TYPE_SUMMIT : TYPE_SINK);
			saddle = new PointRecord(pf.saddle.s, TYPE_SADDLE);
			
			parent_geocode = pf.parent != null ? geocode(pf.parent) : null;
			pthresh_geocode = pf.pthresh != null ? geocode(pf.pthresh) : null;
			thresh_path = pf.threshPath != null ? wkb.write(makePath(pf.threshPath, pf.threshTrim)) : null;
			parent_path = pf.parentPath != null ? wkb.write(makePath(pf.parentPath)) : null;
			domain = pf.domainBoundary != null ? wkb.write(makeDomain(pf.domainBoundary)) : null;
			subsaddles = SubsaddleRecord.fromPromFact(pf);
		}
		
		public void process(PreparedStatement ps) throws SQLException {
			ps.setLong(1, p.geocode);
			ps.setLong(2, saddle.geocode);
			ps.setInt(3, (int)(1000. * Math.abs(pf.p.elev - pf.saddle.s.elev)));
			ps.setInt(4, pf.promRank);
			ps.setInt(5, pf.thresh == null ? 1 : 0);
			ps.setObject(6, parent_geocode);
			ps.setObject(7, pthresh_geocode);
			ps.setBytes(8, thresh_path);
			ps.setBytes(9, parent_path);
			ps.setBytes(10, domain);
			ps.addBatch();
		}
		
		public void process(PreparedStatement stInsPt, PreparedStatement stInsProm, PreparedStatement stInsSS) throws SQLException {
			p.process(stInsPt);
			saddle.process(stInsPt);
			process(stInsProm);
			for (SubsaddleRecord ss : subsaddles) {
				ss.process(stInsSS);
			}
		}
	}
	
	// thread safety?
	static GeometryFactory gf = new GeometryFactory();
	static WKBWriter wkb = new WKBWriter();

	static long geocode(Point p) {
		return PointIndex.iGeocode(p.ix);
	}
	
	static LineString makePath(List<Long> ixs) {
		return makePath(ixs, 0);
	}
	static LineString makePath(List<Long> ixs, double trim) {
		List<Coordinate> coords = new ArrayList<>();
		for (long ix : ixs) {
			if (ix == PointIndex.NULL) {
				continue;
			}
			double ll[] = PointIndex.toLatLon(ix);
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
	
	static MultiLineString makeDomain(List<List<Long>> segs) {
		List<LineString> paths = new ArrayList<>();
		for (List<Long> seg : segs) {
			try {
				paths.add(makePath(seg));
			} catch (IllegalArgumentException e) {
				System.out.println("invalid geometry");
			}
		}
		return gf.createMultiLineString(paths.toArray(new LineString[paths.size()]));
	}
	
	static class SpatialiteSink implements FileIO.Sink<PromFact> {
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

	            stmt.execute(
	                "create table points (" +
	            	"  geocode int8 primary key," +
	                "  type int not null," +
	            	"  elev_mm int not null," +
	                "  isodist_cm int not null" +
	            	");"
	            );
	            stmt.execute("SELECT AddGeometryColumn('points', 'loc', 4326, 'POINT', 'XY');");
	            stmt.execute(
	                "create table prom (" +
	                "  point int8 primary key references points," +
	                "  saddle int8 references points not null," +
	                "  prom_mm int not null," +
	                "  prom_rank int not null," +
	                "  min_bound int not null," +
	                "  prom_parent int8 references points," +
	                "  line_parent int8 references points" +
	                ");"
	            );
	            stmt.execute("SELECT AddGeometryColumn('prom', 'thresh_path', 4326, 'LINESTRING', 'XY');");
	            stmt.execute("SELECT AddGeometryColumn('prom', 'parent_path', 4326, 'LINESTRING', 'XY');");
	            stmt.execute("SELECT AddGeometryColumn('prom', 'domain', 4326, 'MULTILINESTRING', 'XY');");
	            stmt.execute(
	                    "create table subsaddles (" +
	                    "  point int8 references prom," +
	                    "  saddle int8 references prom(saddle) not null," +
	                    "  is_elev int not null," +
	                    "  is_prom int not null" + //," +
	                    //"  primary key (point, saddle)" +
	                    ");"// without rowid;"
	                );
	            // TODO: multisaddles can make subsaddles not unique?
	            // could they have different elev/prom flags?
	            
	            // disable primary key indexes till end?
	            conn.setAutoCommit(false);
	            String insPt = "replace into points values (?,?,?,?,GeomFromWKB(?, 4326))"; // why replace? should be unique?
	            stInsPt = conn.prepareStatement(insPt);
	            String insProm = "insert into prom values (?,?,?,?,?,?,?,GeomFromWKB(?, 4326),GeomFromWKB(?, 4326),GeomFromWKB(?, 4326))";
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

		public void write(PromFact pf) throws IOException {
			try {
				Record rec = new Record(pf);
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
				conn.commit();
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
			stInsPt.executeBatch();
			stInsProm.executeBatch();
			stInsSS.executeBatch();
			// don't commit here; only commit at end
			totalWritten += curBatchSize;
			curBatchSize = 0;
			LOG.info(totalWritten + " written to db");
		}
	}
	
	
	
	
	
	static class MSTDebugSink implements FileIO.Sink<KV<Long, Long>> {
		final int BATCH_SIZE = 5000;
		WritableByteChannel finalDst;

		File dbpath;
		Connection conn;
		int curBatchSize = 0;
		PreparedStatement stInsEdge;

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
				stInsEdge.setString(1, makePath(Lists.newArrayList(edge.getKey(), edge.getValue())).toText());
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
				double coords[] = PointIndex.toLatLon(ix);
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

}
