package com.mrgris.prominence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.runners.direct.repackaged.runners.core.java.repackaged.com.google.common.collect.Sets;
import org.sqlite.SQLiteConfig;

import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.util.GeoCode;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

public class AvroToDb {
	
	static final int TYPE_SUMMIT = 1;
	static final int TYPE_SADDLE = 0;
	static final int TYPE_SINK = -1;

	static GeometryFactory gf = new GeometryFactory();

	static long geocode(Point p) {
		return PointIndex.iGeocode(p.ix);
	}
	
	public static void addPoint(PreparedStatement ps, Point p, int type) throws SQLException {
		long ix = p.ix;
		double coords[] = PointIndex.toLatLon(ix);
		long geo = GeoCode.fromCoord(coords[0], coords[1]);
		com.vividsolutions.jts.geom.Point pt = gf.createPoint(new Coordinate(coords[1], coords[0]));
		
        ps.setLong(1, geo);
        ps.setInt(2, type);
        ps.setInt(3, (int)(p.elev * 1000.));
        ps.setInt(4, p.isodist == 0 ? 0 : p.isodist > 0 ? p.isodist - Integer.MAX_VALUE : p.isodist - Integer.MIN_VALUE);
        ps.setString(5, pt.toText());
        ps.addBatch();
	}
	
	static LineString makePath(List<Long> ixs) {
		List<Coordinate> coords = new ArrayList<>();
		for (long ix : ixs) {
			if (ix == PointIndex.NULL) {
				continue;
			}
			double ll[] = PointIndex.toLatLon(ix);
			coords.add(new Coordinate(ll[1], ll[0]));
		}
		return gf.createLineString(new CoordinateArraySequence(coords.toArray(new Coordinate[coords.size()])));
	}
	
	static MultiLineString makeDomain(List<List<Long>> segs) {
		LineString[] paths = new LineString[segs.size()];
		for (int i = 0; i < segs.size(); i++) {
			paths[i] = makePath(segs.get(i));
		}
		return gf.createMultiLineString(paths);
	}
	
	public static void main(String[] args) {
		String pipelineOutput = args[0];
		String dbname = args[1];
		
		try {

			/* TODO known_point table
			 * geo + latlon
			 * names
			 * highpoints of
			 * ref ids (gnis, peakbagger, etc.)
			 */
			
            SQLiteConfig config = new SQLiteConfig();
            config.enableLoadExtension(true);
	        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbname, config.toProperties());

            Statement stmt = conn.createStatement();
            stmt.execute("SELECT load_extension('mod_spatialite')");
            stmt.execute("SELECT InitSpatialMetadata(1)");
                        
            stmt.execute(
                "create table points (" +
            	"  geocode int64 primary key," +
                "  type int not null," +
            	"  elev_mm int not null," +
                "  isodist_cm int not null" +
            	");"
            );
            stmt.execute("SELECT AddGeometryColumn('points', 'loc', 4326, 'POINT', 'XY');");
            stmt.execute(
                "create table prom (" +
                "  point int64 primary key references points," +
                "  saddle int64 references points not null," +
                "  prom_mm int not null," +
                "  prom_rank int not null," +
                "  min_bound int not null," +
                "  prom_parent int64 references points," +
                "  line_parent int64 references points" +
                ");"
            );
            stmt.execute("SELECT AddGeometryColumn('prom', 'thresh_path', 4326, 'LINESTRING', 'XY');");
            stmt.execute("SELECT AddGeometryColumn('prom', 'parent_path', 4326, 'LINESTRING', 'XY');");
            stmt.execute("SELECT AddGeometryColumn('prom', 'domain', 4326, 'MULTILINESTRING', 'XY');");
            stmt.execute(
                    "create table subsaddles (" +
                    "  point int64 references prom," +
                    "  saddle int64 references prom(saddle) not null," +
                    "  is_elev int not null," +
                    "  is_prom int not null" + //," +
                    //"  primary key (point, saddle)" +
                    ");"// without rowid;"
                );
            // TODO: multisaddles can make subsaddles not unique?
            // could they have different elev/prom flags?
            
            conn.setAutoCommit(false);
            int batchSize = 10000;            
            String insPt = "replace into points values (?,?,?,?,GeomFromText(?, 4326))";
            PreparedStatement stInsPt = conn.prepareStatement(insPt);
            String insProm = "insert into prom values (?,?,?,?,?,?,?,GeomFromText(?, 4326),GeomFromText(?, 4326),GeomFromText(?, 4326))";
            PreparedStatement stInsProm = conn.prepareStatement(insProm);
            String insSS = "insert into subsaddles values (?,?,?,?)";
            PreparedStatement stInsSS = conn.prepareStatement(insSS);
            
			DatumReader<PromFact> userDatumReader = new ReflectDatumReader<PromFact>(PromFact.class);
			DataFileReader<PromFact> dataFileReader = new DataFileReader<PromFact>(new File(pipelineOutput), userDatumReader);
			PromFact pf = null;
			
			int i = 0;
			while (dataFileReader.hasNext()) {
				pf = dataFileReader.next(pf);

				addPoint(stInsPt, pf.p, Point.compareElev(pf.p, pf.saddle.s) > 0 ? TYPE_SUMMIT : TYPE_SINK);
				addPoint(stInsPt, pf.saddle.s, TYPE_SADDLE);
				
				stInsProm.setLong(1, geocode(pf.p));
				stInsProm.setLong(2, geocode(pf.saddle.s));
				stInsProm.setInt(3, (int)(1000. * Math.abs(pf.p.elev - pf.saddle.s.elev)));
				stInsProm.setInt(4, pf.promRank);
				stInsProm.setInt(5, pf.thresh == null ? 1 : 0);
				stInsProm.setObject(6, pf.parent != null ? geocode(pf.parent) : null);
				stInsProm.setObject(7, pf.pthresh != null ? geocode(pf.pthresh) : null);
				stInsProm.setString(8, pf.threshPath != null ? makePath(pf.threshPath).toText() : null);
				stInsProm.setString(9, pf.parentPath != null ? makePath(pf.parentPath).toText() : null);
				stInsProm.setString(10, pf.domainBoundary != null ? makeDomain(pf.domainBoundary).toText() : null);
				stInsProm.addBatch();
				
				// TODO: key this set by geocode, not pointix
				Set<Long> ssElev = new HashSet<>();
				Set<Long> ssProm = new HashSet<>();
				for (PromFact.Saddle ss : pf.elevSubsaddles) {
					ssElev.add(ss.s.ix);
				}
				for (PromFact.Saddle ss : pf.promSubsaddles) {
					ssProm.add(ss.s.ix);
				}
				for (Long ss : Sets.union(ssElev, ssProm)) {
					stInsSS.setLong(1, geocode(pf.p));
					stInsSS.setLong(2, PointIndex.iGeocode(ss));
					stInsSS.setInt(3, ssElev.contains(ss) ? 1 : 0);
					stInsSS.setInt(4, ssProm.contains(ss) ? 1 : 0);
					stInsSS.addBatch();
				}
				
	            if (i % batchSize == 0) {
	            	stInsPt.executeBatch();
	            	stInsProm.executeBatch();
	            	stInsSS.executeBatch();
	            	System.out.println("batching " + i);
	            }
				i++;
			}

        	stInsPt.executeBatch();
        	stInsProm.executeBatch();
        	stInsSS.executeBatch();
        	System.out.println("finalizing " + i);
			
            conn.commit();
            conn.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
