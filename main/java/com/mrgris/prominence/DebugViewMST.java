package com.mrgris.prominence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.sqlite.SQLiteConfig;

// use jts for building wkt's

public class DebugViewMST {
	
	static void addCoord(long ix, List<double[]> coords) {
		if (ix == PointIndex.NULL) {
			return;
		}
		double ll[] = PointIndex.toLatLon(ix);
		coords.add(new double[] {ll[1], ll[0]});
	}
	
	public static void main(String[] args) {
		String pipelineOutput = args[0];
		String dbname = args[1];
		
		try {

            SQLiteConfig config = new SQLiteConfig();
            config.enableLoadExtension(true);
	        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbname, config.toProperties());

            Statement stmt = conn.createStatement();
            stmt.execute("SELECT load_extension('mod_spatialite')");
            stmt.execute("SELECT InitSpatialMetadata(1)");
                        
            stmt.execute(
                "create table edges (geocode saddle);"
            );
            stmt.execute("SELECT AddGeometryColumn('edges', 'edge', 4326, 'LINESTRING', 'XY');");
            
            conn.setAutoCommit(false);
            int batchSize = 10000;            
            String insEdge = "insert into edges values (?,GeomFromText(?, 4326))";
            PreparedStatement stInsEdge = conn.prepareStatement(insEdge);
            
			DatumReader<Edge> userDatumReader = new ReflectDatumReader<Edge>(Edge.class);
			DataFileReader<Edge> dataFileReader = new DataFileReader<Edge>(new File(pipelineOutput), userDatumReader);
			Edge e = null;
			
			int i = 0;
			while (dataFileReader.hasNext()) {
				e = dataFileReader.next(e);

				List<double[]> coords = new ArrayList<double[]>();
				addCoord(e.a, coords);
				addCoord(e.saddle, coords);
				addCoord(e.b, coords);

				StringBuilder wkt = new StringBuilder();
				wkt.append("LINESTRING(");
				for (int n = 0; n < coords.size(); n++) {
					wkt.append(String.format("%f %f", coords.get(n)[0], coords.get(n)[1]));
					if (n < coords.size() - 1) {
						wkt.append(",");
					}
				}
				wkt.append(")");
				String geom = wkt.toString();
				
				stInsEdge.setLong(1, PointIndex.iGeocode(e.saddle));
				stInsEdge.setString(2, wkt.toString());
				stInsEdge.addBatch();
								
	            if (i % batchSize == 0) {
	            	stInsEdge.executeBatch();
	            	System.out.println("batching " + i);
	            }
				i++;
			}

        	stInsEdge.executeBatch();
        	System.out.println("finalizing " + i);
			
            conn.commit();
            conn.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
