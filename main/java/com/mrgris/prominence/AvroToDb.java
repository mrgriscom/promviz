package com.mrgris.prominence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.sqlite.SQLiteConfig;

import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.util.GeoCode;

// use jts for building wkt's

public class AvroToDb {
	
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
                        
            stmt.execute("create table points (geocode int64 primary key);");
            stmt.execute("SELECT AddGeometryColumn('points', 'loc', 4326, 'POINT', 'XY')");
            
            conn.setAutoCommit(false);
            int batchSize = 10000;            
            String sqlInsert = "insert into points values (?,GeomFromText(?, 4326))";
            PreparedStatement ps = conn.prepareStatement(sqlInsert);
            
			DatumReader<PromFact> userDatumReader = new ReflectDatumReader<PromFact>(PromFact.class);
			DataFileReader<PromFact> dataFileReader = new DataFileReader<PromFact>(new File(pipelineOutput), userDatumReader);
			PromFact pf = null;
			
			int i = 0;
			while (dataFileReader.hasNext()) {
				pf = dataFileReader.next(pf);

				long ix = pf.p.ix;
				double coords[] = PointIndex.toLatLon(ix);
				long geo = GeoCode.fromCoord(coords[0], coords[1]);
				
	            ps.setLong(1, geo);
	            ps.setString(2, String.format("POINT(%f %f)", coords[1], coords[0]));
	            ps.addBatch();
	            if (i % batchSize == 0) {
	            	int[] counts = ps.executeBatch();
	            	System.out.println("batching " + i);
	            }
				i++;
			}

            conn.commit();
            conn.close();
			
			System.out.println(i);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
