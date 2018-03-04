package com.mrgris.prominence;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

import com.mrgris.prominence.Prominence.PromFact;

// support spatialite

public class AvroToDb {
	
	public static void main(String[] args) {
		String pipelineOutput = args[0];
		String dbname = args[1];
		
		try {

	        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbname);

            Statement stmt = conn.createStatement();
            stmt.execute("create table points (ix int64 primary key);");

            conn.setAutoCommit(false);
            int batchSize = 10000;            
            String sqlInsert = "insert into points values (?)";
            PreparedStatement ps = conn.prepareStatement(sqlInsert);

			DatumReader<PromFact> userDatumReader = new ReflectDatumReader<PromFact>(PromFact.class);
			DataFileReader<PromFact> dataFileReader = new DataFileReader<PromFact>(new File(pipelineOutput), userDatumReader);
			PromFact pf = null;
			
			int i = 0;
			while (dataFileReader.hasNext()) {
				pf = dataFileReader.next(pf);
				
	            ps.setLong(1, pf.p.ix);
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
