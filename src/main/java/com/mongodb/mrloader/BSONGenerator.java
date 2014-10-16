package com.mongodb.mrloader;

import org.apache.hadoop.mapreduce.JobContext;

import com.mongodb.DBObject;

public interface BSONGenerator {
	
	public void setup(JobContext context);
	public DBObject parse(String entry);

}
