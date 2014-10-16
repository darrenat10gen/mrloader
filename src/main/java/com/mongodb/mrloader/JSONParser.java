package com.mongodb.mrloader;

import org.apache.hadoop.mapreduce.JobContext;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class JSONParser implements BSONGenerator {

	@Override
	public DBObject parse(String entry) {
		
		return (DBObject)JSON.parse(entry);
	}

	@Override
	public void setup(JobContext context) {
		// configure the parser, nothing to do		
	}
}
