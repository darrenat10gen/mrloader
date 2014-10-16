package com.mongodb.mrloader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mongodb.BulkWriteOperation;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import static com.mongodb.mrloader.LoaderConfiguration.*;

public class BatchMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	private static final IntWritable one = new IntWritable(1);
	private static final String LINE_DELIMITER = "\n";
	private static final int DEFAULT_RETRY_ATTEMPTS = 5;
	private static final Class<? extends BSONGenerator> DEFAULT_PARSER = JSONParser.class;
	
	private DBCollection collection = null;
	private int retryAttempts;
	private BSONGenerator parser = null;
	private boolean parseOnly = false;


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		Configuration config = context.getConfiguration();
		
		// Setup the target collection
		String dbName = config.get(CFG_DATABASE);
		String collName = config.get(CFG_COLLECTION);
		MongoClient client = ConnectionManager.getConnection(context);
		this.collection = client.getDB(dbName).getCollection(collName);
		
		// Setup other configuration
		this.retryAttempts = config.getInt(CFG_RETRY_COUNT, DEFAULT_RETRY_ATTEMPTS);
		this.parseOnly = config.getBoolean(CFG_PARSE_ONLY, false);
		
		// Create and configure the correct parser
		try{
			String parserClassName = config.get(CFG_PARSER, null);
			if(parserClassName != null){
				this.parser = (BSONGenerator) Class.forName(parserClassName).newInstance();
			} else {
				this.parser = DEFAULT_PARSER.newInstance();
			}
			this.parser.setup(context);
		} catch(Exception ex) {
			System.err.println("Error creating parser : " + ex.getMessage());
		}
		
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int remainingAttempts = this.retryAttempts;
		while(remainingAttempts-- > 0){
			try{
				long start = System.currentTimeMillis();
				String batchString = value.toString();
				String[] entries = batchString.split(LINE_DELIMITER);
				if(entries.length > 0){          	
					// Create the batch of inserts
					BulkWriteOperation currentOp = this.collection.initializeUnorderedBulkOperation();
					for(String line : entries){
						DBObject obj = this.parser.parse(line);
						currentOp.insert(obj);
					}
					
					// If not just parsing, insert
					if(parseOnly  == false){
						currentOp.execute();
					}
				}           
				long end = System.currentTimeMillis();

				// Report the latency as the output
				context.write(new IntWritable((int)(end - start)), one);
				return;
			} catch(Exception ex){
				System.err.println("Error in batch : " + ex.toString() + "... retrying...");
				System.err.flush();
			}
		}

		// Only makes it here if all the retries are exhausted
		System.err.println("Batch FAILED after " + DEFAULT_RETRY_ATTEMPTS + " attempts");
		System.err.flush();
	}
} 
