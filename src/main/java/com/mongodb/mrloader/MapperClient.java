package com.mongodb.mrloader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import static com.mongodb.mrloader.LoaderConfiguration.*;

public class MapperClient {

	private static final String DEFAULT_JOB_NAME = "MongoDBLoader-" + System.currentTimeMillis();
	private static final String JOB_FLOW_PATH = "/mnt/var/lib/info/job-flow.json";
	private static final String JOB_FLOW_ID_KEY = "jobFlowId";

	public static void main(String[] args) throws Exception {
		
		
		// Convert the args to a job configuration
		Configuration jobConfig = LoaderConfiguration.parseCommandLine(args);
		
		// If discovery enabled find the mongos list from the supplied URI
		ConnectionManager.configureMongoConnection(jobConfig);	
		
		// Get the unique ID for this job
		String jobId = getJobId();
		
		// if 'mapred_job_id' in os.environ:
		//     jobID = os.environ['mapred_job_id']
		    		
		// Create the job and configure jar from this class
		Job job = new Job(jobConfig, jobConfig.get(CFG_JOB_NAME, DEFAULT_JOB_NAME));
		job.setJarByClass(MapperClient.class);

		// The output is int:int (collating latencies)
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Use batch mapper and collating reducer
		job.setMapperClass(BatchMapper.class);
		job.setReducerClass(SummingReducer.class);

		// Read line batches from input files and write to txt output
		job.setInputFormatClass(BatchTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Setup the input and output S3 locations
		FileInputFormat.addInputPath(job, new Path(jobConfig.get(CFG_INPUT_URI)));
		
		String outputUri = jobConfig.get(CFG_OUTPUT_URI);
		if(outputUri.endsWith("/") == false){ outputUri += "/"; };
		if(jobId != null){ outputUri += jobId + "/"; }
		System.out.println("Sending output to : " + outputUri);
		FileOutputFormat.setOutputPath(job, new Path(outputUri));

		// Execute job and wait
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

	
	private static String getJobId() {
		try{
			// Read the job_flow json file from the node
        	String content = new Scanner(new File(JOB_FLOW_PATH)).useDelimiter("\\Z").next();
        	DBObject json = (DBObject) JSON.parse(content);
        	return (String) json.get(JOB_FLOW_ID_KEY);
        	
		} catch(Exception e){}

		return null;
	}
}			
	
	
