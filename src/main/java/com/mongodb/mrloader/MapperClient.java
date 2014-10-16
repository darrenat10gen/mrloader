package com.mongodb.mrloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static com.mongodb.mrloader.LoaderConfiguration.*;

public class MapperClient {

	private static final String DEFAULT_JOB_NAME = "MongoDBLoader-" + System.currentTimeMillis();

	public static void main(String[] args) throws Exception {
		
		// Convert the args to a job configuration
		Configuration jobConfig = LoaderConfiguration.parseCommandLine(args);
		
		// If discovery enabled find the mongos list from the supplied URI
		ConnectionManager.configureMongoConnection(jobConfig);	
		
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
		FileOutputFormat.setOutputPath(job, new Path(jobConfig.get(CFG_OUTPUT_URI)));

		// Execute job and wait
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}