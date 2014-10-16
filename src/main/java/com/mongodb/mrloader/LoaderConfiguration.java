package com.mongodb.mrloader;

import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

public class LoaderConfiguration {

	// Config params from arguments
	public static final String CFG_MONGO_URI 	= "mongos_uri";
	public static final String CFG_INPUT_URI 	= "input_uri";
	public static final String CFG_OUTPUT_URI 	= "output_uri";
	public static final String CFG_NO_DISCOVERY = "no_discovery";
	public static final String CFG_BATCH_SIZE 	= "batch_size";
	public static final String CFG_JOB_NAME 	= "job_name";
	public static final String CFG_RETRY_COUNT 	= "retry_count";
	public static final String CFG_PARSER 		= "parser_class";
	public static final String CFG_PARSE_ONLY 	= "parse_only";
		
	// Computed job configuration
	public static final String CFG_MONGOS_LIST 	= "mongos_list";
	public static final String CFG_DATABASE 	= "database";
	public static final String CFG_COLLECTION 	= "collection";
	
	public static Configuration parseCommandLine(String[] args){
		
		// Define the required options
		Option inputOption = new Option(CFG_INPUT_URI, true, "path to input files");
		inputOption.setRequired(true);
		Option outputOption = new Option(CFG_OUTPUT_URI, true, "destination path for output");
		outputOption.setRequired(true);
		Option mongosOption = new Option(CFG_MONGO_URI, true, "address of any mongos in the system");
		mongosOption.setRequired(true);

		// Create the options 
		Options options = new Options();
		options.addOption(inputOption);
		options.addOption(outputOption);
		options.addOption(mongosOption);
		options.addOption(CFG_PARSE_ONLY, false, "do not insert records after parsing");
		options.addOption(CFG_NO_DISCOVERY, false, "do not attempt to discover all mongos instances");
		options.addOption(CFG_BATCH_SIZE, true, "number of documents per batch");
		options.addOption(CFG_JOB_NAME, true, "name of the map-reduce job");
		options.addOption(CFG_RETRY_COUNT, true, "number of retry attempts for failed batches");
		options.addOption(CFG_PARSER, true, "name of class to use as BSON parser");
		
	
		try{
			// Parse the command line for known options
			CommandLineParser parser = new org.apache.commons.cli.BasicParser();
			CommandLine parsedConfig = parser.parse(options, args);
			
			// Iterate over options and replicate to jobConfig
			Configuration jobConfig = new Configuration();
			@SuppressWarnings("unchecked")
			Iterator<Option> iter = parsedConfig.iterator();
			while(iter.hasNext()){
				Option option = iter.next();
				if(option.hasArg()){
					jobConfig.set(option.getOpt(), option.getValue());
				} else {
					jobConfig.setBoolean(option.getOpt(), true);
				}
			}
			
			return jobConfig;
		}
		catch(ParseException pex){

			System.err.println("Error parsing command line : " + pex.getMessage());
			System.err.flush();
			System.exit(1);
		}
		
		return null;
	}

}
