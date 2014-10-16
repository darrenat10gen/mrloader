package com.mongodb.mrloader;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import static com.mongodb.mrloader.LoaderConfiguration.*;

public class ConnectionManager {
	
	public static final String CFG_MONGOS_LIST_DELIMITER = ",";
	private static final String CONFIG_DB_NAME = "config";
	private static final String MONGOS_COLL_NAME = "mongos";
	
	private static final ObjectId oid = new ObjectId();
	private static final Random random = new Random(oid.hashCode());

	private static MongoClient connection = null;
	
	public static synchronized MongoClient getConnection(JobContext context){
		
		if(connection == null){
			
			// Pull the address list from the context
			Configuration config = context.getConfiguration();
			String mongosListCfg = config.get(CFG_MONGOS_LIST);
			String rawConnString = config.get(CFG_MONGO_URI);
			String[] mongosList = mongosListCfg.split(CFG_MONGOS_LIST_DELIMITER);
			if(mongosList.length > 0){

				// Make a selection from the available mongos'
				String mongosHost = mongosList[random.nextInt(mongosList.length)];	
				
				// Find the bounds of the host section
				int hostBegin = rawConnString.indexOf("//") + 2;
				int hostEnd = rawConnString.indexOf('/', hostBegin);
				int credsIndex = rawConnString.indexOf('@', hostBegin);
				if (credsIndex >= 0 && credsIndex < hostEnd){
					hostBegin = credsIndex + 1;
				}
				
				// Replace the host section with the selected host string
				String targetConnString = rawConnString.replace(
						rawConnString.subSequence(hostBegin, hostEnd), mongosHost);
				
				try {
					System.out.println("Connecting [" + oid.toString() + "] : " + targetConnString);
					MongoClientURI uri = new MongoClientURI(targetConnString);
					connection = new MongoClient(uri);
				} catch (Exception e) {
					System.err.println("Error connecting to Mongos : " + e.getMessage());
				}
			}		
		}
		
		return connection;
	}

	public static void configureMongoConnection(Configuration config) {
		
		DBCursor mongos = null;
		try{
			
			// Get the configured uri and hosts list
			String rawConnString = config.get(CFG_MONGO_URI);
			MongoClientURI rawUri = new MongoClientURI(rawConnString);
			
			boolean noDiscover = config.getBoolean(CFG_NO_DISCOVERY, false);
			List<String> hostList = null;
			
			if(noDiscover){
				
				// with "no discover" use only the hosts from the URI
				hostList = rawUri.getHosts();
			} else {
				
				
				// Connect using the supplied URI to the config DB
				MongoClient client = new MongoClient(rawUri);
				DBCollection mongosColl = client.getDB(CONFIG_DB_NAME).
						getCollection(MONGOS_COLL_NAME);
				
				// Get the mongos collection and iterate the ID's
				hostList = new ArrayList<String>();
				mongos = mongosColl.find(new BasicDBObject(), new BasicDBObject("_id", true));
				while(mongos.hasNext()){
					String host = (String) mongos.next().get("_id");
					System.out.println("Adding discovered mongos : " + host);
					hostList.add(host);
				}
			}
			
			if(hostList.size() > 0){
				// Take the mongos list and stringify into job config
				StringBuilder mongosList = new StringBuilder();
				mongosList.append(hostList.get(0));
				for(int i = 1; i < hostList.size(); i++){
					mongosList.append(CFG_MONGOS_LIST_DELIMITER);
					mongosList.append(hostList.get(i));
				}
				
				// Write the mongos list back to the config
				config.set(CFG_MONGOS_LIST, mongosList.toString());
			}

			// Set the database and collection names
			config.set(CFG_DATABASE, rawUri.getDatabase());
			config.set(CFG_COLLECTION, rawUri.getCollection());

		} 
		catch(Exception ex){
			
			// If there is an issue discovering the mongos list, fail the job
			System.err.println("Error discovering Mongos list : " + ex.getMessage());
			System.exit(1);
		} 
		finally {
			if(mongos != null){
				mongos.close();
			}
		}
	}
}
