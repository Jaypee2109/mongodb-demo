package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.result.InsertManyResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

public class CombinedIndex {
    public static void main(String[] args) {


        String connectionString = "mongodb+srv://julianpielmaier:bruteForce@cluster0.ylk9rcf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";

        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();

        // Create a new client and connect to the server
        try (MongoClient mongoClient = MongoClients.create(settings)) {

            try {

                // Send a ping to confirm a successful connection
                MongoDatabase adminDatabase = mongoClient.getDatabase("admin");
                adminDatabase.runCommand(new Document("ping", 1));
                System.out.println("Pinged your deployment. You successfully connected to MongoDB!");
            

                // Connect to database
                MongoDatabase database = mongoClient.getDatabase("sample_training");

                try {    

                    TimeSeriesOptions tsOptions = new TimeSeriesOptions("timestamp");
                    CreateCollectionOptions collOptions = new CreateCollectionOptions().timeSeriesOptions(tsOptions);
                    
                    database.getCollection("benchmark").drop();

                    // Create time series collection
                    if(!database.listCollectionNames().into(new ArrayList<String>()).contains("benchmark")) {
                        database.createCollection("benchmark", collOptions);

                        // Create combined index
                        MongoCollection<Document> collection = database.getCollection("benchmark");
                        collection.createIndex(Indexes.ascending("timeseries", "timestamp", "property"));

                        // Create sample timeseries data
                        Random random = new Random(123);
                        long timestamp = 1704067200000L; //UTC 01.01.2024
                        ArrayList<Document> records = new ArrayList<>();
                        ArrayList<Long> timestamps = new ArrayList<>();
                        String[] properties = {"value1", "value2", "value3", "value4", "value5"};
                        int batchSize = 100_000;
                        long timestep = 3600000; //one day in ms

                        int numberOfTimeseries = 5;
                        int numberOfTimestamps = 1_000_000;
                        

                        for(int i = 0; i < numberOfTimestamps; i++){
                            timestamps.add(timestamp);
                            timestamp += timestep; 
                        }

                        Collections.shuffle(timestamps, random);
                        

                        for(int j = 0; j < numberOfTimeseries; j++){

                            for(int k = 0; k < timestamps.size(); k++){

                                // Create timeseries data point
                                records.add(
                                    new Document()
                                        .append("timeseries", String.valueOf(j))
                                        .append("timestamp", new Date(timestamps.get(k)))
                                        .append("property", properties[random.nextInt(properties.length)])
                                        );

                                

                        
                                if(k % batchSize == 0){
                            
                                    try {

                                    // Save sample documents into the collection
                                    collection.insertMany(records);
                                    records.clear();
                                    System.out.println("Inserted " + batchSize +  " Documents");
                                
                                } catch (MongoException me) {
                                    System.err.println("Unable to insert due to an error: " + me);

                                }
                            }

                            }

                            try {
                                // Save remaining documents into the collection
                                collection.insertMany(records);
                                records.clear();
                        
                                
                            } catch (MongoException me) {
                                System.err.println("Unable to insert due to an error: " + me);
    
                            }

                            System.out.println("Timeseries finished");

                        }

                        
                    } else {
                        System.out.println("Collection benchmark already exists!");
                    }

                    
                
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error while inserting records");
                }
                

            } catch (MongoException e) {
                e.printStackTrace();

            }
             

        }

    }

    
    
}
