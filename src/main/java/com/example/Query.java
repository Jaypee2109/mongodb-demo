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

public class Query {
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
                        Date from = new Date(1704585600000L); //UTC 07.01.2024
                        Date to = new Date(1704672000000L); //UTC 08.01.2024
                        String property = "value3";
                        String timeseries = "0";


                        MongoCollection<Document> collection = database.getCollection("benchmark");
                        
                        String queryStats = collection.aggregate(
                            Arrays.asList(
                                Aggregates.match(Filters.eq("timeseries", timeseries)),
                                Aggregates.match(Filters.eq("property", property)),
                                Aggregates.match(Filters.and(
                                        Filters.gte("timestamp", from),
                                        Filters.lte("timestamp", to)
                                    )
                                )
                            )
                        ).explain(ExplainVerbosity.EXECUTION_STATS).toJson(JsonWriterSettings.builder().indent(true).build());

                        System.out.println(queryStats);

                  

                    
                
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
