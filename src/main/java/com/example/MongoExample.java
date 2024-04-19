package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.result.InsertManyResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import org.bson.Document;

public class MongoExample {
    public static void main(String[] args) {


        String connectionString = "mongodb+srv://julianpielmaier:bruteForce@mycluster.um2rnsa.mongodb.net/?retryWrites=true&w=majority&appName=myCluster";

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
                    
                    //database.getCollection("timeseries").drop();

                    // Creates a time series collection that stores "sensorData" values over time
                    if(!database.listCollectionNames().into(new ArrayList<String>()).contains("timeseries")) {
                        database.createCollection("timeseries", collOptions);

                        MongoCollection<Document> collection = database.getCollection("timeseries");


                        // Create sample timeseries data
                        Random random = new Random(123);
                        long timestamp = 1676934000L;
                        ArrayList<Document> timeseriesData = new ArrayList<>();
                        int range = 10;
                        int timeseriesDataPoints = 1_000_000;
                        int batchSize = 10_000;
                        int[] stationMeans = new int[5];
                        String[] stations = {"E 31 St & 3 Ave", 
                        "Broadway & W 32 St",
                        "Howard St & Centre St",
                        "South End Ave & Liberty St",
                        "Greenwich Ave & 8 Ave"};

                        for(int i=0; i < 5; i++){
                            stationMeans[i] = random.nextInt(100);
                        }


                        for(int i = 0; i < timeseriesDataPoints ; i++){

                            // Create timeseries data point
                            timeseriesData.add(
                                new Document()
                                .append("timestamp", new Date(timestamp))
                                .append("station", stations[i % 5])
                                .append("value", stationMeans[i % 5] + random.nextInt(2 * range + 1) - range));

                            timestamp += 3600000;
                        
                            if(i % batchSize == 0){
                            
                                try {

                                // Save sample documents into the collection
                                collection.insertMany(timeseriesData);
                                timeseriesData.clear();
                                System.out.println("Inserted " + batchSize +  " Documents");
                                
                                // Prints the IDs of the inserted documents
                                //System.out.println("Inserted document ids: " + result.getInsertedIds());

                                
                                } catch (MongoException me) {
                                    System.err.println("Unable to insert due to an error: " + me);

                                }

                            }

                        }

                        try {
                            // Save sample documents into the collection
                            InsertManyResult result = collection.insertMany(timeseriesData);
                        
                            // Prints the IDs of the inserted documents
                            //System.out.println("Inserted document ids: " + result.getInsertedIds());


                        } catch (MongoException me) {
                            System.err.println("Unable to insert due to an error: " + me);

                        }

                        // Print information about the database's collections
                        //Document commandResult = database.runCommand(new Document("listCollections", new BsonInt64(1)));
                        //List<String> keys = Arrays.asList("cursor");
                        //System.out.println("listCollections: " + commandResult.getEmbedded(keys, Document.class).toJson());

                    } else {
                        System.out.println("Collection timeseries already exists!");
                    }
                
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error while inserting timeseries data");
                }
                
                try {

                    if(!database.listCollectionNames().into(new ArrayList<String>()).contains("stations")) {
                        database.createCollection("stations");

                        MongoCollection<Document> collection = database.getCollection("stations");


                        ArrayList<Document> stations = new ArrayList<>();

                        stations.addAll(Arrays.asList(
                            new Document()
                            .append("name", "E 31 St & 3 Ave"),
                            new Document()
                            .append("name", "Broadway & W 32 St"),
                            new Document()
                            .append("name", "Howard St & Centre St"),
                            new Document()
                            .append("name", "South End Ave & Liberty St"),
                            new Document()
                            .append("name", "Greenwich Ave & 8 Ave")
                        )
                        );

                        collection.insertMany(stations);

                    } else{
                        System.out.println("Collection stations already exists!");
                    }

                } catch(Exception e){
                    e.printStackTrace();
                    System.out.println("Error while inserting graph data");
                }
                


            } catch (MongoException e) {
                e.printStackTrace();

            }
             

        }

    }

    
    
}
