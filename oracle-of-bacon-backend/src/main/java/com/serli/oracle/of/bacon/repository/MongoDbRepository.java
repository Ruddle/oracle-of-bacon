package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

public class MongoDbRepository {

    private final MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
        database = mongoClient.getDatabase("workshop");
        collection = database.getCollection("actors");
    }

    public Optional<Document> getActorByName(String name) {
        Document doc = collection.find(eq("name", name)).first();
        return Optional.ofNullable(doc);
    }
}
