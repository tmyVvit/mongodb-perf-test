package org.terry.importer.task;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.util.Properties;
import org.bson.Document;
import org.terry.common.task.Task;

public abstract class AbstractMongoDbTask implements Task {

    public static final String MONGODB = "mongodb";

    public static final String URI_PROP = MONGODB + ".uri";

    public static final String DATABASE_PROP = MONGODB + ".database";

    public static final String COLLECTION_PROP = MONGODB + ".collection";

    public static final String PRINT_RESULT = MONGODB + ".printResult";

    private MongoClient mongoClient;

    private MongoCollection<Document> mongoCollection;

    protected boolean printResult = false;

    @Override
    public void init(Properties prop) {
        mongoClient = MongoClients.create(prop.getProperty(URI_PROP, "mongodb://127.0.0.1:27017"));
        String database = prop.getProperty(DATABASE_PROP, "tmy");
        String collection = prop.getProperty(COLLECTION_PROP, "tmy_coll");
        mongoCollection = mongoClient.getDatabase(database).getCollection(collection);

        if (prop.containsKey(PRINT_RESULT)) {
            printResult = Boolean.parseBoolean(prop.getProperty(PRINT_RESULT, "false"));
        }

    }

    protected MongoClient getMongoClient() {
        return mongoClient;
    }

    protected MongoCollection<Document> getMongoCollection() {
        return mongoCollection;
    }
}
