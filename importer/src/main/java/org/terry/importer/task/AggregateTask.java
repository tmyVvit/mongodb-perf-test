package org.terry.importer.task;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BucketOptions;
import com.mongodb.client.model.Facet;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import java.util.Arrays;
import java.util.Properties;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.terry.common.task.Task;

public class AggregateTask implements Task {

    public static final String MONGODB = "mongodb";

    public static final String URI_PROP = MONGODB + ".uri";

    public static final String DATABASE_PROP = MONGODB + ".database";

    public static final String COLLECTION_PROP = MONGODB + ".collection";

    public static final String PRINT_RESULT = MONGODB + ".printResult";

    private MongoClient mongoClient;

    private MongoCollection<Document> mongoCollection;

    private boolean printResult = false;

    @Override
    public void init(Properties properties) {
        mongoClient = MongoClients.create(properties.getProperty(URI_PROP, "mongodb://127.0.0.1:27017"));
        String database = properties.getProperty(DATABASE_PROP, "tmy");
        String collection = properties.getProperty(COLLECTION_PROP, "tmy_coll");
        mongoCollection = mongoClient.getDatabase(database).getCollection(collection);

        if (properties.containsKey(PRINT_RESULT)) {
            printResult = Boolean.parseBoolean(properties.getProperty(PRINT_RESULT, "false"));
        }
    }

    @Override
    public boolean run() {
        return aggregateTest(mongoCollection);
    }

    private boolean aggregateTest(MongoCollection<Document> collection) {
        boolean success = true;
        try {
            Bson match = Aggregates.match(Filters.type("age", "number"));
            Bson project = Aggregates.project(Projections.include("_id", "age"));

            Bson distribution = Aggregates.bucket("$age", Arrays.asList(0, 18, 24, 35, 60, 100), new BucketOptions().defaultBucket("invalid"));
            Bson minmax = Aggregates.group(null, Arrays.asList(Accumulators.max("max", "$age"), Accumulators.min("min", "$age")));
            Bson count = Aggregates.count("totalCount");
            Facet facet0 = new Facet("distribution", distribution);
            Facet facet1 = new Facet("minmax", minmax);
            Facet facet2 = new Facet("count", count);
            Bson facets = Aggregates.facet(facet0, facet1, facet2);
            AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(match, project, facets));

            for (Document d : iterable) {
                if (printResult) {
                    System.out.println(d);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

}
