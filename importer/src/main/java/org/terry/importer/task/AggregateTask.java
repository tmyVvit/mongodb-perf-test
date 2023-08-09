package org.terry.importer.task;

import com.mongodb.client.AggregateIterable;
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

public class AggregateTask extends AbstractMongoDbTask {


    @Override
    public boolean run() {
        return aggregateTest(getMongoCollection());
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
