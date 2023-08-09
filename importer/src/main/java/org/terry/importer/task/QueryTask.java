package org.terry.importer.task;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.bson.Document;
import org.bson.conversions.Bson;

public class QueryTask extends AbstractMongoDbTask {

    private List<String> searchFields;
    private List<String> values;
    private int size;

    public static final String SEARCH_FIELDS_PROP = "mongodb.search.field";

    public static final String SEARCH_VALUE_PROP = "mongodb.search.value";

    public static final String NULL_STRING = "null";

    @Override
    public void init(Properties prop) {
        super.init(prop);
        String fields = prop.getProperty(SEARCH_FIELDS_PROP);
        if (fields == null || fields.length() == 0) {
            throw new IllegalArgumentException(SEARCH_FIELDS_PROP + " cannot be empty");
        }

        searchFields = Arrays.asList(fields.split(","));

        String value = prop.getProperty(SEARCH_VALUE_PROP);
        if (value == null || value.length() == 0) {
            throw new IllegalArgumentException(SEARCH_VALUE_PROP + " cannot be empty");
        }
        values = Arrays.asList(value.split(","));
        if (values.size() != searchFields.size()) {
            throw new IllegalArgumentException(SEARCH_FIELDS_PROP + " and " + SEARCH_VALUE_PROP + " are not match");
        }
        size = searchFields.size();
        for (int i = 0; i < size; i++) {
            if (NULL_STRING.equals(values.get(i))) {
                values.set(i, null);
            }
        }
    }

    @Override
    public boolean run() {
        List<Bson> filters = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String f = searchFields.get(i);
            String v = values.get(i);
            filters.add(Filters.eq(f, v));
        }
        try {
            FindIterable<Document> findIterable = getMongoCollection().find(Filters.and(filters));
            Document first = findIterable.first();
            if (printResult) {
                System.out.println(first);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
