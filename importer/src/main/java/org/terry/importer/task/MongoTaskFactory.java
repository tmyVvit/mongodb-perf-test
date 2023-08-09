package org.terry.importer.task;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("checked")
public class MongoTaskFactory {

    private static final Map<String, Class<?>> taskMap;

    static {
        taskMap = new HashMap<>();
        taskMap.put("agg", AggregateTask.class);
        taskMap.put("query", QueryTask.class);
    }

    public static AbstractMongoDbTask newTask(String name) {
        Class<AbstractMongoDbTask> clazz = (Class<AbstractMongoDbTask>) taskMap.get(name);
        if (clazz == null) {
            throw new IllegalArgumentException("invalid task name: " + name + ". valid names: " + taskNames());
        }

        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("system error, please check", e);
        }
    }

    public static Set<String> taskNames() {
        return Collections.unmodifiableSet(taskMap.keySet());
    }

}
