package org.terry.importer;

import java.io.FileInputStream;
import java.util.Properties;
import org.terry.common.workload.MultiWorkload;
import org.terry.common.workload.Workload;
import org.terry.importer.task.AggregateTask;

public class MongoImporter {

    private static final String FILE_PROP = "file";
    private static final String THREAD_PROP = "thread";

    private static final String OPERATION_COUNT = "operation.count";
    private static final String BUCKET_SIZE = "bucket.size";

    public static void main(String[] args) {
        Properties prop = new Properties();
        parseArgs(prop, args);

        int thread = Integer.parseInt(prop.getProperty(THREAD_PROP, "1"));
        int opCount = Integer.parseInt(prop.getProperty(OPERATION_COUNT, "10"));
        int bucketSize = Integer.parseInt(prop.getProperty(BUCKET_SIZE, "1000"));

        AggregateTask task = new AggregateTask();
        task.init(prop);
        Workload workload;
        if (thread > 1) {
            workload = new MultiWorkload(opCount, bucketSize, task, thread);
        } else {
            workload = new Workload(opCount, bucketSize, task);
        }

        long st = System.currentTimeMillis();
        workload.start();
        long et = System.currentTimeMillis();

        System.out.println("Total time cost: " + (et - st) / 1000);
    }

    private static void parseArgs(Properties prop, String[] args) {
        int idx = 0;
        while (idx < args.length) {
            String arg = args[idx];
            String key = null, val = null;
            if (arg.startsWith("--")) {
                arg = arg.substring(2);
                String[] vals = arg.split("=", 2);
                if (vals.length == 2) {
                    key = vals[0];
                    val = vals[1];
                }
            } else if (arg.startsWith("-")) {
                key = arg.substring(1);
                val = (idx + 1) < args.length ? args[idx+1] : null;
                if (val == null || val.startsWith("-")) {
                    idx++;
                    continue;
                }
            } else {
                System.out.println("invalid args: " + arg);
            }

            if (key != null) {
                if (FILE_PROP.equals(key)) {
                    parseFile(prop, val);
                } else {
                    prop.setProperty(key, val);
                }
            }
            idx++;
        }
    }

    private static void parseFile(Properties prop, String file) {
        Properties tmp = new Properties();
        try {
            tmp.load(new FileInputStream(file));
        } catch (Exception e) {
            System.err.println("failed to open file and load properties");
            e.printStackTrace();
            System.exit(1);
        }
        for (String k : tmp.stringPropertyNames()) {
            prop.setProperty(k, tmp.getProperty(k));
        }
    }

}
