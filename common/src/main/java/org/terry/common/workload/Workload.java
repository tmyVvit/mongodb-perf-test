package org.terry.common.workload;

import org.terry.common.Stats;
import org.terry.common.measurements.Measurement;
import org.terry.common.task.Task;

public class Workload {
    private final int opCount;

    private final Task task;

    private final Measurement measurement;

    public Workload(int opCount, int bucketSize, Task task) {
        if (task == null) {
            throw new IllegalArgumentException("task cannot be empty");
        }
        if (bucketSize <= 10 || bucketSize > 10000) {
            throw new IllegalArgumentException("bucket size should between [10, 10000]");
        }
        this.opCount = opCount;
        this.task = task;
        measurement = new Measurement(bucketSize);
    }

    public Workload(int opCount, Task task, Measurement measurement) {
        this.opCount = opCount;
        this.task = task;
        this.measurement = measurement;
    }

    public void start() {
        inner();
        System.out.println("[DONE] result: \n" + getStats());
    }

    protected void inner() {
        for (int i = 0; i < opCount; i++) {
            long st = System.nanoTime();
            boolean success = task.run();
            long latency = (System.nanoTime() - st)/1000;
            measurement.measure(latency, success);
        }
    }

    public Stats getStats() {
        return measurement.stats();
    }
}
