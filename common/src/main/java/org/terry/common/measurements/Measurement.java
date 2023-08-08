package org.terry.common.measurements;

import org.terry.common.Stats;

public class Measurement {

    private int opCount;

    private int successCount;

    private int failedCount;

    private int latencyOverflowCount;

    private long totalLatency;

    private long minLatency = -1;

    private long maxLatency = -1;

    /**
     * bucket[i] 代表时延为 i 的次数，方便计算 p95, p99
     */
    private final int[] buckets;

    private final int bucketCount;

    public Measurement(int bucketCount) {
        this.bucketCount = bucketCount;
        this.buckets = new int[this.bucketCount];
    }

    public synchronized void measure(long latency, boolean success) {
        long idx = latency / 1000;
        if (idx >= bucketCount) {
            latencyOverflowCount++;
        } else {
            buckets[(int) idx]++;
        }
        totalLatency += latency;
        opCount++;
        if (success) {
            successCount++;
        } else {
            failedCount++;
        }

        if (maxLatency == -1 || maxLatency < latency) {
            maxLatency = latency;
        }
        if (minLatency == -1 || minLatency > latency) {
            minLatency = latency;
        }
    }

    public Stats stats() {
        double avg = totalLatency / ((double) opCount);
        long p95 = 0,p99 = 0;

        int ops = 0;
        boolean doneP95 = false;
        for (int i = 0; i < bucketCount; i++) {
            ops += buckets[i];
            if ((!doneP95) && ops / ((double) opCount) >= 0.95) {
                doneP95 = true;
                p95 = (long) i * 1000;
            }

            if (ops / ((double) opCount) >= 0.99) {
                p99 = (long) i * 1000;
                break;
            }
        }

        return new Stats()
            .avg(avg)
            .max(maxLatency)
            .min(minLatency)
            .p95(p95)
            .p99(p99)
            .ops(opCount)
            .failed(failedCount)
            .success(successCount)
            .overflow(latencyOverflowCount)
            ;
    }


}
