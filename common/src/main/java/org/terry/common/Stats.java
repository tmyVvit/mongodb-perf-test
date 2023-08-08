package org.terry.common;

public class Stats {
    private double avg;
    private long min;
    private long max;
    private long p95;
    private long p99;

    private int ops;
    private int success;
    private int failed;
    private int overflow;

    public Stats() {}

    public double getAvg() {
        return avg;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public long getP95() {
        return p95;
    }

    public long getP99() {
        return p99;
    }

    public int getOps() {
        return ops;
    }

    public int getFailed() {
        return failed;
    }

    public int getSuccess() {
        return success;
    }

    public int getOverflow() {
        return overflow;
    }

    public Stats avg(double avg) {
        this.avg = avg;
        return this;
    }

    public Stats min(long min) {
        this.min = min;
        return this;
    }

    public Stats max(long max) {
        this.max = max;
        return this;
    }

    public Stats p95(long p95) {
        this.p95 = p95;
        return this;
    }

    public Stats p99(long p99) {
        this.p99 = p99;
        return this;
    }

    public Stats success(int success) {
        this.success = success;
        return this;
    }

    public Stats failed(int failed) {
        this.failed = failed;
        return this;
    }

    public Stats ops(int ops) {
        this.ops = ops;
        return this;
    }

    public Stats overflow(int overflow) {
        this.overflow = overflow;
        return this;
    }

    @Override
    public String toString() {
        return "avg|min|max|p95|p99|total ops|success ops|fail ops|latency overflow\n"
            + String.format("%.02f|%d|%d|%d|%d|%d|%d|%d|%d", avg, min, max, p95, p99, ops, success, failed, overflow);
    }
}
