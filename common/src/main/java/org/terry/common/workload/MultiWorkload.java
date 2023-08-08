package org.terry.common.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.terry.common.task.Task;

public class MultiWorkload extends Workload {

    private final int thread;

    public MultiWorkload(int opCount, int bucketSize, Task task, int thread) {
        super(opCount/thread, bucketSize, task);
        this.thread = thread;
    }

    public void start() {
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(thread);
        for (int i = 0; i < thread; i++) {
            Thread t = new Thread(new InnerRun(super::inner, latch));
            t.start();
            threads.add(t);
        }

//        for (Thread t : threads) {
//            try {
//                t.join();
//            } catch (InterruptedException ignore) {
//            }
//        }
        try {
            latch.await();
        } catch (InterruptedException ignore) {
        }
        System.out.println("[DONE] result: \n" + getStats());
    }

    private static class InnerRun implements Runnable {

        final Runnable runnable;
        final CountDownLatch latch;

        InnerRun(Runnable runnable, CountDownLatch latch) {
            this.runnable = runnable;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } finally {
                latch.countDown();
            }
        }
    }
}
