package org.example;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

public class PerformanceTest {
    // Track timing per thread to avoid conflicts in concurrent access
    public static final ConcurrentHashMap<Long, ThreadTimer> threadTimers = new ConcurrentHashMap<>();

    // Aggregate statistics
    public static final AtomicLong totalLockWaitTime = new AtomicLong(0);
    public static final AtomicLong totalUpdateTime = new AtomicLong(0);
    public static final AtomicLong lockAttempts = new AtomicLong(0);
    public static final AtomicLong failedLockAttempts = new AtomicLong(0);

    public static void main(String[] args) {
        // Test with different loads
        testWithLoad(100);  // Low load
        testWithLoad(500);  // Medium load
        testWithLoad(1000); // High load
    }

    private static void testWithLoad(int numTasks) {
        // Reset statistics
        threadTimers.clear();
        totalLockWaitTime.set(0);
        totalUpdateTime.set(0);
        lockAttempts.set(0);
        failedLockAttempts.set(0);

        System.out.println("\nTesting with " + numTasks + " tasks:");
        AvoidDeadlockLockingTable.numTasks = numTasks;

        InstrumentedLockingTable table = new InstrumentedLockingTable(
                AvoidDeadlockLockingTable.READ_COMMITED);

        long startTime = System.nanoTime();
        table.run();
        long endTime = System.nanoTime();

        printStatistics(startTime, endTime);
    }

    private static void printStatistics(long startTime, long endTime) {
        long totalTime = (endTime - startTime) / 1_000_000; // Convert to ms
        long avgLockWaitTime = lockAttempts.get() > 0 ?
                totalLockWaitTime.get() / lockAttempts.get() / 1_000_000 : 0;

        System.out.println("Performance Statistics:");
        System.out.println("----------------------");
        System.out.println("Total execution time: " + totalTime + "ms");
        System.out.println("Total lock wait time: " + (totalLockWaitTime.get() / 1_000_000) + "ms");
        System.out.println("Average lock wait time: " + avgLockWaitTime + "ms per attempt");
        System.out.println("Total update time: " + (totalUpdateTime.get() / 1_000_000) + "ms");
        System.out.println("Lock attempts: " + lockAttempts.get());
        System.out.println("Failed lock attempts: " + failedLockAttempts.get());
        System.out.println("Lock contention rate: " +
                (lockAttempts.get() > 0 ?
                        (failedLockAttempts.get() * 100.0 / lockAttempts.get()) : 0) + "%");
    }
}

class ThreadTimer {
    private long lockWaitStartTime = 0;

    public void startWait() {
        lockWaitStartTime = System.nanoTime();
    }

    public long endWait() {
        if (lockWaitStartTime == 0) return 0;
        long waitTime = System.nanoTime() - lockWaitStartTime;
        lockWaitStartTime = 0;
        return waitTime;
    }
}

class InstrumentedLockingTable extends AvoidDeadlockLockingTable {

    public InstrumentedLockingTable(int isolation_level) {
        super(isolation_level);
    }

    @Override
    public void getWriteLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        ThreadTimer timer = getOrCreateThreadTimer();
        timer.startWait();
        PerformanceTest.lockAttempts.incrementAndGet();

        try {
            boolean acquired = false;
            while (!acquired) {
                acquired = rowLocks[tid].tryAcquire(numTasks, TIME_OUT, java.util.concurrent.TimeUnit.SECONDS);
                if (!acquired) {
                    PerformanceTest.failedLockAttempts.incrementAndGet();
                    Transaction holdingTransaction = getHolderTransaction(tid);
                    if (holdingTransaction != null) {
                        if (shouldAbort(requestingTransaction, holdingTransaction)) {
                            holdingTransaction.rollback();
                            break;
                        }
                    }
                }
            }
            PerformanceTest.totalLockWaitTime.addAndGet(timer.endWait());

            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.writeLockGranted(tid);
        } catch (InterruptedException e) {
            PerformanceTest.totalLockWaitTime.addAndGet(timer.endWait());
            e.printStackTrace();
        }
    }

    @Override
    public void getReadLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        ThreadTimer timer = getOrCreateThreadTimer();
        timer.startWait();
        PerformanceTest.lockAttempts.incrementAndGet();

        try {
            boolean acquired = false;
            while (!acquired) {
                acquired = rowLocks[tid].tryAcquire(TIME_OUT, java.util.concurrent.TimeUnit.SECONDS);
                if (!acquired) {
                    PerformanceTest.failedLockAttempts.incrementAndGet();
                    Transaction holdingTransaction = getHolderTransaction(tid);
                    if (holdingTransaction != null) {
                        if (shouldAbort(requestingTransaction, holdingTransaction)) {
                            holdingTransaction.rollback();
                            break;
                        }
                    }
                }
            }
            PerformanceTest.totalLockWaitTime.addAndGet(timer.endWait());

            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.readLockGranted(tid);
        } catch (InterruptedException e) {
            PerformanceTest.totalLockWaitTime.addAndGet(timer.endWait());
            e.printStackTrace();
        }
    }

    @Override
    public double[] update(final int tid, final int task, final double new_value) {
        long startTime = System.nanoTime();
        double[] result = super.update(tid, task, new_value);
        PerformanceTest.totalUpdateTime.addAndGet(System.nanoTime() - startTime);
        return result;
    }

    private ThreadTimer getOrCreateThreadTimer() {
        long threadId = Thread.currentThread().getId();
        return PerformanceTest.threadTimers.computeIfAbsent(threadId, k -> new ThreadTimer());
    }
}