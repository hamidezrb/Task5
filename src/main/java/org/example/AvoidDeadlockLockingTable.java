package org.example;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

public class AvoidDeadlockLockingTable {
    private final ConcurrentHashMap<Integer, Transaction> lockHolders = new ConcurrentHashMap<>();
    static boolean favorOldTransactions = true;

    static int numTasks = 10;
    static int numQueriesPerTask = 100;
    final AvoidDeadlockWriter[] myWriter;
    final ArrayList<AvoidDeadlockReader> myReaders = new ArrayList<>();
    static final int THOUSAND = 1000;
    static final long ADDITIONAL_WRITE_COST = 10;

    Random rand = new Random();
    int numTuples = 5 * THOUSAND * THOUSAND;
    double maxValue = 50.0f;
    AtomicInteger tasksFinished;
    public static final int NOT_MODIFIED = -1;

    private double[][] tableData;
    static final int DATA_ID = 0;
    static final int DATA_VALUE = 1;
    static final int MODIFIED_BY = 2;
    static final int READ_UN_COMMITED = 0;
    static final int READ_COMMITED = READ_UN_COMMITED + 1;
    static final int REPEATABLE_READ = READ_COMMITED + 1;
    static final int USE_TABLE_LOCKS = REPEATABLE_READ + 1;

    final int isolationLevel;
    static final String[] LOCK_LEVEL_NAMES = {"READ_UN_COMMITED", "READ_COMMITED", "REPEATABLE_READ", "SERIAL"};
    public final Semaphore rowLocks[];
    private final Semaphore table_lock = new Semaphore(1, true);
    long TIME_OUT = 5;

    // Statistics tracking
    private final AtomicInteger multiPointReadCount = new AtomicInteger(0);
    private final AtomicInteger fullScanReadCount = new AtomicInteger(0);
    private final AtomicInteger writeCount = new AtomicInteger(0);

    public AvoidDeadlockLockingTable(final int isolation_level) {
        System.out.println("Creating Table with isolation level " + LOCK_LEVEL_NAMES[isolation_level] + " with " + numTuples + " tuples");
        this.isolationLevel = isolation_level;
        this.tasksFinished = new AtomicInteger(0);
        this.myWriter = new AvoidDeadlockWriter[numTasks];

        this.tableData = new double[numTuples][3];
        this.rowLocks = new Semaphore[numTuples];

        initializeTableData();
    }

    private void initializeTableData() {
        System.out.print("Creating DB ");
        for (int i = 0; i < numTuples; i++) {
            this.tableData[i][DATA_ID] = i;
            this.tableData[i][DATA_VALUE] = rand.nextDouble() * maxValue;
            this.tableData[i][MODIFIED_BY] = NOT_MODIFIED;
            this.rowLocks[i] = new Semaphore(numTasks, true);
        }
        System.out.println("[DONE]");
    }

    public void run() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numTasks);
        initializeTransactions(executor);

        double start = System.currentTimeMillis();
        waitForCompletion(executor);
        double stop = System.currentTimeMillis();

        printStatistics(start, stop);
    }

    private void initializeTransactions(ThreadPoolExecutor executor) {
        // Create and start transactions based on random probability
        for (int i = 0; i < numTasks; i++) {
            double p = rand.nextDouble();
            if (p >= Transaction.FULL_SCAN_THRESHOLD) {
                // Write transaction
                System.out.println("Write");
                myWriter[i] = new AvoidDeadlockWriter(i, this, favorOldTransactions);
                executor.execute(myWriter[i]);
                writeCount.incrementAndGet();
            }
            else if (p >= Transaction.MULTI_POINT_THRESHOLD) {
                // Full scan read transaction
                System.out.println("Read");
                AvoidDeadlockReader reader = new AvoidDeadlockReader(this, favorOldTransactions,writeCount);
                myReaders.add(reader);
                executor.execute(reader);
                fullScanReadCount.incrementAndGet();
            }
            else {
                // Multi-point read transaction
                System.out.println("8888888888888");
                AvoidDeadlockReader reader = new AvoidDeadlockReader(this, favorOldTransactions,writeCount);
                myReaders.add(reader);
                executor.execute(reader);
                multiPointReadCount.incrementAndGet();
            }
        }
        executor.shutdown();
    }

    private void waitForCompletion(ThreadPoolExecutor executor) {
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void printStatistics(double start, double stop) {
        System.out.println("\n=== Transaction Statistics ===");
        System.out.println("Total execution time: " + (stop - start) + " ms");
        System.out.println("Multi-point read transactions: " + multiPointReadCount.get());
        System.out.println("Full scan read transactions: " + fullScanReadCount.get());
        System.out.println("Write transactions: " + writeCount.get());
        System.out.println("Tasks finished: " + tasksFinished.get() + " of " + numTasks);
        System.out.println("==========================\n");
    }

    public void getWriteLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        try {
            while (!this.rowLocks[tid].tryAcquire(numTasks, TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
                Transaction holdingTransaction = getHolderTransaction(tid);
                if (holdingTransaction != null) {
                    if (shouldAbort(requestingTransaction, holdingTransaction)) {
                        System.out.println("Aborting " + (favorOlder ? "younger" : "older") + " transaction " + holdingTransaction);
                        holdingTransaction.rollback();
                        break;
                    }
                }
                System.out.println(requestingTransaction + " Waiting for write lock of tuple " + tid + " Deadlock?");
            }
            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.writeLockGranted(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void getReadLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        try {
            while (!this.rowLocks[tid].tryAcquire(TIME_OUT, java.util.concurrent.TimeUnit.SECONDS)) {
                Transaction holdingTransaction = getHolderTransaction(tid);
                if (holdingTransaction != null) {
                    if (shouldAbort(requestingTransaction, holdingTransaction)) {
                        System.out.println("Aborting " + (favorOlder ? "younger" : "older") + " transaction " + holdingTransaction);
                        holdingTransaction.rollback();
                        break;
                    }
                }
                System.out.println(requestingTransaction + " Waiting for read lock of tuple " + tid + " Deadlock?");
            }
            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.readLockGranted(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean shouldAbort(Transaction requester, Transaction holder) {
        return favorOldTransactions ?
                requester.getTimestamp() < holder.getTimestamp() :
                requester.getTimestamp() > holder.getTimestamp();
    }

    public void releaseWriteLock(final int tid) {
        clearHolderTransaction(tid);
        this.rowLocks[tid].release(numTasks);
    }

    public void releaseReadLock(final int tid) {
        clearHolderTransaction(tid);
        this.rowLocks[tid].release();
    }

    public Transaction getHolderTransaction(int tid) {
        return lockHolders.get(tid);
    }

    public void setHolderTransaction(int tid, Transaction t) {
        lockHolders.put(tid, t);
    }

    public void clearHolderTransaction(int tid) {
        lockHolders.remove(tid);
    }

    public double[] getTuple(final int tid) {
        return this.tableData[tid];
    }

    public double[] update(final int tid, final int task, final double new_value) {
        double[] tuple = this.tableData[tid];
        double[] beforeImage = new double[tuple.length];
        System.arraycopy(tuple, 0, beforeImage, 0, tuple.length);
        tuple[MODIFIED_BY] = task;
        tuple[DATA_VALUE] = new_value;
        try {
            Thread.sleep(ADDITIONAL_WRITE_COST);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return beforeImage;
    }

    public void restoreOldTupleValues(double[] beforeImage, int tid) {
        double[] tuple = this.tableData[tid];
        System.arraycopy(beforeImage, 0, tuple, 0, tuple.length);
    }

    public void getTableLock() {
        this.table_lock.acquireUninterruptibly();
    }

    public void releaseTableLock() {
        this.table_lock.release();
    }

    public static void main(String[] args) {
        int isolation_level = REPEATABLE_READ;
        AvoidDeadlockLockingTable.numTasks = 10;

        System.out.println("=== Testing " + (favorOldTransactions ? "Favor Old" : "Favor Younger") + " Transactions ===");
        AvoidDeadlockLockingTable b = new AvoidDeadlockLockingTable(isolation_level);
        b.run();
    }
}