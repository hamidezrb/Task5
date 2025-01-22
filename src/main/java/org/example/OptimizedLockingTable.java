package org.example;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OptimizedLockingTable extends AvoidDeadlockLockingTable {
    private final ReentrantReadWriteLock[] optimizedLocks;

    public OptimizedLockingTable(int isolation_level) {
        super(isolation_level);
        this.optimizedLocks = new ReentrantReadWriteLock[numTuples];
        for (int i = 0; i < numTuples; i++) {
            optimizedLocks[i] = new ReentrantReadWriteLock(true); // fair locking
        }
    }

    @Override
    public void getWriteLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        try {
            if (!optimizedLocks[tid].writeLock().tryLock(TIME_OUT, TimeUnit.SECONDS)) {
                handleDeadlock(tid, requestingTransaction, favorOlder);
            }
            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.writeLockGranted(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getReadLock(final int tid, Transaction requestingTransaction, boolean favorOlder) {
        try {
            if (!optimizedLocks[tid].readLock().tryLock(TIME_OUT, TimeUnit.SECONDS)) {
                handleDeadlock(tid, requestingTransaction, favorOlder);
            }
            setHolderTransaction(tid, requestingTransaction);
            requestingTransaction.readLockGranted(tid);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void handleDeadlock(int tid, Transaction requestingTransaction, boolean favorOlder) {
        Transaction holdingTransaction = getHolderTransaction(tid);
        if (holdingTransaction != null && shouldAbort(requestingTransaction, holdingTransaction)) {
            holdingTransaction.rollback();
        }
    }
}
