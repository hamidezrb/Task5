package org.example;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

public abstract class Transaction implements Runnable {
	public static final double MULTI_POINT_THRESHOLD = 0.3;
	public static final double FULL_SCAN_THRESHOLD = 0.7;

	protected TransactionType type;
	public long startTime;
	public ArrayList<Integer> myReadLocks = new ArrayList<Integer>();
	public ArrayList<Integer> myWriteLocks = new ArrayList<Integer>();
	AtomicBoolean isRolledBack = new AtomicBoolean(false);
	private static final AtomicLong timestampCounter = new AtomicLong(0);
	private final long timestamp;
	protected static final Random random = new Random();

	public enum TransactionType {
		MULTI_POINT_READ,
		FULL_SCAN_READ,
		WRITE
	}

	public Transaction() {
		this.timestamp = timestampCounter.incrementAndGet();
		determineTransactionType();
	}

	protected void determineTransactionType() {
		double p = random.nextDouble();
		if (p < MULTI_POINT_THRESHOLD) {
			type = TransactionType.MULTI_POINT_READ;
		} else if (p < FULL_SCAN_THRESHOLD) {
			type = TransactionType.FULL_SCAN_READ;
		} else {
			type = TransactionType.WRITE;
		}
	}

	public TransactionType getType() {
		return type;
	}

	@Override
	public void run() {
		this.startTime = System.currentTimeMillis();
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public boolean begin_of_transaction() {
		myReadLocks.clear();
		myWriteLocks.clear();
		return true;
	}

	public ArrayList<Integer> myReadLocks() {
		return this.myReadLocks;
	}

	public ArrayList<Integer> myWriteLocks() {
		return this.myWriteLocks;
	}

	public abstract boolean rollback();

	public void readLockGranted(final int id){
		myReadLocks.add(id);
	}

	public void writeLockGranted(final int id){
		myWriteLocks.add(id);
	}

	public boolean isRolledBack() {
		return this.isRolledBack.get();
	}
}