package org.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvoidDeadlockWriter extends Transaction {

    private final boolean favorOld; // Strategy flag
    final int task;
    final int[] ids_to_update;
    final public AtomicBoolean comitted = new AtomicBoolean(false);
    final AvoidDeadlockLockingTable table;
    final ArrayList<double[]> beforeImages= new ArrayList<double[]>();

    public AvoidDeadlockWriter(int id, AvoidDeadlockLockingTable t, boolean favorOld){
        this.task  = id;
        this.table = t;
        this.favorOld = favorOld; // Set strategy
        this.ids_to_update = new int[AvoidDeadlockLockingTable.numQueriesPerTask];
        getTuplesForUpdate();
    }

    private void getTuplesForUpdate() {
        for(int q=0;q<ids_to_update.length;q++) {
            int id;
            while(contains((id = table.rand.nextInt(table.numTuples)),this.ids_to_update));
            ids_to_update[q] = id;
        }
    }

    private boolean contains(int id, int[] array) {
        for(int v : array ) {
            if(v==id) {
                return true;
            }
        }
        return false;
    }

    void update_repeatable_read() {
        for(int tid : ids_to_update) {
            table.getWriteLock(tid, this,favorOld);
            double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
            double[] beforeImage = table.update(tid,task,new_value);
            beforeImages.add(beforeImage); // Save the before image
        }
        comitted.set(true);	// Needs to done before the locks are released
        for(int tid : ids_to_update) {
            table.releaseWriteLock(tid);
        }
    }

    void update_read_commited() {
        for(int tid : ids_to_update) {
            table.getWriteLock(tid, this,favorOld);
            double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
            double[] beforeImage = table.update(tid,task,new_value);
            beforeImages.add(beforeImage); // Save the before image
        }
        comitted.set(true);	// Needs to done before first lock is released
        for(int tid : ids_to_update) {
            table.releaseWriteLock(tid);
        }
    }

    private void update() {
        for(int tid : ids_to_update) {
            double new_value = table.rand.nextDouble()*table.maxValue;//some hopefully different value
            table.update(tid,task,new_value);
        }
    }

    public String toString() {
		return "Task "+task +" updating "+ ( (ids_to_update.length < 10 ) ? Arrays.toString(ids_to_update) : "["+ids_to_update[0]+", "+ids_to_update[1]+", "+ids_to_update[2]+", ... ");
	}

    @Override
    public boolean rollback() {
        for(int i=0;i<this.myWriteLocks.size();i++) {
            //XXX assumes that index i belongs to the same tuple
            int tid = this.myWriteLocks.get(i);
            if (i < this.beforeImages.size()) {
                double[] beforeImage = this.beforeImages.get(i);
                this.table.restoreOldTupleValues(beforeImage, tid);
            } else {
                System.err.println("Warning: Missing beforeImage for tid " + tid);
            }
            this.table.releaseWriteLock(tid);
        }
        if(!this.myReadLocks.isEmpty()) {
            for(int tid : myReadLocks) {
                this.table.releaseReadLock(tid);
            }
        }
        return true;
    }

    @Override
    public void run() {
        super.run();

        // Only proceed with updates if this is a write transaction
        if (this.type != TransactionType.WRITE) {
            System.out.println("Task " + task + " is not a write transaction, skipping updates");
            table.tasksFinished.incrementAndGet();
            return;
        }

        if (table.isolationLevel == AvoidDeadlockLockingTable.USE_TABLE_LOCKS) {
            table.getTableLock();
            update();
            comitted.set(true);
            table.releaseTableLock();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.REPEATABLE_READ) {
            update_repeatable_read();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.READ_COMMITED) {
            update_read_commited();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.READ_UN_COMMITED) {
            update();
            comitted.set(true);
        }

        System.out.println("Done task " + task + ". I am finisher number " + table.tasksFinished.incrementAndGet());
    }




//    @Override
//    public void run() {
//        if(table.isolationLevel == AvoidDeadlockLockingTable.USE_TABLE_LOCKS) {
//            table.getTableLock();		// (1) Exclusively lock the table
//            update();					// (2) Do the work
//            comitted.set(true);			// Needs to done before the locks are released
//            table.releaseTableLock();	// (3) Release the exclusive lock
//        }else if(table.isolationLevel == AvoidDeadlockLockingTable.REPEATABLE_READ) {
//            update_repeatable_read();
//        }else if(table.isolationLevel == AvoidDeadlockLockingTable.READ_COMMITED) {
//            update_read_commited();
//        }else if(table.isolationLevel == AvoidDeadlockLockingTable.READ_UN_COMMITED) {
//            update();
//            comitted.set(true);
//        }else{
//            System.err.println("ISOLATION_LEVEL="+table.isolationLevel+" currently not supported.");
//            return;
//        }
//
//        System.out.println("Done task "+task+". I am finisher number "+table.tasksFinished.incrementAndGet());
//    }
}
