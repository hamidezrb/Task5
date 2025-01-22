package org.example;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class AvoidDeadlockReader extends Transaction {

    private final AtomicInteger writeCount; // Strategy flag
    private final boolean favorOld; // Strategy flag
    public boolean displayErrors = false;
    final double[] repeatbale_read_checking;
    long start, stop, last_stop;
    int numExecutions = 0;
    ArrayList<UncommittedReads> un_commited = new ArrayList<UncommittedReads>();
    ArrayList<NonRepeatableReads> non_repeatable = new ArrayList<NonRepeatableReads>();

    final AvoidDeadlockLockingTable table;
    private final int[] selectedTuples; // For multi-point queries

    public AvoidDeadlockReader(AvoidDeadlockLockingTable t,boolean favorOld,AtomicInteger writeCount){
        this.table = t;
        this.writeCount = writeCount;
        this.favorOld = favorOld;
        this.repeatbale_read_checking = new double[table.numTuples];
//        this.selectedTuples = new int[AvoidDeadlockLockingTable.numQueriesPerTask];
        this.selectedTuples = new int[2];
        if (this.type == TransactionType.MULTI_POINT_READ) {
            selectRandomTuples();
        }
    }


    private void selectRandomTuples() {
        Random rand = new Random();
        for (int i = 0; i < selectedTuples.length; i++) {
            selectedTuples[i] = rand.nextInt(table.numTuples);
        }
    }

    @Override
    public void run() {
        start = System.currentTimeMillis();
        last_stop = start;
        while (table.tasksFinished.get() !=  writeCount.get()) {
            if (type == TransactionType.MULTI_POINT_READ) {
                executeMultiPointRead();
            } else if (type == TransactionType.FULL_SCAN_READ) {
                executeFullScanRead();
            }

            stop = System.currentTimeMillis();
            //printStatistics();
        }
    }

    private void executeMultiPointRead() {
        if (table.isolationLevel == AvoidDeadlockLockingTable.USE_TABLE_LOCKS) {
            table.getTableLock();
            readSelectedTuples();
            table.releaseTableLock();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.REPEATABLE_READ) {
            readSelectedTuplesRepeatable();
        } else {
            readSelectedTuples();
        }
    }

    private void executeFullScanRead() {
        if (table.isolationLevel == AvoidDeadlockLockingTable.USE_TABLE_LOCKS) {
            table.getTableLock();
            read();
            table.releaseTableLock();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.REPEATABLE_READ) {
            read_repeatable();
        } else if (table.isolationLevel == AvoidDeadlockLockingTable.READ_COMMITED) {
            read_commited();
        } else {
            read();
        }
    }

    private void readSelectedTuples() {
        for (int tid : selectedTuples) {
            check_query_1(tid);
            check_query_2(tid);
        }
    }

    private void readSelectedTuplesRepeatable() {
        // Acquire locks for selected tuples
        for (int tid : selectedTuples) {
            table.getReadLock(tid, this, favorOld);
        }

        try {
            for (int tid : selectedTuples) {
                check_query_1(tid);
                check_query_2(tid);
            }
        } finally {
            for (int tid : selectedTuples) {
                table.releaseReadLock(tid);
            }
        }
    }

    private void printStatistics() {
        System.out.println("Reader (" + type + ") finished query " + (numExecutions++) +
                " needed " + (stop - last_stop) +
                " avg run time is " + (stop - start) / (long)numExecutions);

        printAnomalies();
    }

    private void printAnomalies() {
        if (!un_commited.isEmpty()) {
            if (table.isolationLevel <= AvoidDeadlockLockingTable.READ_UN_COMMITED)
                System.out.println("Found un_commited data " + this.un_commited.size() + " [OK for this level]");
            else
                System.err.println("Found un_commited data " + this.un_commited.size());
        }
        if (!non_repeatable.isEmpty()) {
            if (table.isolationLevel < AvoidDeadlockLockingTable.REPEATABLE_READ)
                System.out.println("Found non_repeatable reads " + this.non_repeatable.size() + " [OK for this level]");
            else
                System.err.println("Found non_repeatable reads " + this.non_repeatable.size());
        }
    }

//Old Code

//    @Override
//    public void run() {
//        start = System.currentTimeMillis();
//        last_stop = start;
//        while(table.tasksFinished.get()!=AvoidDeadlockLockingTable.numTasks) {// runs until last Writer task is done. Objective is to execute as many loops as possible.
//            if(table.isolationLevel == AvoidDeadlockLockingTable.USE_TABLE_LOCKS) {
//                // (1) Exclusively lock the table
//                table.getTableLock();
//                //(2) Do the work
//                read();
//                table.releaseTableLock();
//            }else if(table.isolationLevel == AvoidDeadlockLockingTable.REPEATABLE_READ) {
//                read_repeatable();
//            }else if(table.isolationLevel == AvoidDeadlockLockingTable.READ_COMMITED) {
//                read_commited();
//            }else if(table.isolationLevel == AvoidDeadlockLockingTable.READ_UN_COMMITED) {
//                read();// We do not need locks here.
//            }else{
//                System.err.println("ISOLATION_LEVEL="+table.isolationLevel+" currently not supported.");
//                return;
//            }
//            stop = System.currentTimeMillis();
//
//            // Print statistics
//            System.out.println("Reader finished query "+(numExecutions++)+" needed "+(stop-last_stop)+ " avg run time is "+(stop-start)/(long)numExecutions);
//            if(!un_commited.isEmpty()) {
//                if(table.isolationLevel <= AvoidDeadlockLockingTable.READ_UN_COMMITED)
//                    System.out.println("Found un_commited data "+this.un_commited.size()+ " [OK for this level]");
//                else
//                    System.err.println("Found un_commited data "+this.un_commited.size());
//            }
//            if(!non_repeatable.isEmpty()) {
//                if(table.isolationLevel < AvoidDeadlockLockingTable.REPEATABLE_READ)
//                    System.out.println("Found non_repeatable reads "+this.non_repeatable.size()+ " [OK for this level]");
//                else
//                    System.err.println("Found non_repeatable reads "+this.non_repeatable.size());
//            }
//        }
//    }

    void read_commited() {
        //first full-table scan
        for(int tid=0;tid<table.numTuples;tid++) {
            table.getReadLock(tid, this,favorOld);
            check_query_1(tid);
            table.releaseReadLock(tid);
        }

        //second full-table scan
        for(int tid=0;tid<table.numTuples;tid++) {
            table.getReadLock(tid, this,favorOld);
            check_query_2(tid);
            table.releaseReadLock(tid);
        }
    }

void read_repeatable() {
        // Acquire read locks for all tuples at the start
        for (int tid = 0; tid < table.numTuples; tid++) {
            table.getReadLock(tid, this,favorOld);
        }

        try {
            // First full-table scan
            for (int tid = 0; tid < table.numTuples; tid++) {
                check_query_1(tid);
            }

            // Second full-table scan
            for (int tid = 0; tid < table.numTuples; tid++) {
                check_query_2(tid);
            }
        } finally {
            // Release all read locks after both scans are complete
            for (int tid = 0; tid < table.numTuples; tid++) {
                table.releaseReadLock(tid);
            }
        }
    }

    void check_query_1(final int tid) {
        double[] tuple = table.getTuple(tid);

        double check = tuple[AvoidDeadlockLockingTable.DATA_VALUE];
        repeatbale_read_checking[tid] = check;
        int modified_by = (int) tuple[AvoidDeadlockLockingTable.MODIFIED_BY];
        if(modified_by != AvoidDeadlockLockingTable.NOT_MODIFIED) {
            if(!table.myWriter[modified_by].comitted.get()) {
                if(displayErrors) System.err.println("Seeing non comitted values of task "+modified_by +" at "+tid);
                this.un_commited.add(new UncommittedReads(numExecutions, tid, modified_by));
            }
        }
    }

    private void check_query_2(final int tid) {
        double[] tuple = table.getTuple(tid);

        double check = tuple[AvoidDeadlockLockingTable.DATA_VALUE];
        if(check != repeatbale_read_checking[tid]){
            if(displayErrors) System.err.println("Found non-repeatable read at "+tid+ " issued by "+ tuple[AvoidDeadlockLockingTable.MODIFIED_BY]);
            this.non_repeatable.add(new NonRepeatableReads(numExecutions, tid));
        }
        int modified_by = (int) tuple[AvoidDeadlockLockingTable.MODIFIED_BY];
        if(modified_by != AvoidDeadlockLockingTable.NOT_MODIFIED) {
            if(!table.myWriter[modified_by].comitted.get()) {
                if(displayErrors) System.err.println("Seeing non comitted values of task "+modified_by +" at "+tid);
                this.un_commited.add(new UncommittedReads(numExecutions, tid, modified_by));
            }
        }
    }

    private void read() {
        //first full-table scan
        for(int tid=0;tid<table.numTuples;tid++) {
            check_query_1(tid);
        }

        //second full-table scan
        for(int tid=0;tid<table.numTuples;tid++) {
            check_query_2(tid);
        }
    }

    class UncommittedReads{
        final int num_executions, tid, modified_by;

        public UncommittedReads(int num_executions, int tid, int modified_by) {
            this.tid = tid;
            this.num_executions = num_executions;
            this.modified_by = modified_by;
        }
        public String toString() {
            return "Seeing non comitted values of task \"+modified_by +\" at \"+tid";
        }
    }

    class NonRepeatableReads{
        final int num_executions, tid;

        public NonRepeatableReads(int num_executions, int tid) {
            this.tid = tid;
            this.num_executions = num_executions;
        }
        public String toString() {
            return "Found non-repeatable read at "+tid;
        }
    }

    @Override
    public boolean rollback() {
        if(!myWriteLocks.isEmpty()) {
            System.err.println("rollback(): Readers should not holds write locks");
        }
        for(int tid_locked : myReadLocks) {
            this.table.releaseReadLock(tid_locked);
        }
        return true;
    }

}
