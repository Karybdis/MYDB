package com.cheng.mydb.backend.vm;

import com.cheng.mydb.backend.tm.TransactionManager;

public class Visibility {
    private static boolean readCommitted(TransactionManager tm,Transaction t,Entry e) {
        long xid = t.xid;
        long xmin = e.getXMIN();
        long xmax = e.getXMAX();
        if (xmin == xid && xmax == 0) return true;

        if (tm.isCommitted(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid && !tm.isCommitted(xmax)) return true;
        }
        return false;
    }

    private static boolean repeatableCommitted(TransactionManager tm,Transaction t,Entry e) {
        long xid = t.xid;
        long xmin = e.getXMIN();
        long xmax = e.getXMAX();
        if (xmin == xid && xmax == 0) return true;

        if (tm.isCommitted(xmin) && xmin<xid && !t.isInSnapshot(xmin)){
            if (xmax==0) return true;
            if (xmax!=xid && (xmax>xid || !tm.isCommitted(xmax) || t.isInSnapshot(xmax))) return true;
        }
        return false;
    }
}
