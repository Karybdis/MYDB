package com.cheng.mydb.backend.vm;

import com.cheng.mydb.backend.tm.TransactionManager;

public class Visibility {
    public static boolean isVersionSkip(TransactionManager tm,Transaction t,Entry e){
        long xmax=e.getXMAX();
        if (t.level==0) return false;
        else return tm.isCommitted(xmax) && (xmax>t.xid || t.isInSnapshot(xmax));
    }

    public static boolean isVisible(TransactionManager tm,Transaction t,Entry e){
        if (t.level==0) return readCommitted(tm,t,e);
        else return repeatableCommitted(tm,t,e);
    }

    // 读已提交等级下判断是否可见
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

    // 可重复读等级下判断是否可见
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
