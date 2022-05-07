package com.cheng.mydb.backend.dm.dataItem;

import com.cheng.mydb.backend.common.SubArray;
import com.cheng.mydb.backend.dm.page.Page;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();
}
