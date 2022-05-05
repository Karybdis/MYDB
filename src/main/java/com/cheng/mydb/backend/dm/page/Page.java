package com.cheng.mydb.backend.dm.page;

public interface Page {
    void lock();                    // 上页锁
    void unlock();                  // 解锁
    void release();                 // 释放该页
    void setDirty(boolean dirty);   // 设置脏页
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}