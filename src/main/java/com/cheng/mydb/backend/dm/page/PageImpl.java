package com.cheng.mydb.backend.dm.page;

import com.cheng.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;

public class PageImpl implements Page {
    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;

    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
    }

    public void lock() {

    }

    public void unlock() {

    }

    public void release() {

    }

    public void setDirty(boolean dirty) {
        this.dirty=dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return 0;
    }

    public byte[] getData() {
        return new byte[0];
    }
}
