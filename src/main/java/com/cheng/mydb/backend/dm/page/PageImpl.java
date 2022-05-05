package com.cheng.mydb.backend.dm.page;

import com.cheng.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {
    private int pageNumber;         // 这个页面的页号，该页号从1开始
    private byte[] data;            // 这个页实际包含的字节数据
    private PageCache pageCache;

    private boolean dirty;
    private Lock lock;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        this.lock=new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pageCache.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty=dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }
}
