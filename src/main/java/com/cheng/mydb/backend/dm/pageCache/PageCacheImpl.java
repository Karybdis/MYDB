package com.cheng.mydb.backend.dm.pageCache;

import com.cheng.mydb.backend.common.AbstractCache;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.dm.page.PageImpl;
import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private Lock lock;
    private RandomAccessFile file;
    private FileChannel fc;

    private AtomicInteger pageNumbers;

    public PageCacheImpl(RandomAccessFile file,FileChannel fc,int maxResources) {
        super(maxResources);
        if(maxResources < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        lock=new ReentrantLock();
        this.file=file;
        this.fc=fc;
        pageNumbers=new AtomicInteger();
    }

    // 当资源不在缓存时，从数据源获取
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno=(int) key;
        long offset=pageOffset(pgno);

        ByteBuffer buf=ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }
        catch (IOException e){
            Panic.panic(e);
        }
        lock.unlock();
        return new PageImpl(pgno,buf.array(),this);
    }

    // 对应页的偏移量，页码从1开始
    private long pageOffset(int pgno) {
        return (pgno-1)*PAGE_SIZE;
    }


    // 当资源被驱逐时，如果是脏页，刷回数据源
    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    // 刷回数据源
    private void flush(Page page) {
        int pgno=page.getPageNumber();
        long offset = pageOffset(pgno);

        lock.lock();
        try {
            ByteBuffer buf=ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

    }

    public int newPage(byte[] initData) {
        int pgno=pageNumbers.incrementAndGet();
        Page page=new PageImpl(pgno,initData,null);
        flush(page);
        return pgno;
    }

    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    public void close() {
        super.close();
        try{
            file.close();
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public void release(Page page) {
        release(page.getPageNumber());
    }

    public void truncateByBgno(int maxPgno) {

    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    public void flushPage(Page page) {
        flush(page);
    }
}
