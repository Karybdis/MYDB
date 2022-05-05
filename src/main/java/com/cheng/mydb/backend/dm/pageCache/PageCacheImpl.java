package com.cheng.mydb.backend.dm.pageCache;

import com.cheng.mydb.backend.common.AbstractCache;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.dm.page.PageImpl;
import com.cheng.mydb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private Lock lock;
    private RandomAccessFile file;
    private FileChannel fc;

    public PageCacheImpl(RandomAccessFile file,FileChannel fc,int maxResources) {
        super(maxResources);
        lock=new ReentrantLock();
        this.file=file;
        this.fc=fc;
    }

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

    private long pageOffset(int pgno) {
        return (pgno-1)*PAGE_SIZE;
    }

    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()){
            flush(page);
            page.setDirty(false);
        }
    }

    private void flush(Page page) {
        int pgno=page.getPageNumber();
        long offset = pageOffset(pgno);

        lock.lock();
        try {
            ByteBuffer buf=ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false;);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }

    }

    public int newPage(byte[] initData) {
        return 0;
    }

    public Page getPage(int pgno) throws Exception {
        return null;
    }

    public void close() {

    }

    public void release(Page page) {

    }

    public void truncateByBgno(int maxPgno) {

    }

    public int getPageNumber() {
        return 0;
    }

    public void flushPage(Page pg) {

    }
}
