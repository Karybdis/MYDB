package com.cheng.mydb.backend.dm.dataItem;

import com.cheng.mydb.backend.common.SubArray;
import com.cheng.mydb.backend.dm.DataManager;
import com.cheng.mydb.backend.dm.DataManagerImpl;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {
    private SubArray raw;
    private byte[] oldRaw;
    private Lock rlock;
    private Lock wlock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page,long uid,DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.page = page;
        ReadWriteLock lock=new ReentrantReadWriteLock();
        rlock=lock.readLock();
        wlock=lock.writeLock();
    }

    public boolean isValid(){
        return raw.raw[raw.start+OFFSET_VALID]==(byte)0;
    }

    public SubArray data() {
        return new SubArray(raw.raw,raw.start+OFFSET_DATA,raw.end);
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：在修改之前需要调用 before() 方法，想要撤销修改时，
     * 调用 unBefore() 方法，在修改完成后，调用 after() 方法。整个流程，主要是为了保存前相数据，并及时落日志。
     * DM 会保证对 DataItem 的修改是原子性的。
     */
    public void before() {
        wlock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length);
    }

    public void unBefore() {
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
        wlock.unlock();
    }

    public void after(long xid) {

    }

    public void release() {

    }

    public void lock() {

    }

    public void unlock() {

    }

    public void rLock() {

    }

    public void rUnLock() {

    }

    public Page page() {
        return this.page;
    }

    public long getUid() {
        return this.uid;
    }

    public byte[] getOldRaw() {
        return this.oldRaw;
    }

    public SubArray getRaw() {
        return this.raw;
    }
}
