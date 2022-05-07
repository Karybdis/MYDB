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
    static final byte OFFSET_VALID=0;
    static final byte OFFSET_SIZE=1;
    static final byte OFFSET_DATA=3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rlock;
    private Lock wlock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, DataManagerImpl dm, long uid, Page page) {
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

    public void before() {

    }

    public void unBefore() {

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
        return null;
    }

    public long getUid() {
        return 0;
    }

    public byte[] getOldRaw() {
        return new byte[0];
    }

    public SubArray getRaw() {
        return null;
    }
}
