package com.cheng.mydb.backend.vm;

import com.cheng.mydb.backend.common.AbstractCache;
import com.cheng.mydb.backend.dm.DataManager;
import com.cheng.mydb.backend.tm.TransactionManager;
import com.cheng.mydb.backend.tm.TransactionManagerImpl;
import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    TransactionManager tm;
    DataManager dm;
    Lock lock;
    Map<Long,Transaction> activateTransaction;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        lock = new ReentrantLock();
        lt = new LockTable();
        activateTransaction = new HashMap<>();
        activateTransaction.put((long)TransactionManagerImpl.SUPER_XID,
                Transaction.newTransaction(TransactionManagerImpl.SUPER_XID,0,null));
    }

    // read() 方法读取一个 entry，注意判断下可见性即可：
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activateTransaction.get(xid);
        lock.unlock();
        if (t.err!=null){
            throw t.err;
        }
        Entry entry = super.get(uid);
        try{
            if (Visibility.isVisible(tm,t,entry)){
                return entry.data();
            }
            else return null;
        } finally {
            entry.release();
        }
    }


    // insert() 则是将数据包裹成 Entry，无脑交给 DM 插入即可：
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t=activateTransaction.get(xid);
        lock.unlock();
        if (t.err!=null){
            throw t.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid,data);
    }


    // 一是可见性判断，二是获取资源的锁，三是版本跳跃判断。删除的操作只有一个设置 XMAX。
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activateTransaction.get(xid);
        lock.unlock();
        if (t.err!=null){
            throw t.err;
        }
        Entry entry = null;
        try{
            entry = super.get(uid);
        } catch (Exception e){
            if (e== Error.NullEntryException) return false;
            else throw e;
        }
        try{
            if (!Visibility.isVisible(tm,t,entry)) return false;
            Lock l=null;
            try{
                l=lt.add(xid,uid);
            } catch (Exception e){
                t.err= Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted=true;
                throw t.err;
            }
            if (l!=null){
                l.lock();
                l.unlock();
            }
            if (entry.getXMAX()==xid){
                return false;
            }
            if (Visibility.isVersionSkip(tm,t,entry)){
                t.err= Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted=true;
                throw t.err;
            }
            entry.setXMAX(xid);
            return true;
        } finally {
            entry.release();
        }
    }


    // begin() 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用：
    @Override
    public long begin(int level) {
        lock.lock();
        try{
            long xid=tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activateTransaction);
            activateTransaction.put(xid,t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    // commit() 方法提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态：
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t=activateTransaction.get(xid);
        lock.unlock();
        try{
            if (t.err!=null){
                throw t.err;
            }
        } catch (NullPointerException n){
            System.out.println(xid);
            System.out.println(activateTransaction.keySet());
            Panic.panic(n);
        }
        lock.lock();
        activateTransaction.remove(xid);
        lock.unlock();
        lt.remove(xid);
        tm.commit(xid);
    }

    // abort 事务的方法则有两种，手动和自动。手动指的是调用 abort() 方法，
    // 而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activateTransaction.get(xid);
        if (!autoAborted){
            activateTransaction.remove(xid);
        }
        lock.unlock();
        if (t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseForEntry(Entry entry){
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry=Entry.loadEntry(this,uid);
        if (entry==null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
