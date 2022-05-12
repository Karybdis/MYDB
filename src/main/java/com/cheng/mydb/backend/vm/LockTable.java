package com.cheng.mydb.backend.vm;

import com.cheng.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // 某个UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待某个UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的某个XID的锁
    private Map<Long, Long> waitU;      // 某个XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid,long uid) throws Exception {
        lock.lock();
        try {
            if (isInList(x2u, xid, uid)) return null;
            if (!u2x.containsKey(uid)) {
                putIntoList(x2u, xid, uid);
                u2x.put(uid, xid);
                return null;
            }
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait,uid,xid);
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }


    // 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
    public void remove(long xid){
        lock.lock();
        try{
            List<Long> list=x2u.get(xid);
            if (list!=null){
                while(list.size()==0){
                    long uid=list.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> list=wait.get(uid);
        assert list.size()>0;
        while(list.size()>0){
            long xid=list.remove(0);
            if (!waitLock.containsKey(xid)) continue;
            else{
                putIntoList(x2u,xid,uid);
                u2x.put(uid,xid);
                waitU.remove(xid);
                Lock l=waitLock.get(xid);
                l.unlock();
                break;
            }
        }
        if (list.size()==0) wait.remove(uid);
    }


    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp=new HashMap<>();
        stamp=1;
        for (long xid:x2u.keySet()){
            Integer stp=xidStamp.get(xid);
            if (stp!=null && stp>0) continue;
            stamp++;
            if (dfs(xid)) return true;
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp=xidStamp.get(xid);
        if (stp!=null) return stp==stamp;
        xidStamp.put(xid,stamp);

        Long uid=waitU.get(xid);
        if (uid==null) return false;
        Long x=u2x.get(uid);
        assert x!=null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> map, long uid0, long uid1) {
        List<Long> list=map.get(uid0);
        if (list==null) return;
        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()){
            long e=iterator.next();
            if (e==uid1){
                list.remove(e);
                break;
            }
        }
        if (list.size()==0) map.remove(uid0);
    }

    private void putIntoList(Map<Long, List<Long>> map, long uid0, long uid1) {
        if (!map.containsKey(uid0)){
            map.put(uid0,new ArrayList<>());
        }
        map.get(uid0).add(0,uid1);
    }

    private boolean isInList(Map<Long,List<Long>> map,long uid0,long uid1){
        List<Long> list=map.get(uid0);
        if (list==null) return false;
        for (long e:list){
            if (e==uid1) return true;
        }
        return false;
    }
}
