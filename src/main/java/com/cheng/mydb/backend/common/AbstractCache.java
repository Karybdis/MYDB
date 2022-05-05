package com.cheng.mydb.backend.common;

import com.cheng.mydb.common.Error;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {

    private HashMap<Long,T> cache;              // 实际缓存的数据
    private HashMap<Long,Integer> references;   // 资源引用的个数
    private HashMap<Long,Boolean> getting;      // 资源正在被从数据源获取

    private Lock lock;
    private int maxResources;
    private int cnt=0;

    public AbstractCache(int maxResources) {
        this.maxResources=maxResources;
        cache=new HashMap<>();
        references=new HashMap<>();
        getting=new HashMap<>();
        lock=new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while(true){
            lock.lock();
            if (getting.containsKey(key)){  // 有其他线程正在从数据源中获取数据
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)){    // 缓存中有直接返回
                T obj=cache.get(key);
                references.put(key,references.get(key)+1);
                lock.unlock();
                return obj;
            }

            // 缓存中不存在，从数据源获取
            if (maxResources>0 && cnt==maxResources){   // 缓存资源量到达最大，无法继续添加
                lock.unlock();
                throw Error.CacheFullException;
            }
            cnt++;
            getting.put(key,true);
            lock.unlock();
            break;
        }

        // 从数据源获取
        T obj=null;
        try {
            obj=getForCache(key);
        } catch (Exception e) {
            lock.lock();
            cnt--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        lock.lock();
        getting.remove(key);
        cache.put(key,obj);
        references.put(key,1);
        lock.unlock();

        return obj;
    }

    // 释放一个资源
    protected void release(long key){
        lock.lock();
        try {
            int ref=references.get(key)-1;
            if (ref==0){
                T obj=cache.get(key);
                releaseForCache(obj);
                cnt--;
                cache.remove(key);
                references.remove(key);
            }
            else{
                references.put(key,ref);
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void close(){
        lock.lock();
        try {
            for (long key:cache.keySet()){
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
