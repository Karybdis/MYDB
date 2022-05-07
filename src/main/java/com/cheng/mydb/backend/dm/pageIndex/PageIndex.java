package com.cheng.mydb.backend.dm.pageIndex;

import com.cheng.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    private static final byte INTERVALS_NO=40;                              // 一页分成40个区间
    private static final int THRESHOLD= PageCache.PAGE_SIZE/INTERVALS_NO;   // 每个区间大小

    private List<PageInfo>[] lists; // 第i(1开始)个区间内的page表示该page拥有的空闲空间为THRESHOLD*i-1<=freeSapce<THRESHOLD*i
    private Lock lock;

    public PageIndex() {
        lock=new ReentrantLock();
        lists=new List[INTERVALS_NO+1];
        for (int i=0;i<INTERVALS_NO+1;i++){
            lists[i]=new ArrayList<>();
        }
    }

    public void add(int pgno,int freeSpace){
        lock.lock();
        try{
            int index=freeSpace/THRESHOLD;
            lists[index].add(new PageInfo(pgno,freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize){
        lock.lock();
        try {
            int index = spaceSize / THRESHOLD;
            if (++index > INTERVALS_NO) return null; // 区间从1开始
            while (index <= INTERVALS_NO) {
                if (lists[index].size() == 0) {
                    index++;
                    continue;
                }
                return lists[index].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }


}
