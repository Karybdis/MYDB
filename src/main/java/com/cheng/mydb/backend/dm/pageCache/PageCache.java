package com.cheng.mydb.backend.dm.pageCache;

import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE=1<<13; // 该项目中设置一页=8k
    public static final String DB_SUFFIX = ".db";

    int newPage(byte[] initData);               // 新建页
    Page getPage(int pgno) throws Exception;    // 根据页码获取页
    void close();                               // 关闭页面缓存
    void release(Page page);                    // 释放该页
    void truncateByBgno(int maxPgno);
    int getPageNumber();                        // 获取页码
    void flushPage(Page page);                  // 把该页刷回数据源

    public static PageCacheImpl create(String path,long memory){
        File file=new File(path+DB_SUFFIX);
        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf=null;
        FileChannel fc=null;
        try {
            raf=new RandomAccessFile(file,"rw");
            fc=raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path,long memory){
        File file = new File(path+DB_SUFFIX);
        if(!file.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf=null;
        FileChannel fc=null;
        try {
            raf=new RandomAccessFile(file,"rw");
            fc=raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }
}
