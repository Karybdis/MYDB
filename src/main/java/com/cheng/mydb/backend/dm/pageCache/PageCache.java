package com.cheng.mydb.backend.dm.pageCache;

import com.cheng.mydb.backend.dm.page.Page;

public interface PageCache {
    public static final int PAGE_SIZE=1<<13; // 该项目中设置一页=8k

    int newPage(byte[] initData);               // 新建页
    Page getPage(int pgno) throws Exception;    // 根据页码获取页
    void close();                               // 关闭页面缓存
    void release(Page page);                    // 释放该页
    void truncateByBgno(int maxPgno);
    int getPageNumber();                        // 获取页码
    void flushPage(Page page);                  // 把该页刷回数据源
}
