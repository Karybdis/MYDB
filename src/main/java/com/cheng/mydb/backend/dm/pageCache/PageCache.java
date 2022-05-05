package com.cheng.mydb.backend.dm.pageCache;

import com.cheng.mydb.backend.dm.page.Page;

public interface PageCache {
    public static final int PAGE_SIZE=1<<13; // 8k

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
}
