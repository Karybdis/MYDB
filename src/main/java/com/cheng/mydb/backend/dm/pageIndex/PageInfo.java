package com.cheng.mydb.backend.dm.pageIndex;

// 页码和该页剩余空间的一个简单封装
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
