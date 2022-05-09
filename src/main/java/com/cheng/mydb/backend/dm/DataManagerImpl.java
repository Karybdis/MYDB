package com.cheng.mydb.backend.dm;

import com.cheng.mydb.backend.common.AbstractCache;
import com.cheng.mydb.backend.dm.dataItem.DataItem;
import com.cheng.mydb.backend.dm.dataItem.DataItemImpl;
import com.cheng.mydb.backend.dm.logger.Logger;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.dm.page.PageOne;
import com.cheng.mydb.backend.dm.page.PageX;
import com.cheng.mydb.backend.dm.pageCache.PageCache;
import com.cheng.mydb.backend.dm.pageIndex.PageIndex;
import com.cheng.mydb.backend.dm.pageIndex.PageInfo;
import com.cheng.mydb.backend.tm.TransactionManager;
import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.backend.utils.Types;
import com.cheng.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;


    public DataManagerImpl(PageCache pc,Logger logger,TransactionManager tm) {
        super(0);
        this.pc=pc;
        this.logger=logger;
        this.tm=tm;
        this.pageIndex=new PageIndex();
    }


    // read() 根据 UID 从缓存中获取 DataItem，并校验有效位：
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di =(DataItemImpl) super.get(uid);
        if (!di.isValid()){
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw= DataItem.wrapDataItemRaw(data);
        if (raw.length>PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }

        PageInfo pageInfo=null;
        for (int i = 0; i < 5; i++) {
            pageInfo=pageIndex.select(raw.length);
            if (pageInfo!=null) break;
            else{   // 没有足够空间的页就新建一个
                int newPgno=pc.newPage(PageX.initRaw());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }

        if (pageInfo==null){
            throw Error.DatabaseBusyException;
        }

        Page page=null;
        int freeSpace=0;

        try{
            page=pc.getPage(pageInfo.pgno);
            // 首先做日志
            byte[] log=Recover.insertLog(xid,page,raw);
            logger.log(log);

            // 再执行插入操作
            short offset= PageX.insert(page,raw);
            page.release();
            return Types.addressToUid(pageInfo.pgno,offset);
        } finally {
            // 将取出的pg重新插入pIndex
            if (page!=null){
                pageIndex.add(pageInfo.pgno,PageX.getFreeSpace(page));
            }
            else{
                pageIndex.add(pageInfo.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset=(short)(uid & ((1L<<16)-1));
        uid>>>=32;
        int pgno=(int)(uid & ((1L<<32)-1));
        Page page=pc.getPage(pgno);
        return DataItem.parseDataItem(page,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno=pc.newPage(PageOne.initRaw());
        assert pgno==1;
        try {
            pageOne=pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne=pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    public void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }

    // 为xid生成update日志
    public void logDataItem(long xid,DataItem dataItem){
        byte[] log=Recover.updateLog(xid,dataItem);
        logger.log(log);
    }
}
