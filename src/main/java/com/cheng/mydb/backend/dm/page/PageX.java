package com.cheng.mydb.backend.dm.page;

import com.cheng.mydb.backend.dm.pageCache.PageCache;
import com.cheng.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页的工具类
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始的偏移量
 */
public class PageX {
    private static final byte OFFSET_FREE=0;
    private static final byte OFFSET_DATA=2;
    public static final int MAX_FREE_SPACE= PageCache.PAGE_SIZE-OFFSET_DATA;

    // 返回一个初始化的普通页数据
    public static byte[] initRaw(){
        byte[] raw=new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OFFSET_DATA);
        return raw;
    }

    // 页数据的前两个字节写入新的FSO
    private static void setFSO(byte[] raw, short FSO) {
        System.arraycopy(Parser.short2Bytes(FSO),0,raw,OFFSET_FREE,OFFSET_DATA);
    }

    // 获取page的FSO
    public static short getFSO(Page page){
        return getFSO(page.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    // 将raw插入pg中，返回插入位置
    public static short insert(Page page,byte[] raw){
        page.setDirty(true);
        short FSO = getFSO(page.getData());
        System.arraycopy(raw,0,page.getData(),FSO,raw.length);
        setFSO(page.getData(), (short) (FSO+raw.length));
        return FSO;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE-(int)getFSO(page.getData());
    }

    // 将raw插入page中的offset位置，并将page的FSO设置为较大的FSO
    public static void recoverInsert(Page page,byte[] raw,short offset){
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);

        short FSO=getFSO(page.getData());
        if (FSO<offset+raw.length){
            setFSO(page.getData(), (short) (offset+raw.length));
        }
    }

    // 将raw插入page中的offset位置，不更新update
    public static void recoverUpdate(Page page,byte[] raw,short offset){
        page.setDirty(true);
        System.arraycopy(raw,0,page.getData(),offset,raw.length);
    }
}
