package com.cheng.mydb.backend.dm.dataItem;

import com.cheng.mydb.backend.common.SubArray;
import com.cheng.mydb.backend.dm.DataManager;
import com.cheng.mydb.backend.dm.DataManagerImpl;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.utils.Parser;
import com.cheng.mydb.backend.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

public interface DataItem {
    byte OFFSET_VALID=0;
    byte OFFSET_SIZE=1;
    byte OFFSET_DATA=3;

    public boolean isValid();
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page getPage();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid=new byte[1];
        byte[] size=Parser.short2Bytes((short) raw.length);
        return Bytes.concat(valid,size,raw);
    }

    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dm){
        byte[] raw=page.getData();
        short size= Parser.parseShort(Arrays.copyOfRange(raw,offset+OFFSET_SIZE,offset+OFFSET_DATA));
        short length=(short)(size+OFFSET_DATA);
        long uid= Types.addressToUid(page.getPageNumber(),offset);
        return new DataItemImpl(new SubArray(raw,offset,offset+length),new byte[length],page,uid,dm);
    }

    public static void setDataItemRawInvalid(byte[] raw){
        raw[OFFSET_VALID]=(byte)1;
    }
}
