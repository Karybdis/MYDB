package com.cheng.mydb.backend.vm;

import com.cheng.mydb.backend.common.SubArray;
import com.cheng.mydb.backend.dm.dataItem.DataItem;
import com.cheng.mydb.backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构：
 * [XMIN] [XMAX] [data]
 * XMIN 是创建该条记录（版本）的事务编号，而 XMAX 则是删除该条记录（版本）的事务编号。
 * XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
 * DATA 就是这条记录持有的数据
 */
public class Entry {
    private static final byte OFFSET_XMIN=0;
    private static final byte OFFSET_XMAX=OFFSET_XMIN+8;
    private static final byte OFFSET_DATA=OFFSET_XMAX+8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm,DataItem dataItem,long uid){
        Entry entry = new Entry();
        entry.uid=uid;
        entry.dataItem=dataItem;
        entry.vm=vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm,long uid) throws Exception {
        DataItem di=((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm,di,uid);
    }

    public static byte[] wrapEntryRaw(long xid,byte[] data){
        byte[] XMIN= Parser.long2Bytes(xid);
        byte[] XMAX=new byte[8];
        return Bytes.concat(XMIN,XMAX,data);
    }

    public void remove(){
        dataItem.release();
    }

    // 以拷贝的形式返回内容
    public byte[] data(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            byte[] data=new byte[sa.end-sa.start-OFFSET_DATA];
            System.arraycopy(sa.raw,sa.start+OFFSET_DATA,data,0,data.length);
            return data;
        }
        finally {
            dataItem.rUnLock();
        }
    }

    public long getXMIN(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OFFSET_XMIN,sa.start+OFFSET_XMAX));
        }
        finally {
            dataItem.rUnLock();
        }
    }

    public long getXMAX(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start+OFFSET_XMAX,sa.start+OFFSET_DATA));
        }
        finally {
            dataItem.rUnLock();
        }
    }

    public void setXMAX(long xid){
        dataItem.before();
        try{
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Bytes(xid),0,sa.raw,sa.start+OFFSET_XMAX,8);
        }
        finally {
            dataItem.after(xid);
        }
    }

    public long getUid(){
        return uid;
    }
}
