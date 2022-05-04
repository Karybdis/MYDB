package com.cheng.mydb.backend.tm;

import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.backend.utils.Parser;
import com.cheng.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {
    // XID文件头长度
    static final int XID_HEADER_LENGTH=8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE=1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE=0;
    private static final byte FIELD_TRAN_COMMITED=1;
    private static final byte FIELD_TRAN_ABORTED=2;
    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock lock;

    public static TransactionManagerImpl create(String path){
        File file=new File(path+XID_SUFFIX);
        try {
            if (!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()){
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

        ByteBuffer buf=ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf,fc);
    }

    public static TransactionManagerImpl open(String path){
        File file=new File(path+XID_SUFFIX);
        if (!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || !file.canWrite()){
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

        return new TransactionManagerImpl(raf,fc);
    }

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file=file;
        this.fc=fc;
        lock=new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter(){
        long fileLen=0;
        try {
            fileLen=file.length();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (fileLen<XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf=ByteBuffer.allocate(XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        xidCounter=Parser.parseLong(buf.array());
        long end=getXidPosition(xidCounter);
        if (end!=fileLen-1){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private long getXidPosition(long xid){
        return XID_HEADER_LENGTH+(xid-1)*XID_FIELD_SIZE;
    }

    public long begin() {
        lock.lock();
        try{
            long xid=xidCounter+1;
            updateXID(xid,FIELD_TRAN_ACTIVE);
            incXIDCounter();
            return xid;
        } finally {
            lock.unlock();;
        }
    }

    private void updateXID(long xid, byte status) {
        long offset=getXidPosition(xid);
        byte[] temp=new byte[XID_FIELD_SIZE];
        temp[0]=status;
        ByteBuffer buf=ByteBuffer.wrap(temp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void incXIDCounter() {
        xidCounter++;
        ByteBuffer buf=ByteBuffer.wrap(Parser.long2Bytes(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITED);
    }

    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    private boolean checkXIDStatus(long xid,byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buf=ByteBuffer.allocate(XID_FIELD_SIZE);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0]==status;
    }

    public boolean isActive(long xid) {
        if (xid==SUPER_XID) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if (xid==SUPER_XID) return true;
        return checkXIDStatus(xid,FIELD_TRAN_COMMITED);
    }

    public boolean isAborted(long xid) {
        if (xid==SUPER_XID) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
