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

/**
 * TM用来维护一个 XID 格式的文件
 * 前8个字节为头，记录这个 XID 文件管理的事务的个数，每个事务状态占1字节
 */
public class TransactionManagerImpl implements TransactionManager {
    // XID文件头长度
    static final byte XID_HEADER_LENGTH=8;
    // 每个事务的占用长度
    private static final byte XID_FIELD_SIZE=1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE=0;
    private static final byte FIELD_TRAN_COMMITED=1;
    private static final byte FIELD_TRAN_ABORTED=2;
    // 超级事务，永远为commited状态
    public static final byte SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock lock;

    // 创建一个 XID 文件并创建 TM
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

    // 从一个已有的 XID 文件来创建 TM
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

    // 检验XID文件中事务数量是否正常
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

    //　根据事务ID获取其在XID文件中的偏移量
    private long getXidPosition(long xid){
        // 0-7字节为头部，xid=1从第8字节开始（xid=0不计算在内）
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

    // 更新XID文件中事务状态
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

    // 事务计数加1，同时修改XID文件的头
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
