package com.cheng.mydb.backend.dm.logger;

import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.backend.utils.Parser;
import com.cheng.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 管理日志文件读写
 *
 * 日志文件标准格式为：
 * [XCheckSum] [Log1] [Log2] ... [LogN] [BadTail]
 * XCheckSum 为后续所有日志计算的CheckSum，4字节int类型
 *
 * 每条正确日志的格式为：
 * [Size] [CheckSum] [Data]
 * Size 4字节int 标识此Log中Data长度
 * CheckSum 4字节int
 * Data: [LogType] [XID] [UID] [OldRaw] [NewRaw](Update) 或者 [LogType] [XID] [Pgno] [Offset] [Raw](Insert)
 */
public class LoggerImpl implements Logger {

    private static final int SEED=13331;

    private static final byte OFFSET_SIZE=0;                     // 0-3字节为Size
    private static final byte OFFSET_CheckSum=OFFSET_SIZE+4;     // 4-7字节为CheckSum
    private static final byte OFFSET_DATA=OFFSET_CheckSum+4;     // 8字节开始为日志数据

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int XCheckSum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc){
        this.file=raf;
        this.fc=fc;
        lock=new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int XCheckSum) {
        this.file = raf;
        this.fc = fc;
        this.XCheckSum = XCheckSum;
        lock = new ReentrantLock();
    }

    void init(){
        long size=0;
        try {
            size=file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size<4){
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw=ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.write(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int XCheckSum=Parser.parseInt(raw.array());
        this.fileSize=size;
        this.XCheckSum=XCheckSum;

        checkAndRemoveTail();
    }


    // 在打开一个日志文件时,检查XCheckSum并移除bad tail
    private void checkAndRemoveTail(){
        rewind();

        int xCheck=0;
        while(true){
            byte[] log = internNext();
            if (log==null) break;
            xCheck=calCheckSum(xCheck,log);
        }

        if (xCheck!=XCheckSum){
            Panic.panic(Error.BadLogFileException);
        }

        try {
            // 截断文件到正常日志的末尾
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    // 计算log的CheckSum
    private int calCheckSum(int xCheck,byte[] data){
        for (byte b:data){
            xCheck=xCheck*SEED+b;
        }
        return xCheck;
    }

    // 从文件中获取下一个log
    private byte[] internNext(){
        if (position+OFFSET_DATA>=fileSize){
            return null;
        }
        // 读取size
        ByteBuffer temp=ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(temp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size= Parser.parseInt(temp.array());
        if (position+OFFSET_DATA+size>fileSize){
            return null;
        }

        // 读取整个Log
        ByteBuffer buf=ByteBuffer.allocate(OFFSET_DATA+size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log=buf.array();
        int CheckSum1=calCheckSum(0, Arrays.copyOfRange(log,OFFSET_DATA,log.length));
        int CheckSum2=Parser.parseInt(Arrays.copyOfRange(log,OFFSET_CheckSum,OFFSET_DATA));
        if (CheckSum1!=CheckSum2){
            return null;
        }
        position+=log.length;
        return log;
    }

    // 生成log并写入日志文件
    public void log(byte[] data) {
        byte[] log=wraplog(data);
        ByteBuffer buf=ByteBuffer.wrap(log);
        lock.lock();
        try{
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    // 更新XCheckSum
    private void updateXCheckSum(byte[] log) {
        int XCheckSum=calCheckSum(this.XCheckSum,log);
        ByteBuffer buf=ByteBuffer.wrap(Parser.int2Bytes(XCheckSum));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }


    // 生成log
    private byte[] wraplog(byte[] data) {
        byte[] size=Parser.int2Bytes(data.length);
        byte[] CheckSum=Parser.int2Bytes(calCheckSum(0,data));
        byte[] log=new byte[CheckSum.length+size.length+data.length];
        System.arraycopy(size,0,log,0,size.length);
        System.arraycopy(CheckSum,0,log,size.length,CheckSum.length);
        System.arraycopy(data,0,log,size.length+CheckSum.length,data.length);
        return log;
    }

    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    // 获取下一个log的data
    public byte[] next() {
        lock.lock();
        try{
            byte[] log = internNext();
            return Arrays.copyOfRange(log,OFFSET_DATA,log.length);
        } finally {
            lock.unlock();
        }
    }

    // 指针重新指向第一个log的位置
    public void rewind() {
        position=4;
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
