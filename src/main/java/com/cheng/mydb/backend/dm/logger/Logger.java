package com.cheng.mydb.backend.dm.logger;

import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.common.Error;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface Logger {

    public static final String LOG_SUFFIX=".log";

    void log(byte[] data);                      // 生成log并写入日志文件
    void truncate(long x) throws Exception;     // 在指定位置截断文件
    byte[] next();                              // 获取下一个log的data
    void rewind();                              // 指针重新指向第一个log的位置
    void close();

//    public static Logger create(String path) {
//        File f = new File(path+LoggerImpl.LOG_SUFFIX);
//        try {
//            if(!f.createNewFile()) {
//                Panic.panic(Error.FileExistsException);
//            }
//        } catch (Exception e) {
//            Panic.panic(e);
//        }
//        if(!f.canRead() || !f.canWrite()) {
//            Panic.panic(Error.FileCannotRWException);
//        }
//
//        FileChannel fc = null;
//        RandomAccessFile raf = null;
//        try {
//            raf = new RandomAccessFile(f, "rw");
//            fc = raf.getChannel();
//        } catch (FileNotFoundException e) {
//            Panic.panic(e);
//        }
//
//        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
//        try {
//            fc.position(0);
//            fc.write(buf);
//            fc.force(false);
//        } catch (IOException e) {
//            Panic.panic(e);
//        }
//
//        return new LoggerImpl(raf, fc, 0);
//    }
//
//    public static Logger open(String path) {
//        File f = new File(path+LoggerImpl.LOG_SUFFIX);
//        if(!f.exists()) {
//            Panic.panic(Error.FileNotExistsException);
//        }
//        if(!f.canRead() || !f.canWrite()) {
//            Panic.panic(Error.FileCannotRWException);
//        }
//
//        FileChannel fc = null;
//        RandomAccessFile raf = null;
//        try {
//            raf = new RandomAccessFile(f, "rw");
//            fc = raf.getChannel();
//        } catch (Exception e) {
//            Panic.panic(e);
//        }
//
//        LoggerImpl lg = new LoggerImpl(raf, fc);
//        lg.init();
//
//        return lg;
//    }
}
