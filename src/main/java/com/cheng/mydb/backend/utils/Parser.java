package com.cheng.mydb.backend.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static byte[] long2Bytes(long value){
        return ByteBuffer.allocate(Long.SIZE/Byte.SIZE).putLong(value).array();
    }

    public static long parseLong(byte[] buf){
        return ByteBuffer.wrap(buf,0,8).getLong();
    }
}
