package com.cheng.mydb.backend.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static byte[] int2Bytes(long value){
        return ByteBuffer.allocate(Integer.SIZE/Byte.SIZE).putLong(value).array();
    }

    public static byte[] long2Bytes(long value){
        return ByteBuffer.allocate(Long.SIZE/Byte.SIZE).putLong(value).array();
    }

    public static byte[] short2Bytes(short value){
        return ByteBuffer.allocate(Short.SIZE/Byte.SIZE).putShort(value).array();
    }

    public static int parseInt(byte[] buf){
        return ByteBuffer.wrap(buf,0,4).getInt();
    }

    public static long parseLong(byte[] buf){
        return ByteBuffer.wrap(buf,0,8).getLong();
    }

    public static short parseShort(byte[] buf) {
        return ByteBuffer.wrap(buf,0,2).getShort();
    }
}
