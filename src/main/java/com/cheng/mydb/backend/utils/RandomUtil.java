package com.cheng.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    public static byte[] randomBytes(int len){
        Random random=new SecureRandom();
        byte[] res=new byte[len];
        random.nextBytes(res);
        return res;
    }
}
