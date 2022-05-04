package com.cheng.mydb.backend.utils;

public class Panic {
    public static void panic(Exception ex){
        ex.printStackTrace();
        System.exit(1);
    }
}
