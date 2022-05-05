package com.cheng.mydb.backend.dm.page;

import com.cheng.mydb.backend.dm.pageCache.PageCache;
import com.cheng.mydb.backend.utils.RandomUtil;
import com.sun.scenario.effect.Offset;

import java.util.Arrays;

/**
 * PageOne特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OFFSET_VC=100; // VC偏移量
    private static final int LEN_VC=8;      // VC长8字节

    public static byte[] initRaw(){
        byte[] raw=new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page){
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    // db启动时给100~107字节处填入一个随机字节
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OFFSET_VC,LEN_VC);
    }

    public static void setVcClose(Page page){
        page.setDirty(true);
        setVcClose(page.getData());
    }

    // db关闭时将100~107字节拷贝到108~115字节
    private static void setVcClose(byte[] raw){
        System.arraycopy(raw,OFFSET_VC,raw,OFFSET_VC+LEN_VC,LEN_VC);
    }

    public static boolean checkVc(Page page){
        return checkVc(page.getData());
    }

    //　比较100~107字节和108~115字节是否相等
    private static boolean checkVc(byte[] raw){
        return Arrays.equals(Arrays.copyOfRange(raw,OFFSET_VC,LEN_VC),
                Arrays.copyOfRange(raw,OFFSET_VC+LEN_VC, OFFSET_VC+LEN_VC*2));
    }
}
