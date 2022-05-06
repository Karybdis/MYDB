package com.cheng.mydb.backend.dm;

import com.cheng.mydb.backend.dm.logger.Logger;
import com.cheng.mydb.backend.dm.page.Page;
import com.cheng.mydb.backend.dm.page.PageX;
import com.cheng.mydb.backend.dm.pageCache.PageCache;
import com.cheng.mydb.backend.tm.TransactionManager;
import com.cheng.mydb.backend.utils.Panic;
import com.cheng.mydb.backend.utils.Parser;
import sun.jvm.hotspot.runtime.Bytes;

import java.util.*;

//　这里的log指的是LoggerImpl中提到的log的data
public class Recover {

    private static final byte LOG_TYPE_INSERT=0;
    private static final byte LOG_TYPE_UPDATE=1;

    private static final byte REDO=0;
    private static final byte UNDO=1;

    static class InsertLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    // 执行redo
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    // 执行undo
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OFFSET_TYPE = 0;                           // 0字节      TYPE
    private static final int OFFSET_XID = OFFSET_TYPE+1;                // 1-8字节    XID
    private static final int OFFSET_UPDATE_UID = OFFSET_XID+8;          // 9-16字节   UID
    private static final int OFFSET_UPDATE_RAW = OFFSET_UPDATE_UID+8;   // 17字节开始  RAW

    // 依据log生成UpdateLogInfo对象
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo li=new UpdateLogInfo();
        li.xid= Parser.parseLong(Arrays.copyOfRange(log,OFFSET_XID,OFFSET_UPDATE_UID));
        //　完全不知道为什么要加这个UID，不能用PGNO和OFFSET代替吗
        long uid= Parser.parseLong(Arrays.copyOfRange(log,OFFSET_UPDATE_UID,OFFSET_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OFFSET_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW, OFFSET_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW+length, OFFSET_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OFFSET_INSERT_PGNO = OFFSET_XID+8;             // 9-12字节   PGNO
    private static final int OFFSET_INSERT_OFFSET = OFFSET_INSERT_PGNO+4;   // 13-14字节  OFFSET
    private static final int OFFSET_INSERT_RAW = OFFSET_INSERT_OFFSET+2;    // 15字节开始　RAW

    // 依据log生成InsertLogInfo对象
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OFFSET_INSERT_PGNO, OFFSET_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OFFSET_INSERT_OFFSET, OFFSET_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OFFSET_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
//                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}