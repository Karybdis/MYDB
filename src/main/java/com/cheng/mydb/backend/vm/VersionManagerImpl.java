package com.cheng.mydb.backend.vm;

import com.cheng.mydb.backend.dm.DataManager;
import com.cheng.mydb.backend.tm.TransactionManager;

public class VersionManagerImpl implements VersionManager {
    TransactionManager tm;
    DataManager dm;
}
