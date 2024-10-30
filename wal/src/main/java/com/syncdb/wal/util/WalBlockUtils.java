package com.syncdb.wal.util;

import lombok.extern.slf4j.Slf4j;

import static com.syncdb.wal.constant.Constants.WAL_BLOCK_PREFIX;
import static com.syncdb.wal.constant.Constants.WAL_PATH;

@Slf4j
public class WalBlockUtils {
    public static String getBlockName(String rootPath, Integer i){
        return rootPath + WAL_PATH + WAL_BLOCK_PREFIX + i.toString();
    }
}
