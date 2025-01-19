package com.syncdb.core.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.rocksdb.RocksDB;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NamespaceRecord {
    private String namespace;
    private String bucket = new String(RocksDB.DEFAULT_COLUMN_FAMILY);
    private String key;
    private String value;
}
