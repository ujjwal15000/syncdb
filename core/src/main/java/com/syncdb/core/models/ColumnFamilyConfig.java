package com.syncdb.core.models;

import lombok.Data;

@Data
public class ColumnFamilyConfig {
    private String name;
    private Integer ttl;

    private ColumnFamilyConfig(String name, Integer ttl){
        this.name = name;
        this.ttl = ttl;
    }

    public static ColumnFamilyConfig create(String name, Integer ttl){
        return new ColumnFamilyConfig(name, ttl);
    }
}
