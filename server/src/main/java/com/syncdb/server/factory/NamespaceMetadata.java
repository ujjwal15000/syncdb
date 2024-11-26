package com.syncdb.server.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import lombok.Data;

@Data
public class NamespaceMetadata {
    private final String name;
    private final Integer numPartitions;

    public static NamespaceMetadata create(String name, Integer numPartitions){
        return new NamespaceMetadata(name, numPartitions);
    }
}
