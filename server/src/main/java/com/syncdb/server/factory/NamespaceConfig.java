package com.syncdb.server.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import lombok.Data;

@Data
public class NamespaceConfig {
    private final String name;
    private final Integer numPartitions;
    private final Integer numReplicas;
    private final Murmur3Partitioner partitioner;

    NamespaceConfig(String name, Integer numPartitions, Integer numReplicas){
        this.name = name;
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.partitioner = new Murmur3Partitioner(numPartitions);
    }

    public static NamespaceConfig create(String name, Integer numPartitions, Integer numReplicas){
        return new NamespaceConfig(name, numPartitions, numReplicas);
    }
}
