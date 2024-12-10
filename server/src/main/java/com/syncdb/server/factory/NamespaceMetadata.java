package com.syncdb.server.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NamespaceMetadata {
    private String name;
    private Integer numNodes;
    private Integer numPartitions;
    private Integer numReplicas;

    public static NamespaceMetadata create(String name, Integer numNodes, Integer numPartitions, Integer numReplicas){
        return new NamespaceMetadata(name, numNodes, numPartitions, numReplicas);
    }
}
