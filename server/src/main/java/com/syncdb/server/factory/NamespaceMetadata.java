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
    private Integer numPartitions;

    public static NamespaceMetadata create(String name, Integer numPartitions){
        return new NamespaceMetadata(name, numPartitions);
    }
}
