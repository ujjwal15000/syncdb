package com.syncdb.core.partitioner;

import org.apache.commons.codec.digest.MurmurHash3;

import java.io.Serializable;

public class Murmur3Partitioner implements Serializable {
    private final int numPartitions;

    public Murmur3Partitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getPartition(byte[] key) {
        long[] hash = MurmurHash3.hash128(key);

        long combinedHash = hash[0] ^ hash[1];
        return (int) (Math.abs(combinedHash) % numPartitions);
    }
}
