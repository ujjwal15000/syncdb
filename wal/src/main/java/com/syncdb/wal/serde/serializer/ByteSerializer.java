package com.syncdb.wal.serde.serializer;

import com.syncdb.wal.serde.Serializer;

public class ByteSerializer implements Serializer<byte[]> {
    @Override
    public byte[] serializer(byte[] object) {
        return object;
    }
}
