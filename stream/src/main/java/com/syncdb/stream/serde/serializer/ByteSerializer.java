package com.syncdb.stream.serde.serializer;

import com.syncdb.stream.serde.Serializer;

public class ByteSerializer implements Serializer<byte[]> {
    @Override
    public byte[] serializer(byte[] object) {
        return object;
    }
}
