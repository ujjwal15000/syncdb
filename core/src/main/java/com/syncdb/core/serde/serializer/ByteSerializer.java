package com.syncdb.core.serde.serializer;

import com.syncdb.core.serde.Serializer;

import java.io.Serializable;

public class ByteSerializer implements Serializer<byte[]>, Serializable {
    @Override
    public byte[] serializer(byte[] object) {
        return object;
    }
}
