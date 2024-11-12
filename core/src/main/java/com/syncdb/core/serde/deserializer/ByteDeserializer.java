package com.syncdb.core.serde.deserializer;

import com.syncdb.core.serde.Deserializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class ByteDeserializer implements Deserializer<byte[]>, Serializable {
    @Override
    public byte[] deserialize(byte[] object) {
        return object;
    }

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
        byte[] res = new byte[buffer.limit() - buffer.position()];
        buffer.get(buffer.position(), res);
        return res;
    }
}
