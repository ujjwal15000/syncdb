package com.syncdb.wal.serde.deserializer;

import com.syncdb.wal.serde.Deserializer;

public class ByteDeserializer implements Deserializer<byte[]> {
    @Override
    public byte[] deserializer(byte[] object) {
        return object;
    }

}
