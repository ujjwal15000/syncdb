package com.syncdb.stream.serde.deserializer;

import com.syncdb.stream.serde.Deserializer;

public class ByteDeserializer implements Deserializer<byte[]> {
    @Override
    public byte[] deserializer(byte[] object) {
        return object;
    }

}
