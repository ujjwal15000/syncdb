package com.syncdb.core.serde.deserializer;

import com.syncdb.core.serde.Deserializer;

import java.io.Serializable;

public class ByteDeserializer implements Deserializer<byte[]>, Serializable {
    @Override
    public byte[] deserializer(byte[] object) {
        return object;
    }

}
