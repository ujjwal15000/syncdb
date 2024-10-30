package com.syncdb.wal.serde.deserializer;

import com.syncdb.wal.serde.Deserializer;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserializer(byte[] object) {
    return new String(object);
    }

}
