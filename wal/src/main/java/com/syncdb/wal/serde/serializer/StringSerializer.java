package com.syncdb.wal.serde.serializer;

import com.syncdb.wal.serde.Serializer;

public class StringSerializer implements Serializer<String> {
    @Override
    public byte[] serializer(String object) {
        return object.getBytes();
    }
}
