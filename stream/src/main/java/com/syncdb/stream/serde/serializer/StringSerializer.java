package com.syncdb.stream.serde.serializer;

import com.syncdb.stream.serde.Serializer;

public class StringSerializer implements Serializer<String> {
    @Override
    public byte[] serializer(String object) {
        return object.getBytes();
    }
}
