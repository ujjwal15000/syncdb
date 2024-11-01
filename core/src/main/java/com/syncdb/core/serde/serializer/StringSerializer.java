package com.syncdb.core.serde.serializer;

import com.syncdb.core.serde.Serializer;

import java.io.Serializable;

public class StringSerializer implements Serializer<String>, Serializable {
    @Override
    public byte[] serializer(String object) {
        return object.getBytes();
    }
}
