package com.syncdb.stream.serde.deserializer;

import com.syncdb.stream.serde.Deserializer;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserializer(byte[] object) {
    return new String(object);
    }

}
