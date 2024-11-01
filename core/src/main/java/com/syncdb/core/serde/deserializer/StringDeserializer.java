package com.syncdb.core.serde.deserializer;

import com.syncdb.core.serde.Deserializer;

import java.io.Serializable;

public class StringDeserializer implements Deserializer<String>, Serializable {
    @Override
    public String deserializer(byte[] object) {
    return new String(object);
    }

}
