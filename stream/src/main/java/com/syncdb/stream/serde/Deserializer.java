package com.syncdb.stream.serde;

public interface Deserializer<T> {
    T deserializer(byte[] object);
}
