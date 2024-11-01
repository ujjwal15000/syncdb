package com.syncdb.core.serde;

public interface Deserializer<T> {
    T deserializer(byte[] object);
}
