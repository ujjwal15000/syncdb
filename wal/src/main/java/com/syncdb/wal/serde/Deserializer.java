package com.syncdb.wal.serde;

public interface Deserializer<T> {
    T deserializer(byte[] object);
}
