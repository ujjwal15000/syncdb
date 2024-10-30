package com.syncdb.wal.serde;

public interface Serializer<T> {
    byte[] serializer(T object);
}
