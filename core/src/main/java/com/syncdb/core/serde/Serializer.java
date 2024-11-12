package com.syncdb.core.serde;

public interface Serializer<T> {
    byte[] serialize(T object);
}
