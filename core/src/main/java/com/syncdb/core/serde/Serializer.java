package com.syncdb.core.serde;

public interface Serializer<T> {
    byte[] serializer(T object);
}
