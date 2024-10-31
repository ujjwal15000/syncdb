package com.syncdb.stream.serde;

public interface Serializer<T> {
    byte[] serializer(T object);
}
