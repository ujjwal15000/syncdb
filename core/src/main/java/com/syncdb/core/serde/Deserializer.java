package com.syncdb.core.serde;

import java.nio.ByteBuffer;

public interface Deserializer<T> {
    T deserialize(byte[] object);
    T deserialize(ByteBuffer object);
}
