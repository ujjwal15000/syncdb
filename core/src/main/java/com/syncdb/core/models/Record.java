package com.syncdb.core.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.core.serde.Serializer;
import lombok.*;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Record<K, V> implements Serializable {
    @JsonIgnore
    public static final Record<Object, Object> EMPTY_RECORD = Record.builder().build();

    private K key;
    private V value;

    @SneakyThrows
    public static <K, V> byte[] serialize(Record<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return serialize(record.key, record.value, keySerializer, valueSerializer);
    }

    @SneakyThrows
    public static <K, V> byte[] serialize(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        byte[] keyBuf = keySerializer.serialize(key);
        byte[] valueBuf = valueSerializer.serialize(value);
        return serialize(keyBuf, valueBuf);
    }

    @SneakyThrows
    public static <K, V> byte[] serialize(byte[] key, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + key.length + 4 + value.length);

        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        return buffer.array();
    }

    @SneakyThrows
    public static <K, V> Record<K, V> deserialize(byte[] record, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        ByteBuffer buffer = ByteBuffer.wrap(record);

        int keyLen = buffer.getInt();
        ByteBuffer keySlice = buffer.slice(buffer.position(), keyLen);
        buffer.position(buffer.position() + keyLen);
        K key = keyDeserializer.deserialize(keySlice);

        int valueLen = buffer.getInt();
        ByteBuffer valueSlice = buffer.slice(buffer.position(), valueLen);
        buffer.position(buffer.position() + valueLen);
        V value = valueDeserializer.deserialize(valueSlice);

        return Record.<K, V>builder()
                .key(key)
                .value(value)
                .build();
    }
}
