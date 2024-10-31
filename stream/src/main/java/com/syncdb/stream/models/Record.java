package com.syncdb.stream.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.syncdb.stream.serde.Deserializer;
import com.syncdb.stream.serde.Serializer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Record<K, V> {
    @JsonIgnore
    public static final Record<Object, Object> EMPTY_RECORD = Record.builder().build();

    private K key;
    private V value;

    @SneakyThrows
    public static <K, V> byte[] serialize(Record<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer, ObjectMapper objectMapper) {
        return objectMapper.writeValueAsBytes(Record.<byte[], byte[]>builder()
                .key(keySerializer.serializer(record.getKey()))
                .value(valueSerializer.serializer(record.getValue()))
                .build());
    }

    @SneakyThrows
    public static <K, V> Record<K, V> deserialize(byte[] record, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ObjectMapper objectMapper) {
        Record<byte[], byte[]> deserializedRecord = objectMapper.readValue(record, new TypeReference<>() {});
        return Record.<K, V>builder()
                .key(keyDeserializer.deserializer(deserializedRecord.getKey()))
                .value(valueDeserializer.deserializer(deserializedRecord.getValue()))
                .build();
    }
}
