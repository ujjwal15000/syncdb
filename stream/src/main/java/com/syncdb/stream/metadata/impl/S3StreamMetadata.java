package com.syncdb.stream.metadata.impl;

import com.syncdb.stream.metadata.StreamMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class S3StreamMetadata implements StreamMetadata, Serializable {
    private Long latestBlockId;

    public static byte[] serialize(S3StreamMetadata s3StreamMetadata){
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(s3StreamMetadata.latestBlockId);
        return buffer.array();
    }

    public static S3StreamMetadata deserialize(byte[] data){
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return S3StreamMetadata.builder()
                .latestBlockId(buffer.getLong())
                .build();
    }
}
