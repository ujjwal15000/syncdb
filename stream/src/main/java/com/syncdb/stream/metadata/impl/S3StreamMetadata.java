package com.syncdb.stream.metadata.impl;

import com.syncdb.stream.metadata.StreamMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class S3StreamMetadata implements StreamMetadata {
    private Integer latestBlockId;
}
