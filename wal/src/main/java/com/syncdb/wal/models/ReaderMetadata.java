package com.syncdb.wal.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReaderMetadata {
    private Integer blockId;
    private Integer offset;
}
