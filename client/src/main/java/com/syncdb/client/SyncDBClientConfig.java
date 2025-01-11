package com.syncdb.client;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SyncDBClientConfig {
    private final String host;
    private final Integer port;
    @Builder.Default
    private final Integer numConnections = 8;
}
