package com.syncdb.server.cluster.config;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HelixConfig {
    private String zhHost;
    private String clusterName;
    private String instanceName;
}
