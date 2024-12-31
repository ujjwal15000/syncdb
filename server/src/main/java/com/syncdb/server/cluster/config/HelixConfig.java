package com.syncdb.cluster.config;

import lombok.Builder;
import lombok.Data;

@Data
public class HelixConfig {
    private String zhHost;
    private String clusterName;
    private String instanceName;
    private NODE_TYPE nodeType;

    public HelixConfig(String zhHost, String clusterName, String instanceName, NODE_TYPE nodeType){
        this.zhHost = zhHost;
        this.clusterName = clusterName;
        this.instanceName = instanceName;
        this.nodeType = nodeType;
    }

    public enum NODE_TYPE{
        STORAGE,
        COMPUTE
    }
}
