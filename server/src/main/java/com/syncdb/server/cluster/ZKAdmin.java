package com.syncdb.server.cluster;

import com.syncdb.server.cluster.config.HelixConfig;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;

public class ZKAdmin {
    private final HelixConfig config;
    private final ZKHelixAdmin zkHelixAdmin;

    public ZKAdmin(HelixConfig config) {
        this.config = config;
        this.zkHelixAdmin =
                new ZKHelixAdmin(config.getZhHost());

        this.initCluster();
        this.addNode();
    }

    public void initCluster(){
        zkHelixAdmin.addCluster(config.getClusterName());
    }

    public void addNamespace(String name, int numPartitions) {
        zkHelixAdmin
                .addResource(config.getClusterName(), name, numPartitions, MasterSlaveSMD.name);
    }

    public void addNode() {
        InstanceConfig instanceConfig = new InstanceConfig(config.getInstanceName());
        zkHelixAdmin.addInstance(config.getClusterName(), instanceConfig);
    }
}
