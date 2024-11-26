package com.syncdb.server.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NamespaceFactory {
    private static final String BASE_PATH = "namespace-metadata";

    private static final ConcurrentHashMap<String, NamespaceConfig> namespaceMap =
            new ConcurrentHashMap<>();

    public static void add(NamespaceConfig namespace) {
        namespaceMap.put(namespace.getName(), namespace);
    }

    public static void add(ZkHelixPropertyStore<ZNRecord> propertyStore, NamespaceMetadata namespace) {
        Set<String> names = new HashSet<>(propertyStore.getChildNames(BASE_PATH, 0));
        if(names.contains(namespace.getName()))
            throw new RuntimeException("namespace already exists");

        ZNRecord record = new ZNRecord(namespace.getName());
        record.setSimpleField("name", namespace.getName());
        record.setIntField("num-partitions", namespace.getNumPartitions());
        propertyStore.set(getNamespaceNodePath(namespace.getName()), record, 0);
    }

    private static String getNamespaceNodePath(String name){
        return String.format("%s/%s", BASE_PATH, name);
    }

    public static NamespaceConfig get(String name) {
        return namespaceMap.get(name);
    }

    public static NamespaceMetadata get(ZkHelixPropertyStore<ZNRecord> propertyStore, String name) {
        Set<String> names = new HashSet<>(propertyStore.getChildNames(BASE_PATH, 0));
        if(!names.contains(name))
            throw new RuntimeException("namespace does not exist");
        Stat stat = new Stat();
        ZNRecord record = propertyStore.get(getNamespaceNodePath(name), stat, 0);
        int numPartitions = record.getIntField("num-partitions", -1);
        assert numPartitions != -1;
        return NamespaceMetadata.create(name, numPartitions);
    }
}
