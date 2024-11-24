package com.syncdb.server.factory;

import com.syncdb.core.partitioner.Murmur3Partitioner;

import java.util.concurrent.ConcurrentHashMap;

public class NamespaceFactory {
    private static final ConcurrentHashMap<String, NamespaceConfig> namespaceMap =
            new ConcurrentHashMap<>();

    public static void add(NamespaceConfig namespace) {
        namespaceMap.put(namespace.getName(), namespace);
    }

    public static NamespaceConfig get(String name) {
        return namespaceMap.get(name);
    }

}
