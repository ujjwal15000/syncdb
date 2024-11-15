package com.syncdb.spark.writer.client;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;

import java.util.Map;

public class SyncDbWriteBuilder implements WriteBuilder {
    private final Map<String, String> properties;

    public SyncDbWriteBuilder(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new SyncDbBatchWrite(properties);
    }
}

