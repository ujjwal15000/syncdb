package com.syncdb.spark.writer;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;

import java.util.Map;

public class SyncDbWriteBuilder implements WriteBuilder {
    private final String outputPath;
    private final Map<String, String> properties;

    public SyncDbWriteBuilder(String outputPath, Map<String, String> properties) {
        this.outputPath = outputPath;
        this.properties = properties;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new SyncDbBatchWrite(outputPath, properties);
    }
}

