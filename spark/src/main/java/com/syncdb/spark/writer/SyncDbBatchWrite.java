package com.syncdb.spark.writer;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Map;

public class SyncDbBatchWrite implements BatchWrite {
    private final String outputPath;
    private final Map<String, String> properties;

    public SyncDbBatchWrite(String outputPath, Map<String, String> properties) {
        this.outputPath = outputPath;
        this.properties = properties;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new SyncDbDataWriterFactory(outputPath, properties);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        // Handle commit logic if needed
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        // Handle abort logic if needed
    }
}

