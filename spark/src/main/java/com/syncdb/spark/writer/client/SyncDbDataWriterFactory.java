package com.syncdb.spark.writer.client;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.DataWriter;

import java.util.Map;

public class SyncDbDataWriterFactory implements DataWriterFactory {
    private final Map<String, String> properties;

    public SyncDbDataWriterFactory(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new SyncDbSocketWriter("localhost", 8080);
    }
}
