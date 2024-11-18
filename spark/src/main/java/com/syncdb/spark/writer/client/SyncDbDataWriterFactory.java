package com.syncdb.spark.writer.client;

import com.syncdb.core.protocol.ClientMetadata;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.DataWriter;

import java.util.Map;
import java.util.UUID;

public class SyncDbDataWriterFactory implements DataWriterFactory {
  private final Map<String, String> properties;
  private final String clientId = UUID.randomUUID().toString();
  private final String host;
  private final Integer port;
  private final String namespace;
  private final Integer partitionId;

  public SyncDbDataWriterFactory(Map<String, String> properties) {
    this.properties = properties;
    this.host = properties.get("host");
    this.port = Integer.parseInt(properties.get("port"));
    this.namespace = properties.get("namespace");
    this.partitionId = Integer.parseInt(properties.get("partitionId"));
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new SyncDbSocketWriter(
        this.host,
        this.port,
        new ClientMetadata(this.clientId, this.namespace, this.partitionId, true));
  }
}
