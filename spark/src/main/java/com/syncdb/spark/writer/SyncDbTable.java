package com.syncdb.spark.writer;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.connector.catalog.TableCapability.OVERWRITE_DYNAMIC;

public class SyncDbTable implements Table, SupportsWrite {
  private final StructType schema;
    private final String tableName;
  private final Map<String, String> properties;

  public SyncDbTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.tableName = properties.get("tableName");
    this.properties = properties;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Set.of();
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    String outputPath = properties.getOrDefault("path", "default/output/path");

    return new SyncDbWriteBuilder(outputPath, properties);
  }
}
