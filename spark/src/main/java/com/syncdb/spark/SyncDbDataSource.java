package com.syncdb.spark;

import com.syncdb.spark.writer.SyncDbFileFormat;
import com.syncdb.spark.writer.SyncDbWriteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;

public class SyncDbDataSource implements DataSourceRegister, FileDataSourceV2 {
    public static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
    });

    private String outputPath;
    private Configuration hadoopConf;

    @Override
    public String shortName() {
        return "syncdb";
    }

    @Override
    public Class<? extends FileFormat> fallbackFileFormat() {
        return SyncDbFileFormat.class;
    }

    @Override
    public Table getTable(CaseInsensitiveStringMap options) {
        return new SyncDbTable(DEFAULT_SCHEMA, options.asCaseSensitiveMap());
    }

    @Override
    public Table org$apache$spark$sql$execution$datasources$v2$FileDataSourceV2$$t() {
        return null;
    }

    @Override
    public void org$apache$spark$sql$execution$datasources$v2$FileDataSourceV2$$t_$eq(Table x$1) {

    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return DEFAULT_SCHEMA;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SyncDbTable(DEFAULT_SCHEMA, properties);
    }

    private static class SyncDbTable implements Table, SupportsWrite {
        private final StructType schema;
        private final Map<String, String> properties;

        SyncDbTable(StructType schema, Map<String, String> properties) {
            this.schema = schema;
            this.properties = properties;
        }

        @Override
        public String name() {
            return "syncdb_table";
        }

        @Override
        public StructType schema() {
            return schema;
        }

        @Override
        public Set<TableCapability> capabilities() {
            return Set.of(TableCapability.BATCH_WRITE);
        }

        @Override
        public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
            String outputPath = properties.getOrDefault("path", "default/output/path");

            return new SyncDbWriteBuilder(outputPath, properties);
        }
    }
}
