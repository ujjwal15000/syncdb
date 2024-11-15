package com.syncdb.spark;

import com.syncdb.spark.writer.SyncDbTable;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SyncDbClientDataSource implements DataSourceRegister, TableProvider {
    public static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField("key", DataTypes.BinaryType, false, Metadata.empty()),
            new StructField("value", DataTypes.BinaryType, false, Metadata.empty())
    });

    @Override
    public String shortName() {
        return "syncdb";
    }

//    @Override
//    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
//        return new SyncDbWriteBuilder(info.options());
//    }
//
//    @Override
//    public String name() {
//        return "syncdb";
//    }
//
//    @Override
//    public StructType schema() {
//        return DEFAULT_SCHEMA;
//    }
//
//    @Override
//    public Set<TableCapability> capabilities() {
//        return Set.of(TableCapability.BATCH_WRITE);
//    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return DEFAULT_SCHEMA;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SyncDbTable(schema, properties);
    }
}
