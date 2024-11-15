package com.syncdb.spark.writer.sst;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.rocksdb.RocksDBException;

import java.io.IOException;

public class SyncDbSstOutputWriterFactory extends OutputWriterFactory {

    @Override
    public String getFileExtension(TaskAttemptContext context) {
        return ".sst" + CodecStreams.getCompressionExtension(context);
    }

    @Override
    public OutputWriter newInstance(String path, StructType schema, TaskAttemptContext context) {
        try {
            return new SyncDbSstOutputWriter(path, schema, context);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
