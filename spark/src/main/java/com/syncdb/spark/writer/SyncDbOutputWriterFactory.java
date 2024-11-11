package com.syncdb.spark.writer;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;

public class SyncDbOutputWriterFactory extends OutputWriterFactory {

    @Override
    public String getFileExtension(TaskAttemptContext context) {
        return ".sdb" + CodecStreams.getCompressionExtension(context);
    }

    @Override
    public OutputWriter newInstance(String path, StructType schema, TaskAttemptContext context) {
        return new SyncDbOutputWriter(path, schema, context);
    }
}
