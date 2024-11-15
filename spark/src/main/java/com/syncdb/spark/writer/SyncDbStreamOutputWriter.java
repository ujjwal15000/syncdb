package com.syncdb.spark.writer;

import com.syncdb.core.models.Record;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.OutputStream;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SyncDbStreamOutputWriter extends OutputWriter {
    private final String path;
    private final StructType schema;
    private final TaskAttemptContext context;

    private final OutputStream outputStream;

    public SyncDbStreamOutputWriter(String path, StructType schema, TaskAttemptContext context) {
        this.path = path;
        this.schema = schema;
        this.context = context;

        Path p = new Path(path);
        this.outputStream = CodecStreams.createOutputStream(context, p);
    }

    @Override
    public void write(InternalRow row) {
        byte[] key = row.getBinary(0);
        byte[] value = row.getBinary(1);
        byte[] data = Record.serialize(key, value);

        try {
            outputStream.write(convertToByteArray(data.length));
            outputStream.write(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String path() {
        return this.path;
    }
}
