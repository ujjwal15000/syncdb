package com.syncdb.spark.writer;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.syncdb.core.models.Record;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructType;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class SyncDbSstOutputWriter extends OutputWriter {
    private final String path;
    private final StructType schema;
    private final TaskAttemptContext context;

    private final SstFileWriter writer;

    public SyncDbSstOutputWriter(String path, StructType schema, TaskAttemptContext context) throws IOException, RocksDBException {
        this.path = path;
        this.schema = schema;
        this.context = context;

        Path p = new Path(path);

        CodecStreams.createOutputStream(context, p);

        this.writer = new SstFileWriter(new EnvOptions(), new Options().setCreateIfMissing(true));
        writer.open(p.toUri().getPath());
    }

    @SneakyThrows
    @Override
    public void write(InternalRow row) {
        byte[] key = row.getBinary(0);
        byte[] value = row.getBinary(1);
        writer.put(key, value);
    }

    @Override
    public void close() {
        try {
            writer.finish();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        writer.close();
    }

    @Override
    public String path() {
        return this.path;
    }
}
