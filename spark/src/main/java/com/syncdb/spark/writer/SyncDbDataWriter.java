package com.syncdb.spark.writer;

import com.google.flatbuffers.FlatBufferBuilder;
import com.syncdb.core.flatbuffers.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.io.OutputStream;

public class SyncDbDataWriter implements DataWriter<InternalRow> {
    private final OutputStream outputStream;
    private final FlatBufferBuilder builder;

    public SyncDbDataWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
        this.builder = new FlatBufferBuilder();
    }

    @Override
    public void write(InternalRow row) throws IOException {
        byte[] key = row.getBinary(0);
        byte[] value = row.getBinary(1);

        int keyOffset = Record.createKeyVector(builder, key);
        int valueOffset = Record.createValueVector(builder, value);

        Record.startRecord(builder);
        Record.addKey(builder, keyOffset);
        Record.addValue(builder, valueOffset);

        int recordOffset = Record.endRecord(builder);
        builder.finish(recordOffset);

        outputStream.write(builder.sizedByteArray());
        builder.clear();
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        outputStream.flush();
        return null;
    }

    @Override
    public void abort() {
        // Handle errors or cleanup
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}

