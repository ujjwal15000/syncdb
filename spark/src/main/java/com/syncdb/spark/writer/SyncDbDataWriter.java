package com.syncdb.spark.writer;

import com.syncdb.core.models.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.io.OutputStream;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SyncDbDataWriter implements DataWriter<InternalRow> {
    private final OutputStream outputStream;

    public SyncDbDataWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        byte[] key = row.getBinary(0);
        byte[] value = row.getBinary(1);

        byte[] data = Record.serialize(key, value);
        outputStream.write(convertToByteArray(data.length));
        outputStream.write(data);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        outputStream.flush();
        return null;
    }

    @Override
    public void abort() {
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}

