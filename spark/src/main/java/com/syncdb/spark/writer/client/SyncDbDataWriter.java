package com.syncdb.spark.writer.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;

@Slf4j
public class SyncDbDataWriter implements DataWriter<InternalRow> {
  @Override
  public void write(InternalRow record) throws IOException {
    log.info(
        String.format(
            "key: %s, value: %s",
            new String(record.getBinary(0)), new String(record.getBinary(1))));
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    return null;
  }

  @Override
  public void abort() throws IOException {}

  @Override
  public void close() throws IOException {}
}
