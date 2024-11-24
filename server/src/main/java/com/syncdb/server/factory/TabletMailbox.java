package com.syncdb.server.factory;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import lombok.Data;
import org.rocksdb.WriteBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;

public class TabletMailbox {
  private final Vertx vertx;
  private final Tablet tablet;
  private final TabletConfig config;
  private final WorkerExecutor executor;
  private MessageConsumer<byte[]> writer;
  private MessageConsumer<byte[]> reader;

  private TabletMailbox(Vertx vertx, TabletConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.tablet = TabletFactory.get(config);
    this.executor = vertx.createSharedWorkerExecutor(WORKER_POOL_NAME, 32);
  }

  public static TabletMailbox create(Vertx vertx, TabletConfig config) {
    return new TabletMailbox(vertx, config);
  }

  public void startWriter() {
    this.tablet.openIngestor();
    this.writer =
        vertx.eventBus().consumer(getWriterAddress(config.getNamespace(), config.getPartitionId()));

    writer.handler(
        message ->
            this.writeHandler(ProtocolMessage.deserialize(message.body()))
                .map(MailboxMessage::serialize)
                .concatMapCompletable(res -> message.rxReplyAndRequest(res).ignoreElement())
                .subscribe());
  }

  public void startReader() {
    this.tablet.openReader();
    this.reader =
        vertx.eventBus().consumer(getReaderAddress(config.getNamespace(), config.getPartitionId()));

    reader.handler(
        message ->
            this.readHandler(ProtocolMessage.deserialize(message.body()))
                .map(MailboxMessage::serialize)
                .concatMapCompletable(res -> message.rxReplyAndRequest(res).ignoreElement())
                .subscribe());
  }

  public void closeWriter() {
    writer.rxUnregister().subscribe();
    this.tablet.closeIngestor();
  }

  public void closeReader() {
    reader.rxUnregister().subscribe();
    this.tablet.closeReader();
  }

  private Flowable<MailboxMessage> writeHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case WRITE:
        return this.handleWrite(message);
      default:
        return Flowable.just(
            MailboxMessage.failed(
                String.format(
                    "handler not implemented for message type: %s",
                    message.getMessageType().name())));
    }
  }

  private Flowable<MailboxMessage> readHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case READ:
        return this.handleRead(message);
      default:
        return Flowable.just(
            MailboxMessage.failed(
                String.format(
                    "handler not implemented for message type: %s",
                    message.getMessageType().name())));
    }
  }

  private Flowable<MailboxMessage> handleRead(ProtocolMessage message) {
    return executeBlocking(
            () -> tablet.getReader().bulkRead(ReadMessage.deserializePayload(message.getPayload()).getKeys()))
        .map(
            r -> {
              List<Record<byte[], byte[]>> records = new ArrayList<>();
              for (byte[] value : r) {
                records.add(
                    Record.<byte[], byte[]>builder()
                        .key(message.getPayload())
                        .value(value == null ? new byte[0] : value)
                        .build());
              }
              return ReadAckMessage.serializePayload(records);
            })
        .map(MailboxMessage::success);
  }

  private Flowable<MailboxMessage> handleWrite(ProtocolMessage message) {
    return this.executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records =
                  WriteMessage.deserializePayload(message.getPayload()).getRecords();

              WriteBatch writeBatch = new WriteBatch();
              for (Record<byte[], byte[]> record : records)
                writeBatch.put(record.getKey(), record.getValue());
              tablet.getIngestor().write(writeBatch);
              return true;
            })
        .map(ignore -> MailboxMessage.success(new byte[0]));
  }

  public <V> Flowable<V> executeBlocking(Callable<V> callable) {
    return executor.rxExecuteBlocking(callable, false).toFlowable();
  }

  public static String getWriterAddress(String namespace, Integer partitionId) {
    return namespace + "_" + partitionId + "_WRITER";
  }

  public static String getReaderAddress(String namespace, Integer partitionId) {
    return namespace + "_" + partitionId + "_READER";
  }
}
