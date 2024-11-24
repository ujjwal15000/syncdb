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
                .map(ProtocolMessage::serialize)
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
                .map(ProtocolMessage::serialize)
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

  private Flowable<ProtocolMessage> writeHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case WRITE:
        return this.handleWrite(message);
      case BULK_WRITE:
        return this.handleBulkWrite(message);
      case STREAMING_WRITE:
        return this.handleStreamingWrite(message);
      default:
        return Flowable.just(
            new ErrorMessage(
                message.getSeq(),
                new RuntimeException(
                    String.format(
                        "handler not implemented for message type: %s",
                        message.getMessageType().name()))));
    }
  }

  private Flowable<ProtocolMessage> readHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case READ:
        return this.handleRead(message);
      case BULK_READ:
        return this.handleBulkRead(message);
      default:
        return Flowable.just(
            new ErrorMessage(
                message.getSeq(),
                new RuntimeException(
                    String.format(
                        "handler not implemented for message type: %s",
                        message.getMessageType().name()))));
    }
  }

  private Flowable<ProtocolMessage> handleRead(ProtocolMessage message) {
    return executeBlocking(() -> tablet.getReader().read(ReadMessage.getKey(message)))
        .<ProtocolMessage>map(
            r ->
                new ReadAckMessage(
                    message.getSeq(),
                    List.of(
                        Record.<byte[], byte[]>builder()
                            .key(message.getPayload())
                            .value(r)
                            .build())))
        .switchIfEmpty(
            Flowable.<ProtocolMessage>just(
                new ReadAckMessage(
                    message.getSeq(),
                    List.of(
                        Record.<byte[], byte[]>builder()
                            .key(message.getPayload())
                            .value(new byte[0])
                            .build()))))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleWrite(ProtocolMessage message) {
    return this.<Void>executeBlocking(
            () -> {
              Record<byte[], byte[]> record = WriteMessage.getRecord(message);
              WriteBatch writeBatch = new WriteBatch();
              writeBatch.put(record.getKey(), record.getValue());
              tablet.getIngestor().write(writeBatch);
              return null;
            })
        .<ProtocolMessage>map(ignore -> new WriteAckMessage(message.getSeq()))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleBulkRead(ProtocolMessage message) {
    return executeBlocking(
            () ->
                tablet
                    .getReader()
                    .bulkRead(BulkReadMessage.deserializePayload(message.getPayload())))
        .map(
            r -> {
              List<Record<byte[], byte[]>> records = new ArrayList<>();
              for (byte[] val : r) {
                records.add(
                    Record.<byte[], byte[]>builder()
                        .key(message.getPayload())
                        .value(val == null ? new byte[0] : val)
                        .build());
              }
              return records;
            })
        .<ProtocolMessage>map(r -> new ReadAckMessage(message.getSeq(), r))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleBulkWrite(ProtocolMessage message) {
    return this.<Void>executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records =
                  BulkWriteMessage.deserializePayload(message.getPayload());
              WriteBatch writeBatch = new WriteBatch();
              for (Record<byte[], byte[]> record : records) {
                writeBatch.put(record.getKey(), record.getValue());
              }
              tablet.getIngestor().write(writeBatch);
              return null;
            })
        .<ProtocolMessage>map(ignore -> new WriteAckMessage(message.getSeq()))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleStreamingWrite(ProtocolMessage message) {
    return this.executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records =
                  StreamingWriteMessage.deserializePayload(message.getPayload());
              WriteBatch writeBatch = new WriteBatch();
              for (Record<byte[], byte[]> record : records) {
                writeBatch.put(record.getKey(), record.getValue());
              }
              tablet.getIngestor().write(writeBatch);
              return true;
            })
        .<ProtocolMessage>map(ignore -> new WriteAckMessage(message.getSeq()))
        .onErrorResumeNext(e -> Flowable.<ProtocolMessage>just(new KillStreamMessage(e)));
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
