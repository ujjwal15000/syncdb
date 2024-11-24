package com.syncdb.server.factory;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;
import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.*;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetSocket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.WriteBatch;

@Slf4j
public class MessageStreamHandler {

  private final Vertx vertx;
  private final Tablet tablet;
  private final WorkerExecutor executor;

  public MessageStreamHandler(Vertx vertx, Tablet tablet, WorkerExecutor executor) {
    this.vertx = vertx;
    this.tablet = tablet;
    this.executor = executor;
  }

  public Flowable<ProtocolMessage> handle(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case READ:
        return this.handleRead(message);
      case WRITE:
        return this.handleWrite(message);
      case BULK_READ:
        return this.handleBulkRead(message);
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
}
