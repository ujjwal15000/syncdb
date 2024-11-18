package com.syncdb.server.protocol;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.message.*;
import com.syncdb.server.factory.TabletFactory;
import com.syncdb.tablet.Tablet;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetSocket;
import io.vertx.rxjava3.core.shareddata.AsyncMap;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.WriteBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;
import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

@Slf4j
public class ProtocolStreamHandler {

  private final ConcurrentHashMap<ClientMetadata, Long> socketMap;
  private final Vertx vertx;
  private final WorkerExecutor executor;

  private ClientMetadata clientMetadata;
  private Tablet tablet;

  public ProtocolStreamHandler(Vertx vertx, ConcurrentHashMap<ClientMetadata, Long> socketMap) {
    this.vertx = vertx;
    this.socketMap = socketMap;
    this.executor = vertx.createSharedWorkerExecutor(WORKER_POOL_NAME, 32);
  }

  public Flowable<ProtocolMessage> handle(ProtocolMessage message, NetSocket socket) {
    switch (message.getMessageType()) {
      case NOOP:
        return Flowable.empty();
      case METADATA:
        return this.handleMetadata(message, socket);
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

  private Flowable<ProtocolMessage> handleMetadata(ProtocolMessage message, NetSocket socket) {
    this.clientMetadata = MetadataMessage.deserializePayload(message.getPayload());
    this.tablet =
        TabletFactory.get(
            Tablet.TabletConfig.create(
                clientMetadata.getNamespace(), clientMetadata.getPartitionId()));

    long timerId =
        vertx.setPeriodic(
            5_000,
            id -> {
              byte[] noop = ProtocolMessage.serialize(new NoopMessage());
              byte[] len = convertToByteArray(noop.length);
              socket.rxWrite(Buffer.buffer(len).appendBytes(noop)).subscribe();
            });
    socketMap.put(clientMetadata, timerId);

    socket.closeHandler(
        v -> {
          vertx.cancelTimer(socketMap.get(clientMetadata));
          socketMap.remove(clientMetadata);
        });

    return clientMetadata.getIsStreamWriter()
        ? Flowable.just(new RefreshBufferMessage(getBufferSize()))
        : Flowable.just(new NoopMessage());
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
    return this.<Void>executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records =
                  StreamingWriteMessage.deserializePayload(message.getPayload());
              WriteBatch writeBatch = new WriteBatch();
              for (Record<byte[], byte[]> record : records) {
                writeBatch.put(record.getKey(), record.getValue());
              }
              tablet.getIngestor().write(writeBatch);
              return null;
            })
        .<ProtocolMessage>map(ignore -> new RefreshBufferMessage(getBufferSize()))
        .onErrorResumeNext(e -> Flowable.<ProtocolMessage>just(new KillStreamMessage(e)));
  }

  private Long getBufferSize() {
    Iterator<ClientMetadata> iterator = socketMap.keys().asIterator();
    int count = 0;
    while (iterator.hasNext()) {
      ClientMetadata r = iterator.next();
      if (Objects.equals(r.getNamespace(), clientMetadata.getNamespace())
          && Objects.equals(r.getPartitionId(), clientMetadata.getPartitionId())) {
        count++;
      }
    }
    return count == 0 ? count : tablet.getRateLimiter().getSingleBurstBytes() / count;
  }

  public <V> Flowable<V> executeBlocking(Callable<V> callable) {
    return executor.rxExecuteBlocking(callable, false).toFlowable();
  }
}
