package com.syncdb.server.protocol;

import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.message.*;
import com.syncdb.server.factory.TabletFactory;
import com.syncdb.server.factory.TabletMailbox;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.*;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Objects;
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
  private String writerAddress;
  private String readerAddress;

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
      case END_STREAM:
        return this.handleEndStream(message);
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
            TabletConfig.create(clientMetadata.getNamespace(), clientMetadata.getPartitionId()));

    this.writerAddress = TabletMailbox.getWriterAddress(clientMetadata.getNamespace(), clientMetadata.getPartitionId());
    this.readerAddress = TabletMailbox.getReaderAddress(clientMetadata.getNamespace(), clientMetadata.getPartitionId());

    long timerId =
        vertx.setPeriodic(
            1_000,
            id -> {
              if (clientMetadata.getIsStreamWriter()) {
                byte[] refreshBuffer =
                    ProtocolMessage.serialize(new RefreshBufferMessage(getBufferSize()));
                byte[] len = convertToByteArray(refreshBuffer.length);
                socket.rxWrite(Buffer.buffer(len).appendBytes(refreshBuffer)).subscribe();
              } else {
                byte[] noop = ProtocolMessage.serialize(new NoopMessage());
                byte[] len = convertToByteArray(noop.length);
                socket.rxWrite(Buffer.buffer(len).appendBytes(noop)).subscribe();
              }
            });

    socketMap.put(clientMetadata, timerId);

    socket.closeHandler(
        v -> {
          vertx.cancelTimer(socketMap.get(clientMetadata));
          socketMap.remove(clientMetadata);
        });

    return Flowable.just(new NoopMessage());
  }

  private Flowable<ProtocolMessage> handleRead(ProtocolMessage message) {
    return vertx
        .eventBus()
        .<byte[]>rxRequest(readerAddress, ProtocolMessage.serialize(message))
        .toFlowable()
        .map(Message::body)
        .map(ProtocolMessage::deserialize)
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleWrite(ProtocolMessage message) {
    return vertx
        .eventBus()
        .<byte[]>rxRequest(writerAddress, ProtocolMessage.serialize(message))
        .toFlowable()
        .map(Message::body)
        .map(ProtocolMessage::deserialize)
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleBulkRead(ProtocolMessage message) {
    return vertx
        .eventBus()
        .<byte[]>rxRequest(readerAddress, ProtocolMessage.serialize(message))
        .toFlowable()
        .map(Message::body)
        .map(ProtocolMessage::deserialize)
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleBulkWrite(ProtocolMessage message) {
    return vertx
        .eventBus()
        .<byte[]>rxRequest(writerAddress, ProtocolMessage.serialize(message))
        .toFlowable()
        .map(Message::body)
        .map(ProtocolMessage::deserialize)
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private Flowable<ProtocolMessage> handleStreamingWrite(ProtocolMessage message) {
    return vertx
        .eventBus()
        .<byte[]>rxRequest(writerAddress, ProtocolMessage.serialize(message))
        .toFlowable()
        .map(Message::body)
        .map(ProtocolMessage::deserialize)
        .onErrorResumeNext(e -> Flowable.<ProtocolMessage>just(new KillStreamMessage(e)));
  }

  private Flowable<ProtocolMessage> handleEndStream(ProtocolMessage message) {
    return Flowable.just(new EndStreamMessage());
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
    return count == 0 ? count : tablet.getRateLimiter().getBytesPerSecond() / count;
  }
}
