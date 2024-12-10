package com.syncdb.server.protocol;

import com.syncdb.core.models.Record;
import com.syncdb.core.partitioner.Murmur3Partitioner;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.util.ByteArrayWrapper;
import com.syncdb.server.factory.MailboxMessage;
import com.syncdb.server.factory.NamespaceFactory;
import com.syncdb.server.factory.TabletFactory;
import com.syncdb.server.factory.TabletMailbox;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.functions.BiFunction;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;
import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

@Slf4j
public class ProtocolStreamHandler {

  private final ConcurrentHashMap<ClientMetadata, Long> socketMap;
  private final Vertx vertx;
  private final WorkerExecutor executor;

  private ClientMetadata clientMetadata;

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

  public Flowable<ProtocolMessage> handleMetadata(ProtocolMessage message, NetSocket socket) {

    this.clientMetadata = MetadataMessage.deserializePayload(message.getPayload());

    long timerId =
        vertx.setPeriodic(
            1_000,
            id -> {
              if (clientMetadata.getIsStreamWriter()) {
                byte[] refreshBuffer = ProtocolMessage.serialize(new RefreshBufferMessage(10000L));
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

  public Flowable<ProtocolMessage> handleRead(ProtocolMessage message) {
    ReadMessage.Message readMessage = ReadMessage.deserializePayload(message.getPayload());

    Map<ByteArrayWrapper, Integer> idxMap = new HashMap<>();
    for (int i = 0; i < readMessage.getKeys().size(); i++) {
      idxMap.put(ByteArrayWrapper.create(readMessage.getKeys().get(i)), i);
    }

    return Flowable.fromIterable(readMessage.getKeys())
        .groupBy(
            r -> NamespaceFactory.get(readMessage.getNamespace()).getPartitioner().getPartition(r))
        .flatMap(
            group -> {
              String readerAddress =
                  TabletMailbox.getReaderAddress(readMessage.getNamespace(), group.getKey());
              return group
                  .toList()
                  .toFlowable()
                  .flatMap(
                      keys ->
                          vertx
                              .eventBus()
                              .<byte[]>rxRequest(
                                  readerAddress,
                                  ProtocolMessage.serialize(
                                      new ReadMessage(
                                          message.getSeq(), keys, readMessage.getNamespace())))
                              .toFlowable())
                  .flatMap(
                      res -> {
                        MailboxMessage mailboxMessage = MailboxMessage.deserialize(res.body());
                        if (!mailboxMessage.getErrorMessage().isEmpty()) {
                          throw new RuntimeException(mailboxMessage.getErrorMessage());
                        }
                        return Flowable.fromIterable(
                            ReadAckMessage.deserializePayload(mailboxMessage.getPayload()));
                      });
            })
        .reduce(
            new ArrayList<>(readMessage.getKeys().size()),
            (BiFunction<
                    List<Record<byte[], byte[]>>,
                    Record<byte[], byte[]>,
                    List<Record<byte[], byte[]>>>)
                (records, record) -> {
                  records.add(idxMap.get(ByteArrayWrapper.create(record.getKey())), record);
                  return records;
                })
        .<ProtocolMessage>map(r -> new ReadAckMessage(message.getSeq(), r))
        .toFlowable()
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  public Flowable<ProtocolMessage> handleWrite(ProtocolMessage message) {
    WriteMessage.Message writeMessage = WriteMessage.deserializePayload(message.getPayload());

    return Flowable.fromIterable(writeMessage.getRecords())
        .groupBy(
            r ->
                NamespaceFactory.get(writeMessage.getNamespace())
                    .getPartitioner()
                    .getPartition(r.getKey()))
        .flatMapCompletable(
            group -> {
              String writerAddress =
                  TabletMailbox.getWriterAddress(writeMessage.getNamespace(), group.getKey());
              return group
                  .toList()
                  .flatMapCompletable(
                      li ->
                          vertx
                              .eventBus()
                              .<byte[]>rxRequest(
                                  writerAddress,
                                  ProtocolMessage.serialize(
                                      new WriteMessage(
                                          message.getSeq(), li, writeMessage.getNamespace())))
                              .map(
                                  res -> {
                                    MailboxMessage mailboxMessage =
                                        MailboxMessage.deserialize(res.body());
                                    if (!mailboxMessage.getErrorMessage().isEmpty()) {
                                      throw new RuntimeException(mailboxMessage.getErrorMessage());
                                    }
                                    return mailboxMessage;
                                  })
                              .ignoreElement());
            })
        .andThen(Flowable.<ProtocolMessage>just(new WriteAckMessage(message.getSeq())))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  public Flowable<ProtocolMessage> handleEndStream(ProtocolMessage message) {
    return Flowable.just(new EndStreamMessage());
  }

  //  private Long getBufferSize() {
  //    Iterator<ClientMetadata> iterator = socketMap.keys().asIterator();
  //    int count = 0;
  //    while (iterator.hasNext()) {
  //      ClientMetadata r = iterator.next();
  //      if (Objects.equals(r.getNamespace(), clientMetadata.getNamespace())
  //          && Objects.equals(r.getPartitionId(), clientMetadata.getPartitionId())) {
  //        count++;
  //      }
  //    }
  //    return count == 0 ? count : TabletFactory.getCurrentWriteRate() / count;
  //  }
}
