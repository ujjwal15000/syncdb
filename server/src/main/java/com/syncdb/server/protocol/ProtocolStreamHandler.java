package com.syncdb.server.protocol;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.util.ByteArrayWrapper;
import com.syncdb.server.cluster.factory.MailboxMessage;
import com.syncdb.server.cluster.factory.NamespaceFactory;
import com.syncdb.server.cluster.factory.TabletMailbox;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.functions.BiFunction;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;

@Slf4j
public class ProtocolStreamHandler {
  private final Vertx vertx;
  private final WorkerExecutor executor;

  public ProtocolStreamHandler(Vertx vertx) {
    this.vertx = vertx;
    this.executor = vertx.createSharedWorkerExecutor(WORKER_POOL_NAME, 32);
  }

  public Flowable<ProtocolMessage> handle(ProtocolMessage message, NetSocket socket) {
    switch (message.getMessageType()) {
      case NOOP:
        return Flowable.empty();
      case READ:
        return this.handleRead(message);
      case WRITE:
        return this.handleWrite(message);
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
                          // todo: remove this use connection factory
                          vertx
                              .eventBus()
                              .<byte[]>rxRequest(
                                  readerAddress,
                                  ProtocolMessage.serialize(
                                      new ReadMessage(
                                          message.getSeq(), keys, readMessage.getNamespace())))
                              .toFlowable())
                  .map(ProtocolStreamHandler::processMailboxMessage)
                  .flatMap(
                      mailboxMessage ->
                          Flowable.fromIterable(
                              ReadAckMessage.deserializePayload(mailboxMessage.getPayload())));
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
                          // todo: remove this use connection factory
                          vertx
                              .eventBus()
                              .<byte[]>rxRequest(
                                  writerAddress,
                                  ProtocolMessage.serialize(
                                      new WriteMessage(
                                          message.getSeq(), li, writeMessage.getNamespace())))
                              .map(ProtocolStreamHandler::processMailboxMessage)
                              .ignoreElement());
            })
        .andThen(Flowable.<ProtocolMessage>just(new WriteAckMessage(message.getSeq())))
        .onErrorResumeNext(
            e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e)));
  }

  private static MailboxMessage processMailboxMessage(Message<byte[]> res) {
    MailboxMessage mailboxMessage = MailboxMessage.deserialize(res.body());
    if (!mailboxMessage.getErrorMessage().isEmpty()) {
      throw new RuntimeException(mailboxMessage.getErrorMessage());
    }
    return mailboxMessage;
  }
}
