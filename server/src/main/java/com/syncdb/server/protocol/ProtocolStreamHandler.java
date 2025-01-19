package com.syncdb.server.protocol;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.SocketQueue;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.protocol.writer.ProtocolWriter;
import com.syncdb.core.util.ByteArrayWrapper;
import com.syncdb.server.cluster.factory.*;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.functions.BiFunction;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.syncdb.core.constant.Constants.*;
import static com.syncdb.core.constant.Constants.CONNECTION_FACTORY_NAME;

@Slf4j
public class ProtocolStreamHandler {
  private final Vertx vertx;
  private final WorkerExecutor executor;
  private final TabletMailboxFactory mailboxFactory;
  private final String verticleId;
  private ConnectionFactory connectionFactory;

  public ProtocolStreamHandler(
      Vertx vertx, ConnectionFactory connectionFactory, String verticleId) {
    this.vertx = vertx;
    this.executor = vertx.createSharedWorkerExecutor(WORKER_POOL_NAME, 32);
    this.connectionFactory = connectionFactory;
    this.verticleId = verticleId;
    this.mailboxFactory =
        (TabletMailboxFactory)
            vertx.sharedData().getLocalMap(FACTORY_MAP_NAME).get(TabletMailboxFactory.FACTORY_NAME);
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
    Completable connection = Completable.complete();
    if (connectionFactory == null) {
      this.connectionFactory =
          vertx
              .sharedData()
              .<String, ConnectionFactory>getLocalMap(CONNECTION_FACTORY_MAP_NAME)
              .get(CONNECTION_FACTORY_NAME);
      connection = connectionFactory.register(verticleId);
    }

    ReadMessage.Message readMessage = ReadMessage.deserializePayload(message.getPayload());
    if (readMessage.getPartition() != -1) return handleRoutedRead(readMessage, message.getSeq());

    Map<ByteArrayWrapper, Integer> idxMap = new HashMap<>();
    for (int i = 0; i < readMessage.getKeys().size(); i++) {
      idxMap.put(ByteArrayWrapper.create(readMessage.getKeys().get(i)), i);
    }

    return connection.andThen(
        Flowable.fromIterable(readMessage.getKeys())
            .groupBy(
                r ->
                    NamespaceFactory.get(readMessage.getNamespace())
                        .getPartitioner()
                        .getPartition(r))
            .flatMap(
                group -> {
                  SocketQueue socketQueue =
                      connectionFactory.getReader(group.getKey(), this.verticleId);
                  return group
                      .toList()
                      .toFlowable()
                      .flatMap(
                          keys ->
                              socketQueue
                                  .read(
                                      keys,
                                      readMessage.getNamespace(),
                                      readMessage.getBucket(),
                                      group.getKey())
                                  .flattenAsFlowable(r -> r));
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
                e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e))));
  }

  public Flowable<ProtocolMessage> handleRoutedRead(ReadMessage.Message readMessage, int seq) {
    Map<ByteArrayWrapper, Integer> idxMap = new HashMap<>();
    for (int i = 0; i < readMessage.getKeys().size(); i++) {
      idxMap.put(ByteArrayWrapper.create(readMessage.getKeys().get(i)), i);
    }

    return Flowable.fromIterable(readMessage.getKeys())
        .groupBy(
            r -> NamespaceFactory.get(readMessage.getNamespace()).getPartitioner().getPartition(r))
        .flatMap(
            group ->
                group
                    .toList()
                    .toFlowable()
                    .flatMap(
                        keys ->
                            mailboxFactory
                                .get(
                                    TabletConfig.create(readMessage.getNamespace(), group.getKey()))
                                .handleRead(
                                    ProtocolWriter.createReadMessage(
                                        seq,
                                        keys,
                                        readMessage.getNamespace(),
                                        readMessage.getBucket())))
                    .flatMap(
                        mailboxMessage ->
                            Flowable.fromIterable(
                                ReadAckMessage.deserializePayload(mailboxMessage.getPayload()))))
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
        .<ProtocolMessage>map(r -> new ReadAckMessage(seq, r))
        .toFlowable()
        .onErrorResumeNext(
            e -> {
              log.error("error reading from mailbox ", e);
              return Flowable.<ProtocolMessage>just(new ErrorMessage(seq, e));
            });
  }

  // todo: find a better way
  public Flowable<ProtocolMessage> handleWrite(ProtocolMessage message) {
    Completable connection = Completable.complete();
    if (connectionFactory == null) {
      this.connectionFactory =
          vertx
              .sharedData()
              .<String, ConnectionFactory>getLocalMap(CONNECTION_FACTORY_MAP_NAME)
              .get(CONNECTION_FACTORY_NAME);
      connection = connectionFactory.register(verticleId);
    }

    WriteMessage.Message writeMessage = WriteMessage.deserializePayload(message.getPayload());
    if (writeMessage.getPartition() != -1) return handleRoutedWrite(writeMessage, message.getSeq());

    return connection.andThen(
        Flowable.fromIterable(writeMessage.getRecords())
            .groupBy(
                r ->
                    NamespaceFactory.get(writeMessage.getNamespace())
                        .getPartitioner()
                        .getPartition(r.getKey()))
            .flatMapCompletable(
                group -> {
                  SocketQueue socketQueue =
                      connectionFactory.getWriter(group.getKey(), this.verticleId);
                  return group
                      .toList()
                      .flatMapCompletable(
                          li -> socketQueue.write(li, writeMessage.getNamespace(), writeMessage.getBucket(), group.getKey()));
                })
            .andThen(Flowable.<ProtocolMessage>just(new WriteAckMessage(message.getSeq())))
            .onErrorResumeNext(
                e -> Flowable.<ProtocolMessage>just(new ErrorMessage(message.getSeq(), e))));
  }

  public Flowable<ProtocolMessage> handleRoutedWrite(WriteMessage.Message writeMessage, int seq) {
    return Flowable.fromIterable(writeMessage.getRecords())
        .groupBy(
            r ->
                NamespaceFactory.get(writeMessage.getNamespace())
                    .getPartitioner()
                    .getPartition(r.getKey()))
        .flatMapCompletable(
            group ->
                group
                    .toList()
                    .flatMapCompletable(
                        li ->
                            mailboxFactory
                                .get(
                                    TabletConfig.create(
                                        writeMessage.getNamespace(), group.getKey()))
                                .handleWrite(
                                    ProtocolWriter.createWriteMessage(
                                        seq,
                                        li,
                                        writeMessage.getNamespace(),
                                        writeMessage.getBucket()))
                                .ignoreElements()))
        .andThen(Flowable.<ProtocolMessage>just(new WriteAckMessage(seq)))
        .onErrorResumeNext(
            e -> {
              log.error("error reading from mailbox ", e);
              return Flowable.<ProtocolMessage>just(new ErrorMessage(seq, e));
            });
  }
}
