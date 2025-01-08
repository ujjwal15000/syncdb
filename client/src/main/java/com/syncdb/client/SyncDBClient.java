package com.syncdb.client;

import com.syncdb.client.reader.ProtocolHandlerRegistry;
import com.syncdb.client.writer.ProtocolWriter;
import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.SizePrefixProtocolStreamParser;
import com.syncdb.core.protocol.message.ErrorMessage;
import com.syncdb.core.protocol.message.ReadAckMessage;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.net.NetClientOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetClient;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class SyncDBClient {
  private final SecureRandom secureRandom = new SecureRandom();
  private final Vertx vertx;
  private final NetClient netClient;
  private final ConcurrentHashMap<String, SocketQueue> sockets;

  SyncDBClient(Vertx vertx) {
    this.vertx = vertx;
    this.netClient =
        vertx.createNetClient(
            new NetClientOptions()
                .setIdleTimeout(30_000)
                .setLogActivity(true)
                .setConnectTimeout(30_000));
    this.sockets = new ConcurrentHashMap<>();
  }

  public Completable connect(String host, int port) {
    return netClient
        .rxConnect(port, host)
        .map(
            socket -> {
              String socketId = UUID.randomUUID().toString();
              sockets.put(socketId, new SocketQueue(socketId, socket));
              socket.closeHandler(v -> sockets.remove(socketId));
              return socket;
            })
        .ignoreElement();
  }

  public Completable connect(String host, int port, int numConnections) {
    return Observable.range(0, numConnections).flatMapCompletable(ignore -> connect(host, port));
  }

  private Completable write(List<Record<byte[], byte[]>> records, String namespace) {
    int index = secureRandom.nextInt(sockets.size());
    return sockets.get((String) sockets.keySet().toArray()[index])
            .write(records, namespace);
  }

  private Single<List<Record<byte[], byte[]>>> read(List<byte[]> keys, String namespace) {
    int index = secureRandom.nextInt(sockets.size());
    return sockets.get((String) sockets.keySet().toArray()[index])
            .read(keys, namespace);
  }

  static class SocketQueue {
    private final String socketId;
    private final NetSocket netSocket;
    private final Deque<CompletableEmitter> writerQueue;
    private final Deque<SingleEmitter<List<Record<byte[], byte[]>>>> readerQueue;
    private final ProtocolHandlerRegistry registry;

    SocketQueue(String socketId, NetSocket netSocket) {
      this.socketId = socketId;
      this.netSocket = netSocket;
      this.writerQueue = new ConcurrentLinkedDeque<>();
      this.readerQueue = new ConcurrentLinkedDeque<>();
      this.registry = new ProtocolHandlerRegistry();

      netSocket
          .toFlowable()
          .compose(SizePrefixProtocolStreamParser.read(1024 * 1024))
          .concatMap(Flowable::fromIterable)
          .map(r -> {
            handle(ProtocolMessage.deserialize(r));
            return true;
          })
          .subscribe();
    }

    // todo: find out seq num
    public Completable write(List<Record<byte[], byte[]>> records, String namespace) {
      ProtocolMessage message = ProtocolWriter.createWriteMessage(0, records, namespace);
      return Completable.create(
          emitter ->
              netSocket
                  .rxWrite(Buffer.buffer(ProtocolMessage.serialize(message)))
                  .subscribe(() -> this.writerQueue.add(emitter)));
    }

    public Single<List<Record<byte[], byte[]>>> read(List<byte[]> keys, String namespace) {
      ProtocolMessage message = ProtocolWriter.createReadMessage(0, keys, namespace);
      return Single.create(
              emitter ->
                      netSocket
                              .rxWrite(Buffer.buffer(ProtocolMessage.serialize(message)))
                              .subscribe(() -> this.readerQueue.add(emitter)));
    }

    private void handle(ProtocolMessage message) {
      switch (message.getMessageType()) {
        case NOOP:
          break;
        case READ_ACK:
          this.handleReadAck(message);
        case WRITE_ACK:
          this.handleWriteAck(message);
        default:
          throw new RuntimeException(
              String.format(
                  "handler not implemented for message type: %s", message.getMessageType().name()));
      }
    }

    private void handleReadAck(ProtocolMessage message) {
      SingleEmitter<List<Record<byte[], byte[]>>> emitter = readerQueue.poll();
      if (message.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR) {
        emitter.onError(new RuntimeException(ErrorMessage.getThrowable(message.getPayload())));
      }
      List<Record<byte[], byte[]>> records = ReadAckMessage.deserializePayload(message.getPayload());
      emitter.onSuccess(records);
    }

    private void handleWriteAck(ProtocolMessage message) {
      CompletableEmitter emitter = writerQueue.poll();
      if (message.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR) {
        emitter.onError(new RuntimeException(ErrorMessage.getThrowable(message.getPayload())));
      }
      emitter.onComplete();
    }
  }
}
