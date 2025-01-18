package com.syncdb.core.protocol;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.message.ErrorMessage;
import com.syncdb.core.protocol.message.ReadAckMessage;
import com.syncdb.core.protocol.writer.ProtocolWriter;
import io.reactivex.rxjava3.core.*;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetSocket;
import lombok.Getter;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SocketQueue {
  @Getter
  private final String socketId;
  private final NetSocket netSocket;
  private Boolean connected = true;

  // todo: add close handler based on queue size, only close when empty
  private final Deque<Emitter> queue;

  public SocketQueue(String socketId, NetSocket netSocket) {
    this.socketId = socketId;
    this.netSocket = netSocket;
    this.queue = new ConcurrentLinkedDeque<>();

    netSocket
            .toFlowable()
            .compose(SizePrefixProtocolStreamParser.read(1024 * 1024))
            .concatMap(Flowable::fromIterable)
            .map(
                    r -> {
                      handle(ProtocolMessage.deserialize(r));
                      return true;
                    })
            .subscribe();
  }

  // todo: find out seq num
  public Completable write(List<Record<byte[], byte[]>> records, String namespace) {
    ProtocolMessage message = ProtocolWriter.createWriteMessage(0, records, namespace);
    byte[] serializedMessage = ProtocolMessage.serialize(message);
    return Observable.create(
                    emitter ->
                            Completable.concat(
                                            List.of(
                                                    netSocket.rxWrite(
                                                            Buffer.buffer().appendInt(serializedMessage.length)),
                                                    netSocket.rxWrite(Buffer.buffer(serializedMessage))))
                                    .subscribe(() -> this.queue.add(emitter)))
            .ignoreElements();
  }

  public Completable write(List<Record<byte[], byte[]>> records, String namespace, Integer partition) {
    ProtocolMessage message = ProtocolWriter.createWriteMessage(0, records, namespace, partition);
    byte[] serializedMessage = ProtocolMessage.serialize(message);
    return Observable.create(
                    emitter ->
                            Completable.concat(
                                            List.of(
                                                    netSocket.rxWrite(
                                                            Buffer.buffer().appendInt(serializedMessage.length)),
                                                    netSocket.rxWrite(Buffer.buffer(serializedMessage))))
                                    .subscribe(() -> this.queue.add(emitter)))
            .ignoreElements();
  }

  public Single<List<Record<byte[], byte[]>>> read(List<byte[]> keys, String namespace) {
    ProtocolMessage message = ProtocolWriter.createReadMessage(0, keys, namespace);
    byte[] serializedMessage = ProtocolMessage.serialize(message);
    return Observable.<List<Record<byte[], byte[]>>>create(
                    emitter ->
                            Completable.concat(
                                            List.of(
                                                    netSocket.rxWrite(
                                                            Buffer.buffer().appendInt(serializedMessage.length)),
                                                    netSocket.rxWrite(Buffer.buffer(serializedMessage))))
                                    .subscribe(() -> this.queue.add(emitter)))
            .firstElement()
            .toSingle();
  }

  public Single<List<Record<byte[], byte[]>>> read(List<byte[]> keys, String namespace, Integer partition) {
    ProtocolMessage message = ProtocolWriter.createReadMessage(0, keys, namespace, partition);
    byte[] serializedMessage = ProtocolMessage.serialize(message);
    return Observable.<List<Record<byte[], byte[]>>>create(
                    emitter ->
                            Completable.concat(
                                            List.of(
                                                    netSocket.rxWrite(
                                                            Buffer.buffer().appendInt(serializedMessage.length)),
                                                    netSocket.rxWrite(Buffer.buffer(serializedMessage))))
                                    .subscribe(() -> this.queue.add(emitter)))
            .firstElement()
            .toSingle();
  }

  private void handle(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case NOOP:
        break;
      case READ_ACK:
        this.handleReadAck(message);
        break;
      case WRITE_ACK:
        this.handleWriteAck(message);
        break;
      case ERROR:
        this.handleError(message);
        break;
      default:
        throw new RuntimeException(
                String.format(
                        "handler not implemented for message type: %s", message.getMessageType().name()));
    }
  }

  private void handleReadAck(ProtocolMessage message) {
    Emitter<List<Record<byte[], byte[]>>> emitter = pollEmitter();
    if (message.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR) {
      emitter.onError(new RuntimeException(ErrorMessage.getThrowable(message.getPayload())));
    }
    List<Record<byte[], byte[]>> records =
            ReadAckMessage.deserializePayload(message.getPayload());
    emitter.onNext(records);
    emitter.onComplete();
  }

  private void handleWriteAck(ProtocolMessage message) {
    Emitter<?> emitter = pollEmitter();
    emitter.onComplete();
  }

  private void handleError(ProtocolMessage message) {
    Emitter<?> emitter = pollEmitter();
    emitter.onError(new RuntimeException(ErrorMessage.getThrowable(message.getPayload())));
  }

  @SuppressWarnings("rawtypes")
  private Emitter pollEmitter() {
    Emitter emitter = queue.poll();
    if (!connected && queue.isEmpty()) {
      netSocket.close().subscribe();
    }
    return emitter;
  }

  public void close() {
    this.connected = false;
    while (!this.queue.isEmpty()) {
      Emitter emitter = queue.poll();
      emitter.onError(new RuntimeException("connection was closed"));
    }
  }

  public void closeGracefully() {
    this.connected = false;
  }
}