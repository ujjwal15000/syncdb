package com.syncdb.server.verticle;

import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.server.protocol.SizePrefixProtocolStreamParser;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;

import java.util.concurrent.ConcurrentHashMap;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SocketVerticle extends AbstractVerticle {
  private NetServer netServer;
  private final ConcurrentHashMap<ClientMetadata, Long> socketMap = new ConcurrentHashMap<>();

  private static NetServerOptions netServerOptions =
      new NetServerOptions()
          .setHost("0.0.0.0")
          .setPort(9009)
          .setIdleTimeout(20)
          .setLogActivity(false)
          .setReuseAddress(true)
          .setReusePort(true)
          .setTcpFastOpen(true)
          .setTcpNoDelay(true)
          .setTcpQuickAck(true)
          .setTcpKeepAlive(true)
          .setUseAlpn(false);

  @Override
  public Completable rxStart() {
    return vertx
        .createNetServer(netServerOptions)
        .connectHandler(this::socketHandler)
        .rxListen()
        .doOnSuccess(server -> this.netServer = server)
        .ignoreElement();
  }

  // todo: add metrics in ping pong message
  private void socketHandler(NetSocket socket) {
    ProtocolStreamHandler streamHandler = new ProtocolStreamHandler(this.vertx, this.socketMap);

//    long timerId =
//            vertx.setPeriodic(
//                    1_000,
//                    id -> {
//                        byte[] noop = ProtocolMessage.serialize(new NoopMessage());
//                        byte[] len = convertToByteArray(noop.length);
//                        socket.rxWrite(Buffer.buffer(len).appendBytes(noop)).subscribe();
//                    });

//    socket.closeHandler(v -> vertx.cancelTimer(timerId));

    socket
        .toFlowable()
        .compose(SizePrefixProtocolStreamParser.read(1024 * 1024))
        .concatMap(Flowable::fromIterable)
        .map(ProtocolMessage::deserialize)
        .concatMap(message -> streamHandler.handle(message, socket))
        .map(ProtocolMessage::serialize)
        .concatMapCompletable(
            data -> {
              byte[] len = convertToByteArray(data.length);
              return socket.rxWrite(Buffer.buffer(len).appendBytes(data));
            })
        .subscribe();
  }

  @Override
  public Completable rxStop() {
    return netServer.rxClose();
  }
}
