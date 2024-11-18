package com.syncdb.server.verticle;

import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.server.protocol.SizePrefixProtocolStreamParser;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.SocketMetadata;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;
import io.vertx.rxjava3.core.shareddata.AsyncMap;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class TabletVerticle extends AbstractVerticle {
  private NetServer netServer;
  private AsyncMap<SocketMetadata, NetSocket> socketMap;

  private static NetServerOptions netServerOptions =
      new NetServerOptions()
          .setHost("0.0.0.0")
          .setPort(8080)
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
    startKeepNoopPoller();
    return vertx
        .createNetServer(netServerOptions)
        .connectHandler(this::socketHandler)
        .rxListen()
        .doOnSuccess(server -> this.netServer = server)
        .flatMap(ignore -> vertx.sharedData().<SocketMetadata, NetSocket>getAsyncMap("socket-map"))
        .doOnSuccess(map -> socketMap = map)
        .ignoreElement();
  }

  public void startKeepNoopPoller() {
    vertx.setPeriodic(
        5_000,
        timerId ->
            socketMap
                .values()
                .flattenAsFlowable(r -> r)
                .flatMapCompletable(socket -> socket.rxWrite("NOOP"))
                .subscribe());
  }

  private void socketHandler(NetSocket socket) {
    ProtocolStreamHandler streamHandler = new ProtocolStreamHandler(this.vertx, this.socketMap);
    socket
        .toFlowable()
        .compose(SizePrefixProtocolStreamParser.read(1024 * 1024))
        .map(ProtocolMessage::deserialize)
        .concatMap(message -> streamHandler.handle(message, socket))
        .map(ProtocolMessage::serialize)
        .concatMapCompletable(
            data -> {
              byte[] len = convertToByteArray(data.length);
              return Completable.concatArray(
                  socket.rxWrite(Buffer.buffer(len)), socket.rxWrite(Buffer.buffer(data)));
            })
        .subscribe();
  }

  @Override
  public Completable rxStop() {
    return null;
  }
}
