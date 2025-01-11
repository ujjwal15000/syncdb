package com.syncdb.server.verticle;

import com.syncdb.core.protocol.message.NoopMessage;
import com.syncdb.core.util.NetUtils;
import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.core.protocol.SizePrefixProtocolStreamParser;
import com.syncdb.core.protocol.ProtocolMessage;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class ServerVerticle extends AbstractVerticle {
  private NetServer netServer;

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
    if (Boolean.parseBoolean(System.getProperty("syncdb.initRandomPort", "false"))) {
      int port = NetUtils.getRandomPort();
      netServerOptions.setPort(port);
      System.setProperty("syncdb.serverPort", String.valueOf(port));
    }

    return vertx
        .createNetServer(netServerOptions)
        .connectHandler(this::socketHandler)
        .rxListen()
        .doOnSuccess(server -> this.netServer = server)
        .ignoreElement();
  }

  // todo: fix tablet handlers not wokring on verticles
  private void socketHandler(NetSocket socket) {
    ProtocolStreamHandler streamHandler = new ProtocolStreamHandler(this.vertx);

    long timerId =
        vertx.setPeriodic(
            1_000,
            id -> {
              byte[] noop = ProtocolMessage.serialize(new NoopMessage());
              byte[] len = convertToByteArray(noop.length);
              socket.rxWrite(Buffer.buffer(len).appendBytes(noop)).subscribe();
            });

    socket.closeHandler(v -> vertx.cancelTimer(timerId));

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
