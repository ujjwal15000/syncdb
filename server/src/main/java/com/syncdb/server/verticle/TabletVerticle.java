package com.syncdb.server.verticle;

import com.syncdb.core.protocol.message.NoopMessage;
import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.server.protocol.SizePrefixProtocolStreamParser;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;
import io.vertx.rxjava3.core.shareddata.AsyncMap;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class TabletVerticle extends AbstractVerticle {
  private NetServer netServer;
  private final ConcurrentHashMap<ClientMetadata, Long> socketMap = new ConcurrentHashMap<>();

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
    return vertx
        .createNetServer(netServerOptions)
        .connectHandler(this::socketHandler)
        .rxListen()
        .doOnSuccess(server -> this.netServer = server)
        .ignoreElement();
  }

  private void socketHandler(NetSocket socket) {
    ProtocolStreamHandler streamHandler = new ProtocolStreamHandler(this.vertx, this.socketMap);

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
