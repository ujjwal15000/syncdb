package com.syncdb.server.verticle;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.server.protocol.SizePrefixProtocolStreamParser;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;
import java.util.concurrent.ConcurrentHashMap;

public class StorageVerticle extends AbstractVerticle {
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

  private void socketHandler(NetSocket socket) {

  }

  @Override
  public Completable rxStop() {
    return netServer.rxClose();
  }
}
