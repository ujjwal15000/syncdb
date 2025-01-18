package com.syncdb.server.verticle;

import com.syncdb.core.protocol.message.ErrorMessage;
import com.syncdb.core.protocol.message.NoopMessage;
import com.syncdb.core.util.NetUtils;
import com.syncdb.server.cluster.TabletConsumerManager;
import com.syncdb.server.cluster.factory.ConnectionFactory;
import com.syncdb.server.cluster.factory.TabletMailboxFactory;
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
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

import static com.syncdb.core.constant.Constants.*;
import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

@Slf4j
public class ServerVerticle extends AbstractVerticle {
  private final String verticleId = UUID.randomUUID().toString();

  private NetServer netServer;
  private TabletConsumerManager consumerManager;
  private ConnectionFactory connectionFactory;

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
    int port = Integer.parseInt(System.getProperty("syncdb.serverPort", "9009"));
    netServerOptions.setPort(port);
    TabletMailboxFactory mailboxFactory =
        (TabletMailboxFactory)
            vertx.sharedData().getLocalMap(FACTORY_MAP_NAME).get(TabletMailboxFactory.FACTORY_NAME);
    this.consumerManager =
        TabletConsumerManager.create(
            io.vertx.rxjava3.core.Context.newInstance(this.context), mailboxFactory);
    this.connectionFactory =
        vertx
            .sharedData()
            .<String, ConnectionFactory>getLocalMap(CONNECTION_FACTORY_MAP_NAME)
            .get(CONNECTION_FACTORY_NAME);

    return vertx
        .createNetServer(netServerOptions)
        .connectHandler(this::socketHandler)
        .rxListen()
        .doOnSuccess(
            server -> {
              this.netServer = server;
              this.consumerManager.start();
            })
        .ignoreElement()
        .andThen(connectionFactory.register(verticleId));
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
        .concatMapCompletable(
            message ->
                streamHandler
                    .handle(message, socket)
                    .map(ProtocolMessage::serialize)
                    .onErrorResumeNext(
                        e -> {
                          log.error("message processing error: ", e);
                          ProtocolMessage errorMessage = new ErrorMessage(message.getSeq(), e);
                          return Flowable.just(ProtocolMessage.serialize(errorMessage));
                        })
                    .concatMapCompletable(
                        data -> {
                          byte[] len = convertToByteArray(data.length);
                          return socket.rxWrite(Buffer.buffer(len).appendBytes(data));
                        }))
        .subscribe();
  }

  @Override
  public Completable rxStop() {
    return netServer.rxClose();
  }
}
