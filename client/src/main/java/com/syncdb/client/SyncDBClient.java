package com.syncdb.client;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.SocketQueue;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.net.NetClientOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.net.NetClient;
import lombok.extern.slf4j.Slf4j;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SyncDBClient {
  private final SecureRandom secureRandom = new SecureRandom();
  private final Vertx vertx;
  private final NetClient netClient;
  private final ConcurrentHashMap<String, SocketQueue> sockets;
  private final SyncDBClientConfig config;

  SyncDBClient(Vertx vertx, SyncDBClientConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.netClient =
        vertx.createNetClient(
            new NetClientOptions()
                .setIdleTimeout(30_000)
                .setLogActivity(false)
                .setConnectTimeout(20_000));
    this.sockets = new ConcurrentHashMap<>();
  }

  public static SyncDBClient create(Vertx vertx, SyncDBClientConfig config) {
    return new SyncDBClient(vertx, config);
  }

  // todo: make connections to different nodes
  public Completable connect() {
    return Observable.range(0, config.getNumConnections())
        .flatMapCompletable(
            ignore ->
                netClient
                    .rxConnect(config.getPort(), config.getHost())
                    .map(
                        socket -> {
                          String socketId = UUID.randomUUID().toString();
                          SocketQueue socketQueue = new SocketQueue(socketId, socket);
                          sockets.put(socketId, socketQueue);
                          socket.closeHandler(
                              v -> {
                                socketQueue.close();
                                sockets.remove(socketId);
                              });
                          return socket;
                        })
                    .ignoreElement());
  }

  public Completable write(List<Record<byte[], byte[]>> records, String namespace) {
    int index = secureRandom.nextInt(sockets.size());
    return sockets.get((String) sockets.keySet().toArray()[index]).write(records, namespace);
  }

  public Single<List<Record<byte[], byte[]>>> read(List<byte[]> keys, String namespace) {
    int index = secureRandom.nextInt(sockets.size());
    return sockets.get((String) sockets.keySet().toArray()[index]).read(keys, namespace);
  }

  public void close() {
    sockets.forEach(
        (key, value) -> {
          value.close();
          sockets.remove(key).closeGracefully();
        });
  }
}
