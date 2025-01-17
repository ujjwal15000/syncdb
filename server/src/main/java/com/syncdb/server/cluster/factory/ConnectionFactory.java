package com.syncdb.server.cluster.factory;

import com.syncdb.core.protocol.SocketQueue;
import com.syncdb.server.cluster.Controller;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.NetClientOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.net.NetClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.model.ExternalView;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ConnectionFactory {
  private static final Integer DEFAULT_NUM_CONNECTIONS = 2;

  private final Vertx vertx;
  private final Controller controller;
  private final String namespace;

  private final NetClient netClient;
  private final ConcurrentHashMap<Integer, String> writers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, ConcurrentHashSet<String>> readers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>> sockets =
      new ConcurrentHashMap<>();

  ConnectionFactory(Vertx vertx, Controller controller, String namespace) throws Exception {
    this.vertx = vertx;
    this.controller = controller;
    this.namespace = namespace;

    // todo: tweak this
    this.netClient =
        vertx.createNetClient(
            new NetClientOptions()
                .setIdleTimeout(30_000)
                .setLogActivity(false)
                .setConnectTimeout(20_000));

    ExternalViewChangeListener listener =
        (externalViewList, changeContext) -> {
          for (ExternalView view : externalViewList) {
            if (Objects.equals(view.getResourceName().split("__")[0], namespace)) {
              this.refresh(view);
            }
          }
        };
    controller.getManager().addExternalViewChangeListener(listener);
  }

  private Completable refresh(ExternalView externalView) {
    Map<String, Map<String, String>> stateMap = externalView.getRecord().getMapFields();
    Map<Integer, String> newWriters = new HashMap<>();
    Map<Integer, Set<String>> newReaders = new HashMap<>();

    Set<String> oldNodes = sockets.keySet();
    Set<String> newNodes = new HashSet<>();

    for (String partition : stateMap.keySet()) {
      int id = Integer.parseInt(partition.split("__")[1].split("_")[1]);

      for (String instanceId : stateMap.get(partition).keySet()) {
        if (Objects.equals(stateMap.get(partition).get(instanceId), "MASTER")) {
          newWriters.put(id, instanceId);
          newNodes.add(instanceId);
        } else if (Objects.equals(stateMap.get(partition).get(instanceId), "SLAVE")) {
          if (!newReaders.containsKey(id)) newReaders.put(id, new ConcurrentHashSet<>());
          newReaders.get(id).add(instanceId);
          newNodes.add(instanceId);
        } else
          log.error(
              "unknown state: {} for namespace: {} and partition: {} and instanceId: {}",
              stateMap.get(partition).get(instanceId),
              namespace,
              id,
              instanceId);
      }
    }
    Set<String> oldNodesToRemove = new HashSet<>(oldNodes);
    oldNodesToRemove.removeAll(newNodes);
    newNodes.removeAll(oldNodes);
    return Completable.merge(
            newNodes.stream().map(this::connect).collect(Collectors.toUnmodifiableList()))
        .doOnComplete(
            () -> {
              for (Integer id : newWriters.keySet()) {
                writers.put(id, newWriters.get(id));
              }

              for (Integer id : newReaders.keySet()) {
                if (!readers.containsKey(id)) readers.put(id, new ConcurrentHashSet<>());
                ConcurrentHashSet<String> set = new ConcurrentHashSet<>();
                set.addAll(newReaders.get(id));
                readers.put(id, set);
              }

              for (String node : oldNodesToRemove) {
                if (sockets.get(node) != null) {
                  sockets.get(node).forEach(SocketQueue::close);
                }
              }
            });
  }

  public Completable connect(String instanceId) {
    String host = instanceId.split("_")[0];
    int port = Integer.parseInt(instanceId.split("_")[1]);
    return Observable.range(0, DEFAULT_NUM_CONNECTIONS)
        .flatMapCompletable(
            ignore ->
                netClient
                    .rxConnect(port, host)
                    .map(
                        socket -> {
                          SocketQueue socketQueue = new SocketQueue(instanceId, socket);
                          if (!sockets.containsKey(instanceId))
                            sockets.put(instanceId, new ConcurrentHashSet<>());
                          sockets.get(instanceId).add(socketQueue);

                          socket.closeHandler(
                              v -> {
                                sockets.get(instanceId).remove(socketQueue);
                                socketQueue.close();
                              });
                          return socket;
                        })
                    .ignoreElement());
  }

  public void close() {
    this.writers.clear();
    this.readers.clear();
    this.sockets.keySet().forEach(r -> sockets.get(r).forEach(SocketQueue::close));
  }
}
