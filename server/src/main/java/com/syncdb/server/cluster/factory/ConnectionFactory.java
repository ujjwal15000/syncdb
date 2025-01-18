package com.syncdb.server.cluster.factory;

import com.syncdb.core.protocol.SocketQueue;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.shareddata.Shareable;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.net.NetClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.helix.HelixManager;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.model.ExternalView;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ConnectionFactory implements Shareable {
  private static final Integer DEFAULT_NUM_CONNECTIONS = 2;
  private final SecureRandom secureRandom = new SecureRandom();

  private final Vertx vertx;
  private final HelixManager manager;
  private final String namespace;

  private final NetClient netClient;
  private final ConcurrentHashMap<Integer, String> writers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, ConcurrentHashSet<String>> readers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>>>
      socketMaps = new ConcurrentHashMap<>();

  ConnectionFactory(Vertx vertx, HelixManager manager, String namespace) throws Exception {
    this.vertx = vertx;
    this.manager = manager;
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
              log.info(String.format("external view changed for namespace %s", namespace));
              for (String id : socketMaps.keySet()) {
                log.info(
                    String.format(
                        "refreshing connections for verticleId %s namespace %s", id, namespace));
                this.refresh(view, socketMaps.get(id))
                    .doOnError(
                        e ->
                            log.error(
                                String.format(
                                    "error refreshing connections for verticleId %s namespace %s",
                                    id, namespace),
                                e))
                    .subscribe();
              }
            }
          }
        };
    manager.addExternalViewChangeListener(listener);
  }

  public Completable register(String verticleId) {
    ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>> map = new ConcurrentHashMap<>();
    return initMap(map)
        .doOnComplete(
            () -> {
              log.info(
                  String.format(
                      "registered connection map for verticleId %s namespace %s",
                      verticleId, namespace));
              socketMaps.put(verticleId, map);
            })
        .doOnError(
            e -> log.error(
                String.format(
                    "error registering connection map for verticleId %s namespace %s",
                    verticleId, namespace),
                e));
  }

  public SocketQueue getReader(Integer partitionId, String verticleId) {
    if (!socketMaps.containsKey(verticleId))
      throw new RuntimeException(
          String.format("connection map not found for verticle %s", verticleId));
    if (!readers.containsKey(partitionId))
      throw new RuntimeException(String.format("node not found for partitionId %s", partitionId));

    List<String> nodes = new ArrayList<>(readers.get(partitionId));
    Set<SocketQueue> set =
        socketMaps.get(verticleId).get(nodes.get(secureRandom.nextInt(nodes.size())));
    return new ArrayList<>(set).get(secureRandom.nextInt(set.size()));
  }

  public SocketQueue getWriter(Integer partitionId, String verticleId) {
    if (!socketMaps.containsKey(verticleId))
      throw new RuntimeException(
          String.format("connection map not found for verticle %s", verticleId));
    if (!writers.containsKey(partitionId))
      throw new RuntimeException(String.format("node not found for partitionId %s", partitionId));
    Set<SocketQueue> set = socketMaps.get(verticleId).get(writers.get(partitionId));
    return new ArrayList<>(set).get(secureRandom.nextInt(set.size()));
  }

  public static ConnectionFactory create(Vertx vertx, HelixManager manager, String namespace)
      throws Exception {
    return new ConnectionFactory(vertx, manager, namespace);
  }

  private Completable initMap(ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>> sockets) {
    Set<String> nodes = new HashSet<>(writers.values());
    readers.values().forEach(nodes::addAll);
    return Completable.merge(
        nodes.stream().map(r -> this.connect(r, sockets)).collect(Collectors.toUnmodifiableList()));
  }

  private Completable refresh(
      ExternalView externalView,
      ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>> sockets) {
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
            newNodes.stream()
                .map(r -> this.connect(r, sockets))
                .collect(Collectors.toUnmodifiableList()))
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

  public Completable connect(
      String instanceId, ConcurrentHashMap<String, ConcurrentHashSet<SocketQueue>> sockets) {
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
    this.socketMaps
        .values()
        .forEach(r -> r.values().forEach(map -> map.forEach(SocketQueue::close)));
  }

  public void close(String verticleId) {
    this.socketMaps.get(verticleId).values().forEach(map -> map.forEach(SocketQueue::close));
  }
}
