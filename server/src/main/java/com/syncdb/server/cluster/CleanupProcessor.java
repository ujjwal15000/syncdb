package com.syncdb.server.cluster;

import io.vertx.rxjava3.core.Vertx;

public class CleanupProcessor {
  private final Controller controller;
  private final ZKAdmin zkAdmin;
  private final Vertx vertx;
  private Long purgeJobTimerId;

  private CleanupProcessor(Vertx vertx, Controller controller, ZKAdmin zkAdmin) {
    this.vertx = vertx;
    this.controller = controller;
    this.zkAdmin = zkAdmin;
  }

  public static CleanupProcessor create(Vertx vertx, Controller controller, ZKAdmin zkAdmin) {
    return new CleanupProcessor(vertx, controller, zkAdmin);
  }

  public void start() {
    this.purgeJobTimerId = vertx.setPeriodic(10_000, l -> purgeNodesJob());
  }

  private void purgeNodesJob() {
    if (controller.isLeader()) {
      zkAdmin.purgeOfflineInstances();
    }
  }

  public void close() {
    if (purgeJobTimerId != null) {
      vertx.cancelTimer(this.purgeJobTimerId);
      purgeJobTimerId = null;
    }
  }
}
