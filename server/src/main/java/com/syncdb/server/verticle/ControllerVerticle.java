package com.syncdb.server.verticle;

import static com.syncdb.core.constant.Constants.HELIX_POOL_NAME;
import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;
import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.server.cluster.Controller;
import com.syncdb.server.cluster.ZKAdmin;
import com.syncdb.server.cluster.config.HelixConfig;
import com.syncdb.server.factory.NamespaceConfig;
import com.syncdb.server.factory.NamespaceFactory;
import com.syncdb.server.factory.NamespaceMetadata;
import com.syncdb.server.protocol.ProtocolStreamHandler;
import com.syncdb.server.protocol.SizePrefixProtocolStreamParser;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.core.http.HttpServerRequest;
import io.vertx.rxjava3.core.http.HttpServerResponse;
import io.vertx.rxjava3.core.net.NetServer;
import io.vertx.rxjava3.core.net.NetSocket;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ControllerVerticle extends AbstractVerticle {
  private final Controller controller;
  private final ZKAdmin admin;

  private final ObjectMapper objectMapper;
  private WorkerExecutor executor;
  private HttpServer httpServer;

  private static HttpServerOptions httpServerOptions =
      new HttpServerOptions()
          .setHost("0.0.0.0")
          .setPort(80)
          .setIdleTimeout(20)
          .setLogActivity(false)
          .setCompressionSupported(true)
          .setCompressionLevel(1)
          .setReuseAddress(true)
          .setReusePort(true)
          .setTcpFastOpen(true)
          .setTcpNoDelay(true)
          .setTcpQuickAck(true)
          .setTcpKeepAlive(true)
          .setUseAlpn(false);

  public ControllerVerticle(Controller controller, ZKAdmin admin) {
    this.controller = controller;
    this.admin = admin;
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public Completable rxStart() {
    this.executor = vertx.createSharedWorkerExecutor(HELIX_POOL_NAME, 4);
    return vertx
        .createHttpServer(httpServerOptions)
        .requestHandler(getRouter())
        .rxListen()
        .doOnSuccess(server -> this.httpServer = server)
        .ignoreElement();
  }

  private Router getRouter() {
  // todo: add namespace atomically
    Router router = Router.router(vertx);
    router
        .route("/namespace")
        .method(HttpMethod.POST)
        .consumes("*/json")
        .handler(BodyHandler.create())
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();
              NamespaceMetadata metadata;
              try {
                metadata = objectMapper.readValue(body.toString(), NamespaceMetadata.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(
                    String.format("invalid request body: %s", e.getMessage()));
              }
              executor
                  .rxExecuteBlocking(
                      () -> {
                        NamespaceFactory.add(controller.getPropertyStore(), metadata);
                        admin.addNamespace(
                            metadata.getName(),
                            metadata.getNumNodes(),
                            metadata.getNumPartitions(),
                            metadata.getNumReplicas());
                        return true;
                      })
                  .subscribe(
                      ignore -> {
                        HttpServerResponse response = ctx.response();
                        response.putHeader("content-type", "application/json");
                        response.setStatusCode(204);
                        response.end();
                      },
                      e -> errorHandler(e, ctx));
            });

    // todo: clean up error handling
    router
        .route("/namespace")
        .method(HttpMethod.GET)
        .handler(
            ctx -> {
              List<String> name = ctx.queryParam("name");
              if (name.size() != 1) {
                throw new RuntimeException("invalid request");
              }

              executor
                  .rxExecuteBlocking(
                      () -> NamespaceFactory.get(controller.getPropertyStore(), name.get(0)))
                  .subscribe(
                      metadata -> {
                        String responseBody;
                        try {
                          responseBody = objectMapper.writeValueAsString(metadata);
                        } catch (JsonProcessingException e) {
                          throw new RuntimeException(e);
                        }

                        HttpServerResponse response = ctx.response();
                        response.putHeader("content-type", "application/json");
                        response.setStatusCode(200);
                        response.end(responseBody);
                      },
                      e -> errorHandler(e, ctx));
            });

    vertx
        .eventBus()
        .consumer("namespace-metadata")
        .handler(
            r ->
                executor
                    .rxExecuteBlocking(
                        () ->
                            NamespaceFactory.get(
                                controller.getPropertyStore(), r.body().toString()))
                    .subscribe(metadata -> r.reply(objectMapper.writeValueAsString(metadata))));

    return router;
  }

  private void errorHandler(Throwable e, RoutingContext ctx) {
    log.error(e.getMessage(), e);
    String responseBody =
        new JsonObject(
                Map.of(
                    "error",
                    Map.of(
                        "message",
                        e.getMessage(),
                        "cause",
                        e.getCause() == null ? e.getMessage() : e.getCause())))
            .toString();

    HttpServerResponse response = ctx.response();
    response.putHeader("content-type", "application/json");
    response.setStatusCode(400);
    response.end(responseBody);
  }

  @Override
  public Completable rxStop() {
    return httpServer.rxClose();
  }
}
