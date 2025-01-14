package com.syncdb.server.verticle;

import static com.syncdb.core.constant.Constants.HELIX_POOL_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.core.models.AddBucketRequest;
import com.syncdb.core.models.DropBucketRequest;
import com.syncdb.core.models.NamespaceRecord;
import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.util.NetUtils;
import com.syncdb.server.cluster.Controller;
import com.syncdb.server.cluster.ZKAdmin;
import com.syncdb.server.cluster.factory.BucketConfig;
import com.syncdb.server.cluster.factory.NamespaceFactory;
import com.syncdb.server.cluster.factory.NamespaceMetadata;
import com.syncdb.server.cluster.factory.NamespaceStatus;
import com.syncdb.server.protocol.ProtocolStreamHandler;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.core.http.HttpServerResponse;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.RoutingContext;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

// todo: maybe add all these to redirect to leader???
// todo: add cleanup jobs for leader
// todo: add namespace creation constraints
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
          .setPort(8000)
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
    super();
    this.controller = controller;
    this.admin = admin;
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public Completable rxStart() {
    if (Boolean.parseBoolean(System.getProperty("syncdb.initRandomPort", "false"))) {
      int port = NetUtils.getRandomPort();
      httpServerOptions.setPort(port);
      System.setProperty("syncdb.controllerPort", String.valueOf(port));
    }

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
    initNamespaceRouter(router);
    initBucketRouter(router);
    initDataRouter(router);

    vertx
        .eventBus()
        .consumer("namespace-metadata")
        .handler(
            r ->
                executor
                    .rxExecuteBlocking(
                        () ->
                            NamespaceFactory.getMetadata(
                                controller.getPropertyStore(), r.body().toString()))
                    .subscribe(metadata -> r.reply(objectMapper.writeValueAsString(metadata))));

    return router;
  }

  private void initDataRouter(Router router) {
    ProtocolStreamHandler streamHandler = new ProtocolStreamHandler(this.vertx);
    router
        .route("/data")
        .method(HttpMethod.POST)
        .consumes("*/json")
        .handler(BodyHandler.create())
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();
              NamespaceRecord record;
              try {
                record = objectMapper.readValue(body.toString(), NamespaceRecord.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(
                    String.format("invalid request body: %s", e.getMessage()));
              }

              streamHandler
                  .handleWrite(
                      new WriteMessage(
                          0,
                          List.of(
                              Record.<byte[], byte[]>builder()
                                  .key(record.getKey().getBytes(StandardCharsets.UTF_8))
                                  .value(record.getValue().getBytes(StandardCharsets.UTF_8))
                                  .build()),
                          record.getNamespace()))
                  .map(
                      r -> {
                        if (r.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR)
                          throw new RuntimeException(ErrorMessage.getThrowable(r.getPayload()));
                        return r;
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
        .route("/data")
        .method(HttpMethod.GET)
        .handler(
            ctx -> {
              List<String> namespace = ctx.queryParam("namespace");
              List<String> key = ctx.queryParam("key");
              if (namespace.size() != 1 || key.size() != 1) {
                throw new RuntimeException("invalid request");
              }

              streamHandler
                  .handleRead(
                      new ReadMessage(
                          0,
                          List.of(key.get(0).getBytes(StandardCharsets.UTF_8)),
                          namespace.get(0)))
                  .map(
                      r -> {
                        if (r.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR)
                          throw new RuntimeException(ErrorMessage.getThrowable(r.getPayload()));
                        return r;
                      })
                  .flatMapIterable(r -> ReadAckMessage.deserializePayload(r.getPayload()))
                  .switchIfEmpty(Flowable.error(new RuntimeException("not found")))
                  .subscribe(
                      record -> {
                        String responseBody;
                        try {
                          responseBody =
                              objectMapper.writeValueAsString(
                                  Map.of(
                                      "key",
                                      new String(record.getKey()),
                                      "value",
                                      new String(record.getValue())));
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
  }

  private void initNamespaceRouter(Router router) {
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
                      () ->
                          NamespaceFactory.getMetadata(controller.getPropertyStore(), name.get(0)))
                  .subscribe(
                      metadata -> {
                        NamespaceStatus.StatusHostMapPair pair = admin.getNamespaceStatus(metadata);
                        NamespaceStatus namespaceStatus =
                            NamespaceStatus.create(metadata, pair.getStatus(), pair.getHostMap());
                        String responseBody;
                        try {
                          responseBody = objectMapper.writeValueAsString(namespaceStatus);
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
  }

  private void initBucketRouter(Router router) {
    router
        .route("/bucket")
        .method(HttpMethod.POST)
        .consumes("*/json")
        .handler(BodyHandler.create())
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();
              AddBucketRequest addBucketRequest;
              try {
                addBucketRequest = objectMapper.readValue(body.toString(), AddBucketRequest.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(
                    String.format("invalid request body: %s", e.getMessage()));
              }
              executor
                  .rxExecuteBlocking(
                      () -> {
                        NamespaceFactory.addBucket(
                            controller.getPropertyStore(),
                            addBucketRequest.getNamespace(),
                            BucketConfig.create(
                                addBucketRequest.getName(), addBucketRequest.getTtl()));
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
        .route("/bucket")
        .method(HttpMethod.DELETE)
        .consumes("*/json")
        .handler(BodyHandler.create())
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();
              DropBucketRequest dropBucketRequest;
              try {
                dropBucketRequest =
                    objectMapper.readValue(body.toString(), DropBucketRequest.class);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(
                    String.format("invalid request body: %s", e.getMessage()));
              }
              executor
                  .rxExecuteBlocking(
                      () -> {
                        NamespaceFactory.dropBucket(
                            controller.getPropertyStore(),
                            dropBucketRequest.getNamespace(),
                            dropBucketRequest.getName());
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
                        e.getCause() == null ? e.getMessage() : e.getCause().getMessage())))
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
