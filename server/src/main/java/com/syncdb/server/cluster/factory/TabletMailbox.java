package com.syncdb.server.cluster.factory;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.util.ByteArrayUtils;
import com.syncdb.core.util.TimeUtils;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;

// todo: start these on verticles
@Slf4j
public class TabletMailbox {
  private final Vertx vertx;
  private final TabletConfig config;
  @Getter
  private final Tablet tablet;
  private final WorkerExecutor executor;
  private long syncUpTimerId;

  private TabletMailbox(Vertx vertx, Tablet tablet, TabletConfig config) {
    this.vertx = vertx;
    this.config = config;
    this.tablet = tablet;
    this.executor = vertx.createSharedWorkerExecutor(WORKER_POOL_NAME, 32);
  }

  public static TabletMailbox create(Vertx vertx, Tablet tablet, TabletConfig config) {
    return new TabletMailbox(vertx, tablet, config);
  }

  public void startWriter() {
    this.tablet.openIngestor();
  }

  // todo add these to configs
  public void startReader() {
    this.tablet.openReader();

    long currentSystemTime = System.currentTimeMillis();
    long adjustedTime = currentSystemTime + (TimeUtils.DELTA != null ? TimeUtils.DELTA : 0);

    long interval = 5000;
    long delay = interval - (adjustedTime % interval);

    syncUpTimerId =
        vertx.setPeriodic(
            delay,
            interval,
            t ->
                executeBlocking(
                        () -> {
                          tablet.getSecondary().catchUp();
                          return true;
                        })
                    .subscribe());
  }

  public void closeWriter() {
    vertx.setTimer(5_000, l -> this.tablet.closeIngestor());
  }

  public void closeReader() {
    vertx.cancelTimer(syncUpTimerId);
    vertx.setTimer(5_000, l -> this.tablet.closeReader());
  }

  // todo add bucket config validations
  public Flowable<MailboxMessage> handleRead(ProtocolMessage message) {
    ReadMessage.Message readMessage = ReadMessage.deserializePayload(message.getPayload());
    List<byte[]> keys = readMessage.getKeys();
    // todo: make this more efficient
    NamespaceConfig namespaceConfig = NamespaceFactory.get(readMessage.getNamespace());
    int ttl = namespaceConfig.getBuckets().get(readMessage.getBucket()).getTtl();
    return executeBlocking(() -> tablet.getSecondary().bulkRead(keys, readMessage.getBucket()))
        .map(
            values -> {
              List<Record<byte[], byte[]>> records = new ArrayList<>();
              for (int i = 0; i < values.size(); i++) {
                byte[] value = values.get(i);
                records.add(
                    Record.<byte[], byte[]>builder()
                        .key(keys.get(i))
                        .value(value == null ? new byte[0] : parseExpiredValue(value, ttl))
                        .build());
              }
              return ReadAckMessage.serializePayload(records);
            })
        .map(MailboxMessage::success)
        .onErrorResumeNext(
            e -> {
              log.info("read failed on tablet ", e);
              return Flowable.just(
                  MailboxMessage.failed(
                      String.format(
                          "read failed on tablet with error: %s",
                          e.getMessage())));
            });
  }

  private static byte[] parseExpiredValue(byte[] data, int ttl) {
    if (data.length < 4) {
      throw new IllegalArgumentException("Data must be at least 4 bytes long.");
    }

    ByteBuffer buffer = ByteBuffer.wrap(data);
    byte[] value = new byte[data.length - 4];
    buffer.get(value);

    int timestamp = buffer.order(ByteOrder.LITTLE_ENDIAN).getInt();
    // rocksdb appends in seconds
    if (ttl == 0 || System.currentTimeMillis() / 1000 < timestamp + ttl) {
      return value;
    }
    return new byte[0];
  }

  public Flowable<MailboxMessage> handleWrite(ProtocolMessage message) {
    WriteMessage.Message writeMessage = WriteMessage.deserializePayload(message.getPayload());
    return this.executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records = writeMessage.getRecords();
              tablet.getIngestor().write(records, writeMessage.getBucket());
              return true;
            })
        .map(ignore -> MailboxMessage.success(new byte[0]))
        .onErrorResumeNext(
            e -> {
              log.info("write failed on tablet ", e);
              return Flowable.just(
                  MailboxMessage.failed(
                      String.format(
                          "write failed on tablet with error: %s",
                          e.getMessage())));
            });
  }

  public <V> Flowable<V> executeBlocking(Callable<V> callable) {
    return executor.rxExecuteBlocking(callable, false).toFlowable();
  }

  public void close() {
    this.tablet.close();
  }
}
