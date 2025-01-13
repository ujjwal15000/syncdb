package com.syncdb.server.cluster.factory;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import com.syncdb.core.util.TimeUtils;
import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.rxjava3.core.Context;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.WorkerExecutor;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.syncdb.core.constant.Constants.WORKER_POOL_NAME;
import static com.syncdb.server.cluster.TabletConsumerManager.*;

// todo: start these on verticles
@Slf4j
public class TabletMailbox {
  private final Vertx vertx;
  private final TabletConfig config;
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
    vertx
        .eventBus()
        .publish(
            SYNCDB_TABLET_WRITER_DEPLOYER,
            TabletConfig.serialize(this.config),
            new DeliveryOptions().setLocalOnly(true));
  }

  // todo add these to configs
  public void startReader() {
    this.tablet.openReader();
    vertx
        .eventBus()
        .publish(
            SYNCDB_TABLET_READER_DEPLOYER,
            TabletConfig.serialize(this.config),
            new DeliveryOptions().setLocalOnly(true));

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

  public static MessageConsumer<byte[]> registerReaderOnVerticle(
      Context context, TabletMailbox mailbox) {
    return context
        .owner()
        .eventBus()
        .<byte[]>consumer(
            getReaderAddress(mailbox.config.getNamespace(), mailbox.config.getPartitionId()))
        .handler(
            message ->
                mailbox
                    .readHandler(ProtocolMessage.deserialize(message.body()))
                    .map(MailboxMessage::serialize)
                    .subscribe(message::reply, e -> log.error("unexpected error on tablet: ", e)));
  }

  public static MessageConsumer<byte[]> registerWriterOnVerticle(
      Context context, TabletMailbox mailbox) {
    return context
        .owner()
        .eventBus()
        .<byte[]>consumer(
            getWriterAddress(mailbox.config.getNamespace(), mailbox.config.getPartitionId()))
        .handler(
            message ->
                mailbox
                    .writeHandler(ProtocolMessage.deserialize(message.body()))
                    .map(MailboxMessage::serialize)
                    .subscribe(message::reply, e -> log.error("unexpected error on tablet: ", e)));
  }

  public void closeWriter() {
    vertx.eventBus()
            .publish(SYNCDB_TABLET_WRITER_UN_DEPLOYER, TabletConfig.serialize(config));
    vertx.setTimer(5_000, l -> this.tablet.closeIngestor());
  }

  public void closeReader() {
    vertx.cancelTimer(syncUpTimerId);
    vertx.eventBus()
            .publish(SYNCDB_TABLET_READER_UN_DEPLOYER, TabletConfig.serialize(config));
    vertx.setTimer(5_000, l -> this.tablet.closeReader());
  }

  public Flowable<MailboxMessage> writeHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case WRITE:
        return this.handleWrite(message);
      default:
        return Flowable.just(
            MailboxMessage.failed(
                String.format(
                    "handler not implemented for message type: %s",
                    message.getMessageType().name())));
    }
  }

  public Flowable<MailboxMessage> readHandler(ProtocolMessage message) {
    switch (message.getMessageType()) {
      case READ:
        return this.handleRead(message);
      default:
        return Flowable.just(
            MailboxMessage.failed(
                String.format(
                    "handler not implemented for message type: %s",
                    message.getMessageType().name())));
    }
  }

  private Flowable<MailboxMessage> handleRead(ProtocolMessage message) {
    List<byte[]> keys = ReadMessage.deserializePayload(message.getPayload()).getKeys();
    return executeBlocking(() -> tablet.getSecondary().bulkRead(keys))
        .map(
            values -> {
              List<Record<byte[], byte[]>> records = new ArrayList<>();
              for (int i = 0; i < values.size(); i++) {
                byte[] value = values.get(i);
                records.add(
                    Record.<byte[], byte[]>builder()
                        .key(keys.get(i))
                        .value(value == null ? new byte[0] : value)
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

  private Flowable<MailboxMessage> handleWrite(ProtocolMessage message) {
    return this.executeBlocking(
            () -> {
              List<Record<byte[], byte[]>> records =
                  WriteMessage.deserializePayload(message.getPayload()).getRecords();

              WriteBatch writeBatch = new WriteBatch();
              for (Record<byte[], byte[]> record : records)
                writeBatch.put(record.getKey(), record.getValue());
              tablet.getIngestor().write(writeBatch);
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

  public static String getWriterAddress(String namespace, Integer partitionId) {
    return namespace + "_" + partitionId + "_WRITER";
  }

  public static String getReaderAddress(String namespace, Integer partitionId) {
    return namespace + "_" + partitionId + "_READER";
  }

  public void close() {
    this.tablet.close();
  }
}
