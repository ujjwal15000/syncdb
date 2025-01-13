package com.syncdb.server.cluster;

import com.syncdb.server.cluster.factory.TabletMailbox;
import com.syncdb.server.cluster.factory.TabletMailboxFactory;
import com.syncdb.tablet.TabletConfig;
import io.vertx.rxjava3.core.Context;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;

public class TabletConsumerManager {
  @Getter public static final String SYNCDB_TABLET_READER_DEPLOYER = "SYNCDB_TABLET_READER_DEPLOYER";
  @Getter public static final String SYNCDB_TABLET_READER_UN_DEPLOYER = "SYNCDB_TABLET_READER_UN_DEPLOYER";
  @Getter public static final String SYNCDB_TABLET_WRITER_DEPLOYER = "SYNCDB_TABLET_WRITER_DEPLOYER";
  @Getter public static final String SYNCDB_TABLET_WRITER_UN_DEPLOYER = "SYNCDB_TABLET_WRITER_UN_DEPLOYER";

  private final ConcurrentHashMap<TabletConfig, MessageConsumer<byte[]>> readers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<TabletConfig, MessageConsumer<byte[]>> writers = new ConcurrentHashMap<>();

  private final Context context;
  private final TabletMailboxFactory mailboxFactory;

  TabletConsumerManager(Context context, TabletMailboxFactory mailboxFactory) {
    this.context = context;
    this.mailboxFactory = mailboxFactory;
  }

  public static TabletConsumerManager create(Context context, TabletMailboxFactory mailboxFactory) {
    return new TabletConsumerManager(context, mailboxFactory);
  }

  public void start() {
    context.owner().eventBus().<byte[]>consumer(SYNCDB_TABLET_READER_DEPLOYER, message -> {
      TabletConfig config = TabletConfig.deserialize(message.body());
      MessageConsumer<byte[]> reader = TabletMailbox.registerReaderOnVerticle(context, mailboxFactory.get(config));
      readers.put(config, reader);
    });

    context.owner().eventBus().<byte[]>consumer(SYNCDB_TABLET_READER_UN_DEPLOYER, message -> {
      TabletConfig config = TabletConfig.deserialize(message.body());
      MessageConsumer<byte[]> reader = readers.get(config);
      reader.rxUnregister().subscribe();
      readers.remove(config);
    });

    context.owner().eventBus().<byte[]>consumer(SYNCDB_TABLET_WRITER_DEPLOYER, message -> {
      TabletConfig config = TabletConfig.deserialize(message.body());
      MessageConsumer<byte[]> writer = TabletMailbox.registerWriterOnVerticle(context, mailboxFactory.get(config));
      writers.put(config, writer);
    });

    context.owner().eventBus().<byte[]>consumer(SYNCDB_TABLET_WRITER_UN_DEPLOYER, message -> {
      TabletConfig config = TabletConfig.deserialize(message.body());
      MessageConsumer<byte[]> writer = writers.get(config);
      writer.rxUnregister().subscribe();
      writers.remove(config);
    });
  }
}
