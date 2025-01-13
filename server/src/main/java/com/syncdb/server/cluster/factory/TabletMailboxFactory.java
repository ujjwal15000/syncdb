package com.syncdb.server.cluster.factory;

import com.syncdb.tablet.TabletConfig;
import io.vertx.core.shareddata.Shareable;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TabletMailboxFactory implements Shareable {
  public static final String FACTORY_NAME = "MAILBOX_FACTORY";
  private final ConcurrentHashMap<TabletConfig, TabletMailbox> factory = new ConcurrentHashMap<>();

  public static TabletMailboxFactory create() {
    return new TabletMailboxFactory();
  }

  public void addToFactory(TabletConfig config, TabletMailbox mailbox) {
    factory.put(config, mailbox);
  }

  public void removeFromFactory(TabletConfig config) {
    factory.remove(config);
  }

  public TabletMailbox get(TabletConfig config) {
    return factory.get(config);
  }
}
