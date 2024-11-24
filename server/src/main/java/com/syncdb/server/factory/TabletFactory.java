package com.syncdb.server.factory;

import com.syncdb.tablet.Tablet;
import com.syncdb.tablet.TabletConfig;

import java.util.concurrent.ConcurrentHashMap;

public class TabletFactory {
  private static final ConcurrentHashMap<TabletConfig, Tablet> tabletMap =
      new ConcurrentHashMap<>();

  // todo: convert this to create and put
  public static void add(Tablet tablet) {
    tabletMap.put(tablet.getTabletConfig(), tablet);
  }

  public static Tablet get(TabletConfig config) {
    return tabletMap.get(config);
  }

  public static long getCurrentWriteRate(TabletConfig config) {
    return tabletMap.get(config).getRateLimiter().getSingleBurstBytes();
  }

}
