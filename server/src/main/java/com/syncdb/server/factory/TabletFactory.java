package com.syncdb.server.factory;

import com.syncdb.tablet.Tablet;

import java.util.concurrent.ConcurrentHashMap;

public class TabletFactory {
  private static final ConcurrentHashMap<Tablet.TabletConfig, Tablet> tabletMap =
      new ConcurrentHashMap<>();

  public static void add(Tablet tablet) {
    tabletMap.put(tablet.getTabletConfig(), tablet);
  }

  public static Tablet get(Tablet.TabletConfig config) {
    return tabletMap.get(config);
  }

    public static long getCurrentWriteRate(Tablet.TabletConfig config) {
        return tabletMap.get(config).getRateLimiter().getSingleBurstBytes();
    }
}
