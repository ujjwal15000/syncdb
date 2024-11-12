package com.syncdb.core.models;

import java.util.regex.Pattern;

public class BlockBuilder {
  private static final String PATH_TEMPLATE = "{CLIENT-ID}-{TIMESTAMP}-part-{PARTITION-ID}";

  private final String clientId;
  private final Integer partitionId;

  private BlockBuilder(String clientId, Integer partitionId) {
    this.clientId = clientId;
    this.partitionId = partitionId;
  }

  private BlockBuilder create(String clientId, Integer partitionId) {
    return new BlockBuilder(clientId, partitionId);
  }

  private String next() {
    return PATH_TEMPLATE
        .replace("{CLIENT-ID}", clientId)
        .replace("{TIMESTAMP}", String.valueOf(System.currentTimeMillis()))
        .replace("{PARTITION-ID}", partitionId.toString());
  }

  public Pattern toRegexPattern() {
    String regex =
        PATH_TEMPLATE
            .replace("{CLIENT-ID}", "[a-fA-F0-9\\-]+")
            .replace("{TIMESTAMP}", "\\d{13}")
            .replace("{PARTITION-ID}", "\\d+");
    return Pattern.compile(regex);
  }
}
