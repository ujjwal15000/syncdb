package com.syncdb.core.models;

import java.util.regex.Pattern;

public class PartitionedBlockBuilder {
  private static final String PATH_TEMPLATE = "{TIMESTAMP}-{CLIENT-ID}";
  // copies hadoop format
  private static final String FILE_NAME_TEMPLATE = "part-{PARTITION-ID}-{CLIENT-ID}-c{PART-NUMBER}";

  private final String rootPath;
  private final String clientId;

  private PartitionedBlockBuilder(String clientId) {
    this.rootPath = null;
    this.clientId = clientId;
  }

  private PartitionedBlockBuilder(String rootPath, String clientId) {
    this.rootPath = rootPath;
    this.clientId = clientId;
  }

  public static PartitionedBlockBuilder create(String rootPath, String clientId) {
    return new PartitionedBlockBuilder(rootPath, clientId);
  }

  public static PartitionedBlockBuilder create(String clientId) {
    return new PartitionedBlockBuilder(clientId);
  }

  public String buildNext() {
    String fileName =
        PATH_TEMPLATE
            .replace("{TIMESTAMP}", String.valueOf(System.currentTimeMillis()))
            .replace("{CLIENT-ID}", clientId);
    return rootPath == null ? fileName : rootPath + "/" + fileName;
  }

  public String buildNextForPartition(String prefix, Integer partitionId, Integer partNumber) {
    String fileName =
        FILE_NAME_TEMPLATE
            .replace("{PARTITION-ID}", padId(partitionId, 5))
            .replace("{CLIENT-ID}", clientId)
            .replace("{PART-NUMBER}", padId(partNumber, 3));
    return prefix + "/" + fileName;
  }

  public static String padId(Integer id, Integer maxLength) {
    return String.format("%0" + maxLength + "d", id);
  }

  public Pattern toRegexPattern() {
    String regex =
        PATH_TEMPLATE
                .replace("{TIMESTAMP}", "\\d{13}")
                .replace("{CLIENT-ID}", "[a-fA-F0-9\\-]+");
    return Pattern.compile(regex);
  }
}
