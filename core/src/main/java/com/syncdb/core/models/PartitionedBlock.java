package com.syncdb.core.models;

import java.io.Serializable;
import java.util.regex.Pattern;

public class PartitionedBlock implements Serializable {
  // copies hadoop format
  private static final String FILE_NAME_TEMPLATE = "part-{PARTITION-ID}-{CLIENT-ID}-c{PART-NUMBER}.sdb";

  private final String clientId;

  private PartitionedBlock(String clientId) {
    this.clientId = clientId;
  }

  public static PartitionedBlock create(String clientId) {
    return new PartitionedBlock(clientId);
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

  public static Pattern getFilePattern() {
    String regex =
            FILE_NAME_TEMPLATE
                    .replace("{PARTITION-ID}", "(\\d{5})")
                    .replace("{CLIENT-ID}", "([a-fA-F0-9\\-]+)")
                    .replace("{PART-NUMBER}", "(\\d{3})");
    return Pattern.compile(regex);
  }
}
