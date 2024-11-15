package com.syncdb.core.models;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PartitionedBlockNameBuilder implements Serializable {
  // copies hadoop format
  private static final String FILE_NAME_TEMPLATE =
      "part-{PARTITION-ID}-{CLIENT-ID}-c{PART-NUMBER}{EXTENSION}";

  private final String clientId;

  private PartitionedBlockNameBuilder(String clientId) {
    this.clientId = clientId;
  }

  public static PartitionedBlockNameBuilder create(String clientId) {
    return new PartitionedBlockNameBuilder(clientId);
  }

  public String build(String prefix, Integer partitionId, Integer partNumber, String fileExtension) {
    String fileName =
        FILE_NAME_TEMPLATE
            .replace("{PARTITION-ID}", padId(partitionId, 5))
            .replace("{CLIENT-ID}", clientId)
            .replace("{PART-NUMBER}", padId(partNumber, 3))
            .replace("{EXTENSION}", fileExtension);
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
            .replace("{PART-NUMBER}", "(\\d{3})")
            .replace("{EXTENSION}", ".*");
    return Pattern.compile(regex);
  }

  @Data
  @Builder
  public static class FileName {
    Integer partitionId;
    String clientId;
    Integer partNumber;

    public static FileName create(String name) {
      Matcher matcher = getFilePattern().matcher(name);
      if (!matcher.matches())
        throw new RuntimeException(String.format("invalid file name: %s", name));
      FileName.FileNameBuilder builder = FileName.builder();
      builder.partitionId(Integer.parseInt(matcher.group(1)));
      builder.clientId(matcher.group(2));
      builder.partNumber(Integer.parseInt(matcher.group(3)));
      return builder.build();
    }

    public static String getPath(FileName fileName, String prefix, String fileExtension) {
      if (prefix.endsWith("/")) return prefix + getName(fileName, fileExtension);
      return prefix + "/" + getName(fileName, fileExtension);
    }

    public static String getName(FileName fileName, String fileExtension) {
      return FILE_NAME_TEMPLATE
          .replace("{PARTITION-ID}", padId(fileName.partitionId, 5))
          .replace("{CLIENT-ID}", fileName.clientId)
          .replace("{PART-NUMBER}", padId(fileName.partNumber, 3))
          .replace("{EXTENSION}", fileExtension);
    }
  }
}
