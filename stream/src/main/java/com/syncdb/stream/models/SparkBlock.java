package com.syncdb.stream.models;

import com.syncdb.stream.util.S3Utils;
import io.reactivex.rxjava3.core.Single;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SparkBlock {
  private static final Pattern PATH_PATTERN = Pattern.compile("^part-(\\d+)-([a-f0-9\\-]+)-c(\\d+)\\.mp$");

  @Data
  @Builder
  public static class ParsedPath {
    private final Long timeId;
    private final Integer partitionId;
    private final String uuid;
    private final Integer count;

    public static ParsedPath parse(String timeId, String path) {
      Matcher matcher = PATH_PATTERN.matcher(path);
      if (matcher.matches()) {
        return ParsedPath.builder()
                .timeId(Long.getLong(timeId))
                .partitionId(Integer.parseInt(matcher.group(1)))
                .uuid(matcher.group(2))
                .count(Integer.parseInt(matcher.group(3)))
                .build();
      } else {
        throw new IllegalArgumentException("Invalid path format: " + path);
      }
    }
  }

  @Data
  @Builder
  public static class CommitMetadata {
    private Long timeId;
    private Integer partitionId;
    private Integer countId;
    private Long offset;
  }

  public static Single<List<ParsedPath>> getBlocks(
          S3AsyncClient s3AsyncClient, String bucket, String rootPath) {
    return S3Utils.listObjects(s3AsyncClient, bucket, rootPath)
            .flattenAsObservable(r -> r)
            .concatMap(timeId ->
                    S3Utils.listObjects(s3AsyncClient, bucket, rootPath + "/" + timeId)
                            .flattenAsObservable(r -> r)
                            .map(r -> ParsedPath.parse(timeId, r)))
            .toList();
  }
}
