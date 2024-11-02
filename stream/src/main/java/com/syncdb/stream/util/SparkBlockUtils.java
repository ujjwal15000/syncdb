package com.syncdb.stream.util;

import static com.syncdb.stream.constant.Constants.*;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class SparkBlockUtils {
  private static final Pattern PATH_PATTERN = Pattern.compile("^part-(\\d+)-([a-f0-9\\-]+)-c(\\d+)\\.mp$");

  public static Single<List<ParsedPath>> getBlocks(
      S3AsyncClient s3AsyncClient, String bucket, String rootPath) {
    return Single.fromFuture(
            s3AsyncClient.listObjects(
                ListObjectsRequest.builder().bucket(bucket).prefix(rootPath).build()))
        .flattenAsFlowable(ListObjectsResponse::contents)
        .map(r -> ParsedPath.parse(r.key()))
        .toList();
  }

  @Data
  @Builder
  public static class ParsedPath {
    private final String partNumber;
    private final String uuid;
    private final String count;

    @Override
    public String toString() {
      return "ParsedPath{" +
              "partNumber='" + partNumber + '\'' +
              ", uuid='" + uuid + '\'' +
              ", count='" + count + '\'' +
              '}';
    }

    public static ParsedPath parse(String path) {
      Matcher matcher = PATH_PATTERN.matcher(path);
      if (matcher.matches()) {
        return ParsedPath.builder()
                .partNumber(matcher.group(1))
                .uuid(matcher.group(2))
                .count(matcher.group(3))
                .build();
      } else {
        throw new IllegalArgumentException("Invalid path format: " + path);
      }
    }
  }
}
