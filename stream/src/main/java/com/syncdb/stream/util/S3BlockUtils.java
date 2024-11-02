package com.syncdb.stream.util;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import static com.syncdb.stream.constant.Constants.*;

@Slf4j
public class S3BlockUtils {
  public static String getBlockName(String rootPath, Long i) {
    return rootPath + STREAM_PATH + STREAM_BLOCK_PREFIX + i.toString();
  }

  public static Single<byte[]> getMetadata(S3AsyncClient s3Client, String bucket, String rootPath) {
    return S3Utils.getS3Object(s3Client, bucket, rootPath + WRITER_METADATA_FILE_NAME);
  }

  public static Completable putMetadata(
      S3AsyncClient s3Client, byte[] s3StreamMetadata, String bucket, String rootPath) {
    return S3Utils.putS3Object(
        s3Client, bucket, rootPath + WRITER_METADATA_FILE_NAME, s3StreamMetadata);
  }
}
