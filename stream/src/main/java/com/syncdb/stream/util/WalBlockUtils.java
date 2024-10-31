package com.syncdb.stream.util;

import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import static com.syncdb.stream.constant.Constants.*;

@Slf4j
public class WalBlockUtils {
    public static String getBlockName(String rootPath, Integer i){
        return rootPath + WAL_PATH + WAL_BLOCK_PREFIX + i.toString();
    }

  public static Single<byte[]> getMetadata(S3AsyncClient s3Client, String bucket, String rootPath) {
         return S3Utils.getS3Object(s3Client, bucket, rootPath + WRITER_METADATA_FILE_NAME);
    }
}
