package com.syncdb.stream.reader;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.stream.parser.FlowableMsgPackByteKVStreamReader;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.models.SparkBlock;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.List;

@Slf4j
public class S3MessagePackKVStreamReader<K, V> {

  /*
      reader is responsible for block metadata management

      1. open the blockId to get partition files
      2. open the corresponding partition file
      3. apply serde to it
  */
  public static final Integer DEFAULT_BUFFER_SIZE = 1024 * 1024;

  private final String bucket;
  private final String rootPath;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final S3AsyncClient s3Client;
  private final Integer bufferSize;

  public S3MessagePackKVStreamReader(
      String bucket,
      String region,
      String rootPath,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.s3Client = S3Utils.getClient(region);
    this.bufferSize = DEFAULT_BUFFER_SIZE;
  }

  public S3MessagePackKVStreamReader(
          String bucket,
          String region,
          String rootPath,
          Deserializer<K> keyDeserializer,
          Deserializer<V> valueDeserializer,
          Integer bufferSize) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.s3Client = S3Utils.getClient(region);
    this.bufferSize = bufferSize;
  }

  public Flowable<Record<K, V>> readBlock(String blockPath) {
    return S3Utils.getS3ObjectFlowableStream(s3Client, bucket, rootPath + "/" + blockPath)
        .compose(FlowableMsgPackByteKVStreamReader.read(bufferSize))
        .map(r -> Record.<K,V>builder()
                .key(keyDeserializer.deserializer(r.getKey()))
                .value(valueDeserializer.deserializer(r.getValue()))
                .build());
  }

  public Single<List<SparkBlock.ParsedPath>> getBlocks(){
    return SparkBlock.getBlocks(s3Client, bucket, rootPath);
  }

  public void close() {
    this.s3Client.close();
  }
}
