package com.syncdb.stream.reader;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.stream.parser.FlowableSizePrefixStreamReader;
import com.syncdb.stream.util.S3Utils;
import io.reactivex.rxjava3.core.Flowable;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@Slf4j
public class S3StreamReader<K, V> {

  /*
      reader is responsible for block metadata management

      1. open the block to read records
      2. apply serde to it
  */
  private final Integer DEFAULT_BUFFER_SIZE = 1024 * 1024;

  private final String bucket;
  private final String namespace;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final S3AsyncClient s3Client;
  private final Integer bufferSize;
  ;

  public S3StreamReader(
      String bucket,
      String region,
      String namespace,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this.bucket = bucket;
    this.namespace = namespace;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.s3Client = S3Utils.getClient(region);
    this.bufferSize = DEFAULT_BUFFER_SIZE;
  }

  public S3StreamReader(
      String bucket,
      String region,
      String namespace,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      Integer bufferSize) {
    this.bucket = bucket;
    this.namespace = namespace;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.s3Client = S3Utils.getClient(region);
    this.bufferSize = bufferSize;
  }

  public Flowable<Record<K, V>> readBlock(String blockId) {
    return S3Utils.getS3ObjectFlowableStream(s3Client, bucket, blockId)
        .compose(FlowableSizePrefixStreamReader.read(bufferSize))
        .map(r -> Record.deserialize(r, keyDeserializer, valueDeserializer));
  }

  public void close() {
    this.s3Client.close();
  }
}
