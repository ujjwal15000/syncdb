package com.syncdb.stream.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.stream.metadata.impl.S3StreamMetadata;
import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.stream.parser.FlowableDelimitedStreamReader;
import com.syncdb.stream.util.ObjectMapperUtils;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.util.S3BlockUtils;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import static com.syncdb.stream.constant.Constants.*;

@Slf4j
public class S3StreamReader<K, V> implements StreamReader<K, V, S3StreamMetadata>{

  /*
      reader is responsible for block metadata management

      1. open the blockId in metadata to read records
      2. apply serde to it
  */
  private final Integer DEFAULT_BUFFER_SIZE = 1024 * 1024;

  private final byte[] delimiter = STREAM_DELIMITER.getBytes();
  private final String bucket;
  private final String rootPath;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final S3AsyncClient s3Client;
  private final ObjectMapper objectMapper;
  private final Integer bufferSize;;

  public S3StreamReader(
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
    this.objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
    this.bufferSize = DEFAULT_BUFFER_SIZE;
  }

  public S3StreamReader(
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
    this.objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
    this.bufferSize = bufferSize;
  }

  public Single<S3StreamMetadata> getStreamMetadata() {
    return S3BlockUtils.getMetadata(s3Client, bucket, rootPath)
        .onErrorResumeNext(
            e -> {
              log.error("error getting wal metadata: ", e);
              return Single.error(e);
            })
        .map(r -> objectMapper.readValue(r, S3StreamMetadata.class));
  }

  // offset == blockId in case of s3
  public Flowable<Record<K, V>> readStream(Long offset) {
    return readBlock(offset);
  }

  public Flowable<Record<K, V>> readBlock(Long blockId) {
    return S3Utils.getS3ObjectFlowableStream(s3Client, bucket, S3BlockUtils.getBlockName(rootPath, blockId))
            .compose(FlowableDelimitedStreamReader.read(delimiter, bufferSize))
            .map(r -> Record.deserialize(r, keyDeserializer, valueDeserializer, objectMapper));
  }

  public void close() {
    this.s3Client.close();
  }
}
