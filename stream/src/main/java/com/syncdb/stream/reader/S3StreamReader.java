package com.syncdb.stream.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.stream.metadata.impl.S3StreamMetadata;
import com.syncdb.stream.models.Record;
import com.syncdb.stream.serde.Deserializer;
import com.syncdb.stream.util.FlowableBlockStreamReader;
import com.syncdb.stream.util.ObjectMapperUtils;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.util.WalBlockUtils;
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

  private final String bucket;
  private final String rootPath;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final S3AsyncClient s3Client;
  private final ObjectMapper objectMapper;
  private final byte[] delimiter = STREAM_DELIMITER.getBytes();

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
  }

  public Single<S3StreamMetadata> getStreamMetadata() {
    return WalBlockUtils.getMetadata(s3Client, bucket, rootPath)
        .onErrorResumeNext(
            e -> {
              log.error("error getting wal metadata: ", e);
              return Single.error(e);
            })
        .map(r -> objectMapper.readValue(r, S3StreamMetadata.class));
  }

  public Flowable<Record<K, V>> readBlock(Integer blockId) {
    return S3Utils.getS3ObjectStream(s3Client, bucket, WalBlockUtils.getBlockName(rootPath, blockId))
            .compose(FlowableBlockStreamReader.read(delimiter))
            .map(r -> Record.deserialize(r, keyDeserializer, valueDeserializer, objectMapper));
  }

  public void close() {
    this.s3Client.close();
  }
}
