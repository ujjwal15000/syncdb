package com.syncdb.wal.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.wal.models.Record;
import com.syncdb.wal.serde.Deserializer;
import com.syncdb.wal.util.FlowableBlockStreamReader;
import com.syncdb.wal.util.ObjectMapperUtils;
import com.syncdb.wal.util.S3Utils;
import com.syncdb.wal.util.WalBlockUtils;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.nio.ByteBuffer;

import static com.syncdb.wal.constant.Constants.*;

@Slf4j
public class S3Reader<K, V> {

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

  public S3Reader(
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

  public Flowable<Record<K, V>> readBlockFull(Integer blockId) {
    return S3Utils.getS3Object(s3Client, bucket, WalBlockUtils.getBlockName(rootPath, blockId))
        .map(
            r -> {
              log.info(new String(r));
              return Record.<K, V>builder().build();
            })
        .toFlowable();
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
