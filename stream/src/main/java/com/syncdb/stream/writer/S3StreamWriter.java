package com.syncdb.stream.writer;

import com.syncdb.core.models.PartitionedBlockBuilder;
import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Serializer;
import com.syncdb.stream.parser.FlowableSizePrefixStreamWriter;
import com.syncdb.stream.util.S3Utils;
import io.reactivex.rxjava3.core.*;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.syncdb.core.models.Record.EMPTY_RECORD;

@Slf4j
public class S3StreamWriter<K, V> {
  /*
      1. check metadata path in s3
          if exists get blockId and offset
          else init it
      2. open the first block to append records
      3. get a flowable source of records<K,V>
      4. apply serde to it
      5. put it in the block
      6. check if block limit exceeds
      7. flush the block to s3
      8. update metadata file
  */
  private static final Long DEFAULT_FLUSH_TIMEOUT_MILLIS = 2_000L;
  public static final Integer DEFAULT_BLOCK_SIZE = 512 * 1024 * 1024;

  private final String bucket;
  private final String rootPath;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final S3AsyncClient s3Client;
  private final Integer blockSize;
  private final Long flushTimeout;
  private final PartitionedBlockBuilder blockBuilder;

  public S3StreamWriter(
      String clientId,
      Integer numPartitions,
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.blockBuilder = PartitionedBlockBuilder.create(rootPath, clientId);
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.flushTimeout = DEFAULT_FLUSH_TIMEOUT_MILLIS;
  }

  public S3StreamWriter(
      String clientId,
      Integer numPartitions,
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Long flushTimeout) {
    this.blockBuilder = PartitionedBlockBuilder.create(rootPath, clientId);
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.flushTimeout = flushTimeout;
  }

  public S3StreamWriter(
      String clientId,
      Integer numPartitions,
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Integer blockSize) {
    this.blockBuilder = PartitionedBlockBuilder.create(rootPath, clientId);
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    this.blockSize = blockSize;
    this.flushTimeout = DEFAULT_FLUSH_TIMEOUT_MILLIS;
  }

  public S3StreamWriter(
      String clientId,
      Integer numPartitions,
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Integer blockSize,
      Long flushTimeout) {
    this.blockBuilder = PartitionedBlockBuilder.create(rootPath, clientId);
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    this.blockSize = blockSize;
    this.flushTimeout = flushTimeout;
  }

  // todo partition this and write
  public Completable writeStream(Flowable<Record<K, V>> stream) {
    return stream
        .filter(kvRecord -> !Objects.equals(kvRecord, EMPTY_RECORD))
        .map(r -> Record.serialize(r, keySerializer, valueSerializer))
        .compose(FlowableSizePrefixStreamWriter.write(blockSize, flushTimeout))
        .concatMapCompletable(this::putBlockToS3);
  }

  public Completable writeStream(Flowable<Record<K, V>> stream, Integer partitionId) {
    AtomicInteger partNumber = new AtomicInteger(0);
    String prefix = blockBuilder.buildNext();
    return stream
        .map(r -> Record.serialize(r, keySerializer, valueSerializer))
        .compose(FlowableSizePrefixStreamWriter.write(blockSize, flushTimeout))
        .concatMapCompletable(r -> putBlockToS3(r, prefix, partitionId, partNumber.getAndIncrement()));
  }

  private Completable putBlockToS3(ByteBuffer block) {
    return S3Utils.putS3Object(s3Client, bucket, blockBuilder.buildNext(), copyBuffer(block));
  }

  private Completable putBlockToS3(ByteBuffer block, String prefix, Integer partitionId, Integer partNumber) {
    return S3Utils.putS3Object(
        s3Client,
        bucket,
        blockBuilder.buildNextForPartition(prefix, partitionId, partNumber),
        block);
  }

  // todo: remove this
  private static byte[] copyBuffer(ByteBuffer block) {
    byte[] array = new byte[block.limit()];
    block.get(array, block.position(), block.limit());
    return array;
  }

  public void close() {
    this.s3Client.close();
  }
}
