package com.syncdb.stream.writer;

import com.syncdb.core.models.PartitionedBlock;
import com.syncdb.core.models.Record;
import com.syncdb.core.partitioner.Murmur3Partitioner;
import com.syncdb.core.serde.Serializer;
import com.syncdb.stream.parser.FlowableSizePrefixStreamWriter;
import com.syncdb.stream.util.S3Utils;
import io.reactivex.rxjava3.core.*;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.syncdb.core.models.Record.EMPTY_RECORD;

@Slf4j
public class S3StreamWriter<K, V> {
  // todo: readdress this
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
  private final String namespace;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final S3AsyncClient s3Client;
  private final Integer blockSize;
  private final Long flushTimeout;
  private final PartitionedBlock blockBuilder;
  private final Murmur3Partitioner partitioner;

  public S3StreamWriter(
      String clientId,
      Integer numPartitions,
      String bucket,
      String region,
      String namespace,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.blockBuilder = PartitionedBlock.create(clientId);
    this.partitioner = new Murmur3Partitioner(numPartitions);
    this.bucket = bucket;
    this.namespace = namespace;
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
      String namespace,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Long flushTimeout) {
    this.blockBuilder = PartitionedBlock.create(clientId);
    this.partitioner = new Murmur3Partitioner(numPartitions);
    this.bucket = bucket;
    this.namespace = namespace;
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
      String namespace,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Integer blockSize) {
    this.blockBuilder = PartitionedBlock.create(clientId);
    this.partitioner = new Murmur3Partitioner(numPartitions);
    this.bucket = bucket;
    this.namespace = namespace;
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
      String namespace,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Integer blockSize,
      Long flushTimeout) {
    this.blockBuilder = PartitionedBlock.create(clientId);
    this.partitioner = new Murmur3Partitioner(numPartitions);
    this.bucket = bucket;
    this.namespace = namespace;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    this.blockSize = blockSize;
    this.flushTimeout = flushTimeout;
  }

  public Completable writeStream(Flowable<Record<K, V>> stream) {
    return stream
        .filter(kvRecord -> !Objects.equals(kvRecord, EMPTY_RECORD))
        .groupBy(r -> partitioner.getPartition(keySerializer.serialize(r.getKey())))
        .flatMap(group -> writeStream(group, group.getKey()))
        .flatMapCompletable(
            tempPath -> copyTempBlock(tempPath, namespace).andThen(deleteTempBlock(tempPath)))
        .andThen(this.putMetadata(WriteState._SUCCESS, namespace))
        .onErrorResumeNext(
            e -> {
              log.error("error writing batch", e);
              return this.putMetadata(WriteState._ERROR, namespace);
            });
  }

  private Flowable<String> writeStream(Flowable<Record<K, V>> stream, Integer partitionId) {
    AtomicInteger partNumber = new AtomicInteger(0);
    return stream
        .map(r -> Record.serialize(r, keySerializer, valueSerializer))
        .compose(FlowableSizePrefixStreamWriter.write(blockSize, flushTimeout))
        .concatMap(
            r -> {
              String tempPath = namespace + "/" + "_temporary";
              return putBlockToS3(r, tempPath, partitionId, partNumber.getAndIncrement())
                  .toFlowable();
            });
  }

  private Completable putMetadata(WriteState state, String prefix) {
    return S3Utils.putS3Object(s3Client, bucket, prefix + "/" + state.name(), new byte[0]);
  }

  private Completable copyTempBlock(String tempPath, String finalPathPrefix) {
    String filename = tempPath.split("_temporary/")[tempPath.split("_temporary").length-1];
    return S3Utils.copyS3Object(s3Client, bucket, tempPath, finalPathPrefix + "/" + filename);
  }

  private Completable deleteTempBlock(String key) {
    return S3Utils.deleteS3Object(s3Client, bucket, key);
  }

  private Single<String> putBlockToS3(
      ByteBuffer block, String prefix, Integer partitionId, Integer partNumber) {
    String path = blockBuilder.buildNextForPartition(prefix, partitionId, partNumber);
    return S3Utils.putS3Object(s3Client, bucket, path, block).andThen(Single.just(path));
  }

  public void close() {
    this.s3Client.close();
  }

  public enum WriteState {
    _SUCCESS,
    _ERROR
  }
}
