package com.syncdb.stream.writer;

import com.syncdb.stream.metadata.impl.S3StreamMetadata;
import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Serializer;
import com.syncdb.stream.parser.FlowableSizePrefixStreamWriter;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.util.S3BlockUtils;
import io.reactivex.rxjava3.core.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.syncdb.core.models.Record.EMPTY_RECORD;

@Slf4j
public class S3StreamWriter<K extends Serializable, V extends Serializable>
    implements StreamWriter<K, V, S3StreamMetadata>, Serializable {
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
  private final AtomicLong blockId;
  private final Integer blockSize;
  private final Long flushTimeout;

  public S3StreamWriter(
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    S3StreamMetadata initS3StreamWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicLong(initS3StreamWriterMetadata.getLatestBlockId());
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.flushTimeout = DEFAULT_FLUSH_TIMEOUT_MILLIS;
  }

  public S3StreamWriter(
          String bucket,
          String region,
          String rootPath,
          Serializer<K> keySerializer,
          Serializer<V> valueSerializer,
          Long flushTimeout) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    S3StreamMetadata initS3StreamWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicLong(initS3StreamWriterMetadata.getLatestBlockId());
    this.blockSize = DEFAULT_BLOCK_SIZE;
    this.flushTimeout = flushTimeout;
  }

  public S3StreamWriter(
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Integer blockSize) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    S3StreamMetadata initS3StreamWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicLong(initS3StreamWriterMetadata.getLatestBlockId());
    this.blockSize = blockSize;
    this.flushTimeout = DEFAULT_FLUSH_TIMEOUT_MILLIS;
  }

  public S3StreamWriter(
          String bucket,
          String region,
          String rootPath,
          Serializer<K> keySerializer,
          Serializer<V> valueSerializer,
          Integer blockSize,
          Long flushTimeout) {
    this.bucket = bucket;
    this.rootPath = rootPath;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3Client = S3Utils.getClient(region);
    S3StreamMetadata initS3StreamWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicLong(initS3StreamWriterMetadata.getLatestBlockId());
    this.blockSize = blockSize;
    this.flushTimeout = flushTimeout;
  }

  private Single<S3StreamMetadata> getOrInitMetadata() {
    S3StreamMetadata initS3StreamMetadata = S3StreamMetadata.builder().latestBlockId(0L).build();
    return S3BlockUtils.getMetadata(s3Client, bucket, rootPath)
        .onErrorResumeNext(
            e -> {
              if (e instanceof ExecutionException && e.getCause() instanceof NoSuchBucketException)
                return Single.error(
                    new RuntimeException(String.format("bucket does not exist: %s", bucket)));
              if (e instanceof ExecutionException && e.getCause() instanceof NoSuchKeyException)
                return Single.just(new byte[0]);
              return Single.error(e);
            })
        .flatMap(
            r ->
                r.length != 0
                    ? Single.just(S3StreamMetadata.deserialize(r))
                    : putStreamMetadata(initS3StreamMetadata).andThen(Single.just(initS3StreamMetadata)));
  }

  @SneakyThrows
  private Completable putStreamMetadata(S3StreamMetadata s3StreamWriterMetadata) {
    return S3BlockUtils.putMetadata(s3Client, S3StreamMetadata.serialize(s3StreamWriterMetadata), bucket, rootPath);
  }

  public Completable writeStream(Flowable<Record<K, V>> stream) {
    return stream
        .filter(kvRecord -> !Objects.equals(kvRecord, EMPTY_RECORD))
        .map(r -> Record.serialize(r, keySerializer, valueSerializer))
        .compose(FlowableSizePrefixStreamWriter.write(blockSize, flushTimeout))
        .concatMapCompletable(this::putBlockToS3);
  }

  private Completable putBlockToS3(ByteBuffer block) {
    return S3Utils.putS3Object(
            s3Client,
            bucket,
            S3BlockUtils.getBlockName(rootPath, blockId.getAndIncrement()), copyBuffer(block))
        .andThen(putStreamMetadata(S3StreamMetadata.builder().latestBlockId(blockId.get()).build()));
  }

  private static byte[] copyBuffer(ByteBuffer block){
    byte[] array = new byte[block.limit()];
    block.get(array, block.position(), block.limit());
    return array;
  }

  public Long getLatestBlockId(){
    return blockId.get();
  }

  public void close(){
    this.s3Client.close();
  }
}
