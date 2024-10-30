package com.syncdb.wal.writer;

import com.syncdb.wal.models.Record;
import com.syncdb.wal.models.WriterMetadata;
import com.syncdb.wal.serde.Serializer;
import com.syncdb.wal.util.FlowableBlockStreamWriter;
import com.syncdb.wal.util.ObjectMapperUtils;
import com.syncdb.wal.util.S3Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncdb.wal.util.WalBlockUtils;
import io.reactivex.rxjava3.core.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.syncdb.wal.constant.Constants.*;

@Slf4j
public class S3Writer<K, V> {
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

  private final String bucket;
  private final String rootPath;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final S3AsyncClient s3Client;
  private final ObjectMapper objectMapper;
  private final AtomicInteger blockId;
  private final Integer blockSize;
  private final byte[] delimiter = STREAM_DELIMITER.getBytes();

  public S3Writer(
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
    this.objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
    WriterMetadata initWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicInteger(initWriterMetadata.getBlockId());
    this.blockSize = DEFAULT_BLOCK_SIZE;
  }

  public S3Writer(
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
    this.objectMapper = ObjectMapperUtils.getMsgPackObjectMapper();
    WriterMetadata initWriterMetadata = getOrInitMetadata().blockingGet();
    this.blockId = new AtomicInteger(initWriterMetadata.getBlockId());
    this.blockSize = blockSize;
  }

  private Single<WriterMetadata> getOrInitMetadata() {
    WriterMetadata initWriterMetadata = WriterMetadata.builder().blockId(0).build();
    return S3Utils.getS3Object(s3Client, bucket, rootPath + WRITER_METADATA_FILE_NAME)
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
                    ? Single.just(objectMapper.readValue(r, WriterMetadata.class))
                    : putMetadata(initWriterMetadata).andThen(Single.just(initWriterMetadata)));
  }

  @SneakyThrows
  private Completable putMetadata(WriterMetadata writerMetadata) {
    return S3Utils.putS3Object(
        s3Client,
        bucket,
        rootPath + WRITER_METADATA_FILE_NAME,
        objectMapper.writeValueAsBytes(writerMetadata));
  }

  public Completable writeStream(Flowable<Record<K, V>> stream) {
    return stream
        .map(r -> Record.serialize(r, keySerializer, valueSerializer, objectMapper))
        .compose(FlowableBlockStreamWriter.write(blockSize, delimiter))
        .concatMapCompletable(this::putBlockToS3);
  }

  private Completable putBlockToS3(ByteBuffer block) {
    return S3Utils.putS3Object(
            s3Client,
            bucket,
            WalBlockUtils.getBlockName(rootPath, blockId.incrementAndGet()), copyBuffer(block))
        .andThen(putMetadata(WriterMetadata.builder().blockId(blockId.get()).build()));
  }

  private static byte[] copyBuffer(ByteBuffer block){
    byte[] array = new byte[block.limit()];
    block.get(array, block.position(), block.limit());
    return array;
  }

  public Integer getBlockId(){
    return blockId.get();
  }

  public void close(){
    this.s3Client.close();
  }
}
