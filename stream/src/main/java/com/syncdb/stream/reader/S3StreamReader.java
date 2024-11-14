package com.syncdb.stream.reader;

import com.syncdb.core.models.PartitionedBlock;
import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.stream.adapter.FlowableSizePrefixStreamReader;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.writer.S3StreamWriter;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.rxjava3.FlowableHelper;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.syncdb.core.models.PartitionedBlock.FileName.getPath;

@Slf4j
public class S3StreamReader<K, V> {

  /*
      reader is responsible for block metadata management

      1. open the block to read records
      2. apply serde to it
  */
  private static final Integer DEFAULT_BUFFER_SIZE = 1024 * 1024;


  private final String bucket;
  private final String namespace;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final S3AsyncClient s3Client;
  private final Integer bufferSize;
  private final Scheduler scheduler;
  private final Scheduler.Worker worker;
  private final AtomicLong lastTimestamp = new AtomicLong(0L);
  private final AtomicBoolean running = new AtomicBoolean(false);

  private Disposable readerTask;
  private long tempTimestamp = 0L;

  public S3StreamReader(
      String bucket,
      String region,
      String namespace,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(bucket, region, namespace, keyDeserializer, valueDeserializer, DEFAULT_BUFFER_SIZE, Schedulers.io());
  }

  public S3StreamReader(
      String bucket,
      String region,
      String namespace,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      Integer bufferSize,
      Scheduler scheduler) {
    this.bucket = bucket;
    this.namespace = namespace;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.s3Client = S3Utils.getClient(region);
    this.bufferSize = bufferSize;
    this.scheduler = scheduler;
    this.worker = scheduler.createWorker();
  }

  private void subscribe(
      WriteStream<Record<K, V>> writeStream, Handler<AsyncResult<Void>> handler) {
    getReadStream().pipeTo(writeStream, handler);
  }

  private void subscribe(
      WriteStream<Record<K, V>> writeStream,
      Handler<AsyncResult<Void>> handler,
      Integer partitionId) {
    getReadStream(partitionId).pipeTo(writeStream, handler);
  }

  public ReadStream<Record<K, V>> getReadStream() {
    return FlowableHelper.toReadStream(readAllBlocks());
  }

  public ReadStream<Record<K, V>> getReadStream(Integer partitionId) {
    return FlowableHelper.toReadStream(readAllBlocks(partitionId));
  }

  public void read(Subscriber<Record<K, V>> subscriber, long period, long delay) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                readAllBlocks()
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            period,
            delay,
            TimeUnit.MILLISECONDS);
  }

  public void read(
      Subscriber<Record<K, V>> subscriber, Integer partitionId, long period, long delay) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                readAllBlocks()
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            period,
            delay,
            TimeUnit.MILLISECONDS);
  }

  public Flowable<Record<K, V>> readAllBlocks() {
    return getBlocksToRead()
        .groupBy(PartitionedBlock.FileName::getPartitionId)
        .flatMap(group -> group.map(r -> getPath(r, namespace)).concatMap(this::readBlock));
  }

  public Flowable<Record<K, V>> readAllBlocks(Integer partitionId) {
    return getBlocksToRead(partitionId).map(r -> getPath(r, namespace)).concatMap(this::readBlock);
  }

  private Flowable<PartitionedBlock.FileName> getBlocksToRead(Integer partitionId) {
    return getBlocksToRead().filter(r -> Objects.equals(r.getPartitionId(), partitionId));
  }

  private Flowable<PartitionedBlock.FileName> getBlocksToRead() {
    return S3Utils.listObject(
            s3Client, bucket, namespace + "/" + S3StreamWriter.WriteState._SUCCESS.name())
        .filter(r -> r.lastModified().toEpochMilli() > lastTimestamp.get())
        .map(
            r -> {
              tempTimestamp = r.lastModified().toEpochMilli();
              return r.lastModified().toEpochMilli();
            })
        .toFlowable()
        .flatMap(this::getLatestBlocks);
  }

  private Flowable<PartitionedBlock.FileName> getLatestBlocks(Long latestTimestamp) {
    return S3Utils.listAsS3Objects(s3Client, bucket, namespace)
        .filter(
            r ->
                r.lastModified().toEpochMilli() >= lastTimestamp.get()
                    && r.lastModified().toEpochMilli() <= latestTimestamp)
        .sorted(Comparator.comparing(S3Object::lastModified))
        .map(S3Object::key)
        .map(r -> r.split("/")[r.split("/").length - 1])
        .filter(r -> PartitionedBlock.getFilePattern().matcher(r).matches())
        .map(PartitionedBlock.FileName::create);
  }

  private Flowable<Record<K, V>> readBlock(String blockId) {
    return S3Utils.getS3ObjectFlowableStream(s3Client, bucket, blockId)
        .compose(FlowableSizePrefixStreamReader.read(bufferSize))
        .map(r -> Record.deserialize(r, keyDeserializer, valueDeserializer))
        .doOnComplete(
            () ->
                S3Utils.putObjectTags(
                    s3Client,
                    bucket,
                    blockId,
                    List.of(Tag.builder().key("SUCCESS").value("true").build())))
        .onErrorResumeNext(
            e -> {
              log.error(String.format("error reading block: %s", blockId), e);
              return S3Utils.putObjectTags(
                      s3Client,
                      bucket,
                      blockId,
                      List.of(Tag.builder().key("SUCCESS").value("false").build()))
                  .andThen(Flowable.empty());
            });
  }

  public Long getLastTimestamp() {
    return lastTimestamp.get();
  }

  public void stop() {
    if (readerTask != null && !readerTask.isDisposed()) readerTask.dispose();
  }

  public void close() {
    scheduler.scheduleDirect(() -> readerTask.dispose(), 5_000, TimeUnit.MILLISECONDS);
    this.s3Client.close();
  }
}
