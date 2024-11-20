package com.syncdb.stream.reader;

import com.syncdb.core.models.PartitionedBlockNameBuilder;
import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Deserializer;
import com.syncdb.stream.adapter.FlowableSizePrefixStreamReader;
import com.syncdb.stream.util.S3Utils;
import com.syncdb.stream.writer.S3StreamWriter;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.syncdb.core.models.PartitionedBlockNameBuilder.FileName.getPath;

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
    this(
        bucket,
        region,
        namespace,
        keyDeserializer,
        valueDeserializer,
        DEFAULT_BUFFER_SIZE,
        Schedulers.io());
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

  public void readSst(
      Subscriber<Path> subscriber, long delay, long period, Function<String, Path> pathFunction) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                sstStream(pathFunction)
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            delay,
            period,
            TimeUnit.MILLISECONDS);
  }

  public void readSst(
      Subscriber<Path> subscriber,
      Integer partitionId,
      long delay,
      long period,
      Function<String, Path> pathFunction) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                sstStream(partitionId, pathFunction)
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            delay,
            period,
            TimeUnit.MILLISECONDS);
  }

  public void readRecord(Subscriber<Record<K, V>> subscriber, long delay, long period) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                recordStream()
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            delay,
            period,
            TimeUnit.MILLISECONDS);
  }

  public void readRecord(
      Subscriber<Record<K, V>> subscriber, Integer partitionId, long delay, long period) {
    this.readerTask =
        worker.schedulePeriodically(
            () -> {
              if (running.compareAndSet(false, true)) {
                recordStream(partitionId)
                    .doOnComplete(() -> lastTimestamp.set(tempTimestamp))
                    .doFinally(() -> running.set(false))
                    .subscribe(subscriber);
              }
            },
            delay,
            period,
            TimeUnit.MILLISECONDS);
  }

  // for sst files
  public Flowable<Path> sstStream(Function<String, Path> pathFunction) {
    return getBlocksToRead(READER_TYPE.SST.extension)
        .groupBy(PartitionedBlockNameBuilder.FileName::getPartitionId)
        .flatMap(
            group ->
                group
                    .map(r -> getPath(r, namespace, READER_TYPE.SST.extension))
                    .concatMap(r -> readSST(r, pathFunction)));
  }

  public Flowable<Path> sstStream(Integer partitionId, Function<String, Path> pathFunction) {
    return getBlocksToRead(partitionId, READER_TYPE.SST.extension)
        .map(r -> getPath(r, namespace, READER_TYPE.SST.extension))
        .concatMap(r -> readSST(r, pathFunction));
  }

  // for record streams
  public Flowable<Record<K, V>> recordStream() {
    return getBlocksToRead(READER_TYPE.RECORD_STREAM.extension)
        .groupBy(PartitionedBlockNameBuilder.FileName::getPartitionId)
        .flatMap(
            group ->
                group
                    .map(r -> getPath(r, namespace, READER_TYPE.RECORD_STREAM.extension))
                    .concatMap(this::readBlock));
  }

  public Flowable<Record<K, V>> recordStream(Integer partitionId) {
    return getBlocksToRead(partitionId, READER_TYPE.RECORD_STREAM.extension)
        .map(r -> getPath(r, namespace, READER_TYPE.RECORD_STREAM.extension))
        .concatMap(this::readBlock);
  }

  private Flowable<PartitionedBlockNameBuilder.FileName> getBlocksToRead(
      Integer partitionId, String fileExtension) {
    return getBlocksToRead(fileExtension)
        .filter(r -> Objects.equals(r.getPartitionId(), partitionId));
  }

  private Flowable<PartitionedBlockNameBuilder.FileName> getBlocksToRead(String fileExtension) {
    return S3Utils.listObject(
            s3Client, bucket, namespace + "/" + S3StreamWriter.WriteState._SUCCESS.name())
        .filter(r -> r.lastModified().toEpochMilli() > lastTimestamp.get())
        .map(
            r -> {
              tempTimestamp = r.lastModified().toEpochMilli();
              return r.lastModified().toEpochMilli();
            })
        .toFlowable()
        .flatMap(l -> getLatestBlocks(l, fileExtension));
  }

  private Flowable<PartitionedBlockNameBuilder.FileName> getLatestBlocks(
      Long latestTimestamp, String fileExtension) {
    return S3Utils.listAsS3Objects(s3Client, bucket, namespace)
        .filter(
            r ->
                r.lastModified().toEpochMilli() >= lastTimestamp.get()
                    && r.lastModified().toEpochMilli() <= latestTimestamp)
        .sorted(Comparator.comparing(S3Object::lastModified))
        .map(S3Object::key)
        .map(r -> r.split("/")[r.split("/").length - 1])
        .filter(
            r ->
                PartitionedBlockNameBuilder.getFilePattern().matcher(r).matches()
                    && r.endsWith(fileExtension))
        .map(PartitionedBlockNameBuilder.FileName::create);
  }

  private Flowable<Path> readSST(String blockId, Function<String, Path> pathFunction) {
    Path file = pathFunction.apply(blockId.split("/")[blockId.split("/").length - 1]);
    return S3Utils.getS3ObjectAsFile(s3Client, bucket, blockId, file).andThen(Flowable.just(file));
  }

  private Flowable<Record<K, V>> readBlock(String blockId) {
    return S3Utils.getS3ObjectFlowableStream(s3Client, bucket, blockId)
        .compose(FlowableSizePrefixStreamReader.read(bufferSize))
        .concatMap(Flowable::fromIterable)
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

  public enum READER_TYPE {
    SST(".sst"),
    RECORD_STREAM(".sdb");

    private final String extension;

    READER_TYPE(String extension) {
      this.extension = extension;
    }

    @Override
    public String toString() {
      return extension;
    }
  }
}
