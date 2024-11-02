package com.syncdb.tablet.ingestor;

import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.stream.reader.S3MessagePackKVStreamReader;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function3;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class Ingestor implements Runnable{
  /*
      path for the main read/write instance
      most efficient if writes are key ordered and single threaded
  */
  private static final Long DEFAULT_POLLING_TIME = 5_000L;

  private final PartitionConfig partitionConfig;
  private final Options options;
  private final String path;
  private final RocksDB rocksDB;
  private final Scheduler scheduler = Schedulers.io();
  private final Scheduler.Worker worker;
  private final Disposable ingestor;
  @Getter private final S3MessagePackKVStreamReader<byte[], byte[]> s3MessagePackKVStreamReader;

  // updated in runnable
  private Disposable ingestorTask;
  private Boolean ingesting = Boolean.FALSE;

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig,
      Options options,
      String path,
      String latestBlock,
      Long offset,
      BiConsumer<String, Integer> commitFunction) {
    this.partitionConfig = partitionConfig;
    this.options = options;
    this.path = path;
    this.rocksDB = RocksDB.open(options, path);
    this.s3MessagePackKVStreamReader =
        new S3MessagePackKVStreamReader<>(
            partitionConfig.getS3Bucket(),
            partitionConfig.getAwsRegion(),
            partitionConfig.getNamespace(),
            new ByteDeserializer(),
            new ByteDeserializer());
    this.worker = scheduler.createWorker();
    //todo: configure this
    this.ingestor = worker.schedulePeriodically(this, 0L, DEFAULT_POLLING_TIME, TimeUnit.MILLISECONDS);
  }

  public void write(WriteOptions writeOptions, WriteBatch batch) throws RocksDBException {
    rocksDB.write(writeOptions, batch);
  }

  public void writeWithIndex(WriteOptions writeOptions, WriteBatchWithIndex batch) throws RocksDBException {
    rocksDB.write(writeOptions, batch);
  }

  @SneakyThrows
  public void catchUp() {
    this.rocksDB.tryCatchUpWithPrimary();
  }

  public void close() {
    this.ingestor.dispose();
    if(ingestorTask != null)
      ingestorTask.dispose();
    this.s3MessagePackKVStreamReader.close();
    this.rocksDB.close();
  }

  // todo: implement ingestor and ratelimiter and a register callback function to commit offsets to metadata store
  @Override
  public void run() {
  }
}
