package com.syncdb.tablet.ingestor;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.stream.reader.S3StreamReader;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.rocksdb.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class Ingestor {
  /*
      path for the main read/write instance
      most efficient if writes are key ordered and single threaded
  */
  private static final Long DEFAULT_POLLING_TIME = 5_000L;
  private static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

  private final PartitionConfig partitionConfig;
  private final Options options;
  private final String path;
  private final RocksDB rocksDB;
  private final Scheduler scheduler = Schedulers.io();
  @Getter private final S3StreamReader<byte[], byte[]> s3StreamReader;
  private final IngestionSubscriber ingestionSubscriber;
  private final Integer batchSize;

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig, Options options, String path, Integer batchSize) {
    this.partitionConfig = partitionConfig;
    this.options = options;
    this.path = path;
    this.rocksDB = RocksDB.open(options, path);
    this.s3StreamReader =
        new S3StreamReader<>(
            partitionConfig.getBucket(),
            partitionConfig.getRegion(),
            partitionConfig.getNamespace(),
            new ByteDeserializer(),
            new ByteDeserializer());
    this.ingestionSubscriber = new IngestionSubscriber();
    this.batchSize = batchSize;
    this.start();
  }

  private void start() {
    s3StreamReader.read(
        ingestionSubscriber, partitionConfig.getPartitionId(), DEFAULT_POLLING_TIME, 0L);
  }

  public void write(WriteBatch batch) throws RocksDBException {
    write(DEFAULT_WRITE_OPTIONS, batch);
  }

  public void write(WriteOptions writeOptions, WriteBatch batch) throws RocksDBException {
    rocksDB.write(writeOptions, batch);
  }

  public void writeWithIndex(WriteOptions writeOptions, WriteBatchWithIndex batch)
      throws RocksDBException {
    rocksDB.write(writeOptions, batch);
  }

  public void close() {
    this.s3StreamReader.close();
    this.rocksDB.close();
  }

  // todo: add a flush to this maybe
  private class IngestionSubscriber implements Subscriber<Record<byte[], byte[]>> {
    Subscription upstream;
    WriteBatch batch = new WriteBatch();

    @Override
    public void onSubscribe(Subscription upstream) {
      this.upstream = upstream;
      upstream.request(batchSize);
    }

    @Override
    public void onNext(Record<byte[], byte[]> record) {
      try {
        if (!this.tryOnNext(record)) {
          this.upstream.request(1L);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean tryOnNext(@NonNull Record<byte[], byte[]> record) throws RocksDBException {
      if (record.getKey().length + record.getValue().length + batch.getDataSize() <= batchSize) {
        batch.put(record.getKey(), record.getValue());
        return false;
      }
      write(batch);
      this.batch.clear();
      batch.put(record.getKey(), record.getValue());
      return true;
    }

    @Override
    public void onError(Throwable t) {
      log.error("upstream error: ", t);
    }

    @SneakyThrows
    @Override
    public void onComplete() {
      if(this.batch.getDataSize() > 0){
        write(batch);
        this.batch.clear();
      }
    }
  }
}
