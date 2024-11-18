package com.syncdb.tablet.ingestor;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.stream.reader.S3StreamReader;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.rxjava3.core.file.AsyncFile;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.rocksdb.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;

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
  private final RecordIngestor recordIngestor;
  private final SSTIngestor sstIngestor;
  private final Integer batchSize;
  private final Integer sstBatchSize;

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig, Options options, String path, Integer batchSize, Integer sstBatchSize) {
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
    this.recordIngestor = new RecordIngestor();
    this.sstIngestor = new SSTIngestor();
    this.batchSize = batchSize;
    this.sstBatchSize = sstBatchSize;
  }

  @SneakyThrows
  public void startStreamReader() {
    s3StreamReader.readRecord(
        recordIngestor, partitionConfig.getPartitionId(), 0L, DEFAULT_POLLING_TIME);
  }

  public void startSstReader() {
    s3StreamReader.readSst(
            sstIngestor, partitionConfig.getPartitionId(), 0L, DEFAULT_POLLING_TIME, this::sstPathFunction);
  }

  @SneakyThrows
  public Path sstPathFunction(String fileName) {
    Path sstPath = Path.of(path + "/sst/" + fileName);
    Files.createDirectories(sstPath);
    return sstPath;
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

  public void ingestSst(List<String> files, IngestExternalFileOptions options) throws RocksDBException {
    rocksDB.ingestExternalFile(files, options);
  }

  public void close() {
    this.s3StreamReader.close();
    this.rocksDB.close();
  }

  private class RecordIngestor implements Subscriber<Record<byte[], byte[]>> {
    Subscription upstream;
    WriteBatch batch = new WriteBatch();

    @Override
    public void onSubscribe(Subscription upstream) {
      this.upstream = upstream;
      upstream.request(1L);
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

  private class SSTIngestor implements Subscriber<Path> {
    Subscription upstream;
    List<String> files = new ArrayList<>();

    @Override
    public void onSubscribe(Subscription upstream) {
      this.upstream = upstream;
      upstream.request(batchSize);
    }

    @Override
    public void onNext(Path filePath) {
      try {
        if (!this.tryOnNext(filePath)) {
          this.upstream.request(1L);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean tryOnNext(Path filePath) throws RocksDBException {
      if(files.size() < sstBatchSize){
        this.files.add(filePath.toString());
        return false;
      }
      ingestSst(files, new IngestExternalFileOptions());
      this.files = new ArrayList<>();
      this.files.add(filePath.toString());
      return true;
    }

    @Override
    public void onError(Throwable t) {
      log.error("upstream error: ", t);
    }

    @SneakyThrows
    @Override
    public void onComplete() {
      if(!this.files.isEmpty()){
        ingestSst(files, new IngestExternalFileOptions());
        this.files.clear();
      }
    }
  }
}
