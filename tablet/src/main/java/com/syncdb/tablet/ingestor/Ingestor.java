package com.syncdb.tablet.ingestor;

import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.nio.file.Files;
import java.nio.file.Path;

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
  @Getter private final RocksDB rocksDB;
  private final Scheduler scheduler = Schedulers.io();

  // todo figure this out
  // todo might nee to remove this
  @Getter
  private final RateLimiter rateLimiter =
          new RateLimiter(100 * 1024 * 1024,
                  100_000, 10, RateLimiterMode.WRITES_ONLY, true);

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig, Options options, String path) {
    this.partitionConfig = partitionConfig;
    this.options = options;
    this.options.setRateLimiter(rateLimiter);

    this.path = path;
    this.rocksDB = RocksDB.open(options, path);
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

  public void close() {
    this.rocksDB.close();
  }
}
