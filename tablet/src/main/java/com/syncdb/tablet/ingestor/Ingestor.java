package com.syncdb.tablet.ingestor;

import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.stream.models.SparkBlock;
import com.syncdb.stream.reader.S3StreamReader;
import com.syncdb.tablet.models.PartitionConfig;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
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
  @Getter private final S3StreamReader<byte[], byte[]> s3MessagePackKVStreamReader;

  // updated in runnable
  private Disposable ingestorTask;
  private boolean ingesting = false;

  @SneakyThrows
  public Ingestor(
      PartitionConfig partitionConfig,
      Options options,
      String path,
      SparkBlock.CommitMetadata commitMetadata,
      Consumer<SparkBlock.CommitMetadata> commitFunction) {
    this.partitionConfig = partitionConfig;
    this.options = options;
    this.path = path;
    this.rocksDB = RocksDB.open(options, path);
    this.s3MessagePackKVStreamReader =
        new S3StreamReader<>(
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
  // todo: implement block cleanup based on number of keys to avoid pagination and to have non infinite retention
  // todo: find an approach to commit messages:
  //  should the reader commit?
  //  should it expose the current offset?
  //  should we even consider about the offset?

  /*
    1. get blocks
    2. filter based on blocks greater than equal to current timeId
    3. get a sort based on order: timeId > countId
    4. check if there are updates mark offset -1 in case current block file offset is read
    5. if not start a sequential stream add data to rocksdb
    6. clean-up old files based on retention config
    7. ping the zookeeper watch to reload readers
  */


  @Override
  public void run() {
    try{
      if(!ingesting){
        ingesting = true;
//        s3MessagePackKVStreamReader.getBlocks()
//                .flattenAsObservable(r -> r)
//                .map()
      }
    }
    catch (Exception e){
      log.error("error while reading blocks", e);
    }
    finally{
      ingesting = false;
    }
  }
}
