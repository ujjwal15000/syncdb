package com.syncdb.stream.producer;

import com.syncdb.core.models.Record;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.syncdb.core.models.Record.EMPTY_RECORD;

public class StreamProducer<K, V> implements Producer<K, V> {
  public static final int DEFAULT_BUFFER_SIZE = 128;

  private final int bufferSize;
  private final BlockingDeque<Record<K, V>> recordDeque;
  private final Scheduler.Worker worker;

  @Getter private final Flowable<Record<K, V>> recordStream;
  private Boolean streamKilled = false;

  public StreamProducer() {
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    this.recordDeque = new LinkedBlockingDeque<>(bufferSize);
    this.recordStream = generateStream();
    this.worker = Schedulers.computation().createWorker();
  }

  public StreamProducer(int bufferSize) {
    this.bufferSize = bufferSize;
    this.recordDeque = new LinkedBlockingDeque<>(this.bufferSize);
    this.recordStream = generateStream();
    this.worker = Schedulers.computation().createWorker();
  }

  @SuppressWarnings("unchecked")
  private Flowable<Record<K, V>> generateStream() {
    return Flowable.generate(
        emitter -> {
          if (streamKilled) emitter.onComplete();

          if (recordDeque.peek() != null) emitter.onNext(recordDeque.poll());
          // no-op
          else emitter.onNext((Record<K, V>) EMPTY_RECORD);
        });
  }

  @Override
  public void initTransactions() {}

  @Override
  public void beginTransaction() throws ProducerFencedException {}

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
      throws ProducerFencedException {}

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)
      throws ProducerFencedException {}

  @Override
  public void commitTransaction() throws ProducerFencedException {}

  @Override
  public void abortTransaction() throws ProducerFencedException {}

  @Override
  @SneakyThrows
  // todo clean up this and add emitters to deque as completion handlers
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return Single.fromCallable(() -> {
      this.recordDeque.putLast(Record.<K, V>builder().key(record.key()).value(record.value()).build());
      return new RecordMetadata(new TopicPartition("", 0), 0L, 0, 0L, 4, 4);
    }).toFuture();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    return null;
  }

  @Override
  public void flush() {}

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return List.of();
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return Map.of();
  }

  @Override
  public Uuid clientInstanceId(Duration timeout) {
    return null;
  }

  @Override
  public void close() {
    this.streamKilled = true;
  }

  @Override
  public void close(Duration timeout) {
    this.worker.schedule(() -> this.streamKilled = true, timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
