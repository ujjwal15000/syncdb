package com.syncdb.spark.dbwriter;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.Serializer;
import com.syncdb.stream.writer.S3StreamWriter;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;

public class S3PartitionWriter<K, V> implements ForeachPartitionFunction<Row> {
  private final S3StreamWriter<K, V> s3StreamWriter;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public S3PartitionWriter(
      String bucket,
      String region,
      String rootPath,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.s3StreamWriter =
        new S3StreamWriter<>(bucket, region, rootPath, keySerializer, valueSerializer);
  }

  @Override
  public void call(Iterator<Row> rowIterator) {
    Flowable<Record<K, V>> rowStream =
        Flowable.<Row>generate(
                emitter -> {
                  if (rowIterator.hasNext()) emitter.onNext(rowIterator.next());
                  else emitter.onComplete();
                })
            .map(row -> Record.<K, V>builder().key(row.getAs("key")).value(row.getAs("value")).build());
    s3StreamWriter.writeStream(rowStream)
            .blockingAwait();
  }

}
