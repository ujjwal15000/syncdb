package com.syncdb.spark.writer;

import com.syncdb.core.partitioner.Murmur3Partitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

import static com.syncdb.spark.SyncDbStreamDataSource.DEFAULT_SCHEMA;

public class SyncDbPartitioner extends Partitioner {
  private final int numPartitions;
  private final Murmur3Partitioner murmur3Partitioner;

  public SyncDbPartitioner(int numPartitions) {
    this.numPartitions = numPartitions;
    this.murmur3Partitioner = new Murmur3Partitioner(numPartitions);
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public int getPartition(Object key) {
    return this.murmur3Partitioner.getPartition((byte[]) key);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SyncDbPartitioner) {
      SyncDbPartitioner other = (SyncDbPartitioner) obj;
      return this.numPartitions == other.numPartitions;
    }
    return false;
  }

  public static Dataset<Row> repartitionByKey(Dataset<Row> df, int numPartitions) {
    assert df.schema().equals(DEFAULT_SCHEMA);

    SparkSession sparkSession = df.sparkSession();
    SyncDbPartitioner partitioner = new SyncDbPartitioner(numPartitions);

    JavaPairRDD<byte[], byte[]> repartitionedRDD =
        df.javaRDD()
            .mapToPair(r -> new Tuple2<byte[], byte[]>(r.getAs(0), r.getAs(1)))
            .repartitionAndSortWithinPartitions(partitioner, new ByteArrayComparator());

    return sparkSession.createDataFrame(
        repartitionedRDD.map(tuple -> RowFactory.create(tuple._1, tuple._2)), df.schema());
  }

  public static class ByteArrayComparator implements Comparator<byte[]>, Serializable {
    @Override
    public int compare(byte[] a, byte[] b) {
      for (int i = 0; i < Math.min(a.length, b.length); i++) {
        int diff = Byte.compare(a[i], b[i]);
        if (diff != 0) return diff;
      }
      return Integer.compare(a.length, b.length);
    }
  }
}
