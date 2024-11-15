package com.syncdb.spark.writer.committer;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol;

import java.io.Serializable;

@Slf4j
public class TimestampCommitProtocol extends HadoopMapReduceCommitProtocol implements Serializable {

  public TimestampCommitProtocol(String jobId, String path, boolean dynamicPartitionOverwrite) {
    super(System.currentTimeMillis() + "-" + jobId, path, dynamicPartitionOverwrite);
  }
}
