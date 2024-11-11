package com.syncdb.spark.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.DataWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class SyncDbDataWriterFactory implements DataWriterFactory {
    private final String outputPath;
    private final Map<String, String> properties;

    public SyncDbDataWriterFactory(String outputPath, Map<String, String> properties) {
        this.outputPath = outputPath;
        this.properties = properties;
    }

    private static Configuration createHadoopConfig(Map<String, String> properties) {
        Configuration conf = new Configuration();
        properties.forEach(conf::set);
        return conf;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        try {
            FileSystem fs = FileSystem.get(createHadoopConfig(properties));
            Path path = new Path(outputPath + "/part-" + partitionId + ".bin");
            OutputStream outputStream = fs.create(path);
            return new SyncDbDataWriter(outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error creating FlatBufferDataWriter", e);
        }
    }
}
