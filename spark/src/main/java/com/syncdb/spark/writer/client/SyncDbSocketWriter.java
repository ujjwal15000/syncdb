package com.syncdb.spark.writer.client;

import com.syncdb.client.writer.ProtocolWriter;
import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
import com.syncdb.core.protocol.message.MetadataMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SyncDbSocketWriter implements DataWriter<InternalRow> {
  private final AtomicLong buffer;

  private final String host;
  private final int port;
  private final EventLoopGroup group;
  private final ClientMetadata clientMetadata;
  private final AtomicReference<Channel> channel = new AtomicReference<>();
  private final AtomicInteger seq = new AtomicInteger(1);
  private final List<Record<byte[], byte[]>> records = new ArrayList<>();

  public SyncDbSocketWriter(String host, int port, ClientMetadata clientMetadata) {
    buffer = new AtomicLong(0);

    this.group = new NioEventLoopGroup();
    this.host = host;
    this.port = port;
    this.clientMetadata = clientMetadata;
    this.connect();
  }

  private void connect() {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(group)
        .channel(NioSocketChannel.class)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new ServerHandler(buffer));
              }
            });
    try {
      ChannelFuture future = bootstrap.connect(host, port).sync();
      this.channel.set(future.channel());
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to connect to server", e);
    }

    ProtocolMessage metadataMessage = ProtocolWriter.createMetadataMessage(clientMetadata);
    this.writeToChannel(ProtocolMessage.serialize(metadataMessage));
  }

  public void writeToChannel(byte[] data) {
      if (channel.get() != null && channel.get().isOpen()) {
        channel.get().writeAndFlush(Unpooled.copiedBuffer(convertToByteArray(data.length)));
        channel.get().writeAndFlush(Unpooled.copiedBuffer(data));
    }
  }

  @Override
  public void write(InternalRow row) {
    synchronized (buffer) {
      if (buffer.get() < 0) {
        try {
          buffer.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while waiting", e);
        }
      }
    }
    byte[] key = row.getBinary(0);
    byte[] value = row.getBinary(1);
    byte[] serializedRecord = Record.serialize(key, value);

    synchronized (buffer){
        if (buffer.get() - serializedRecord.length < 0) {
            ProtocolMessage message =
                    ProtocolWriter.createStreamingWriteMessage(
                            seq.getAndIncrement(),
                            records);
            writeToChannel(ProtocolMessage.serialize(message));
            try {
                buffer.wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted while waiting", e);
            }
            records.clear();
        }
        buffer.addAndGet(-serializedRecord.length);
        records.add(Record.<byte[], byte[]>builder().key(key).value(value).build());
    }
  }

  @Override
  public WriterCommitMessage commit() {
    return null;
  }

  @Override
  public void abort() {}

  @Override
  public void close() {
    synchronized (buffer) {
      if (!records.isEmpty()) {
        ProtocolMessage message =
            ProtocolWriter.createStreamingWriteMessage(seq.getAndIncrement(), records);
        writeToChannel(ProtocolMessage.serialize(message));

        message =
                ProtocolWriter.createEndStreamMessage();
        writeToChannel(ProtocolMessage.serialize(message));

//        try {
//          buffer.wait();
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//          throw new RuntimeException("Thread interrupted while waiting", e);
//        }
        records.clear();
      }
    }
    this.channel.get().close();
  }
}
