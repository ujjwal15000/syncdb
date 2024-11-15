package com.syncdb.spark.writer.client;

import com.syncdb.core.models.Record;
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

import java.util.concurrent.atomic.AtomicInteger;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SyncDbSocketWriter implements DataWriter<InternalRow> {
  private final AtomicInteger batch;

  private final String host;
  private final int port;
  private final EventLoopGroup group;
  private Channel channel;

  public SyncDbSocketWriter(String host, int port) {
    batch = new AtomicInteger(0);

    this.group = new NioEventLoopGroup();
    this.host = host;
    this.port = port;
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
                ch.pipeline().addLast(new ServerHandler(batch));
              }
            });
    try {
      ChannelFuture future = bootstrap.connect(host, port).sync();
      this.channel = future.channel();
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to connect to server", e);
    }
  }

  public void writeToChannel(byte[] data) {
    if (channel != null && channel.isOpen()) {
      channel.writeAndFlush(Unpooled.copiedBuffer(data));
    }
  }

  @Override
  public void write(InternalRow row) {
    synchronized (batch) {
      while (batch.get() <= 0) {
        try {
          batch.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while waiting", e);
        }
      }
    }
    byte[] key = row.getBinary(0);
    byte[] value = row.getBinary(1);
    byte[] data = Record.serialize(key, value);

    writeToChannel(convertToByteArray(data.length));
    writeToChannel(data);
    batch.getAndDecrement();
  }

  @Override
  public WriterCommitMessage commit()  {
    return null;
  }

  @Override
  public void abort() {}

  @Override
  public void close()  {
    this.channel.close();
  }
}
