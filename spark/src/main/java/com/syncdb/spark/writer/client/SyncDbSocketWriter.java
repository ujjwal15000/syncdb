package com.syncdb.spark.writer.client;

import com.syncdb.client.writer.ProtocolWriter;
import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;
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
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksDB;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

public class SyncDbSocketWriter implements DataWriter<InternalRow> {

  private final String host;
  private final int port;
  private final EventLoopGroup group;
  private final ClientMetadata clientMetadata;
  private final AtomicReference<Channel> channel = new AtomicReference<>();
  private final AtomicInteger seq = new AtomicInteger(1);

  private final RateLimiter rateLimiter;

  public SyncDbSocketWriter(String host, int port, ClientMetadata clientMetadata) {
    RocksDB.loadLibrary();
    this.rateLimiter = new RateLimiter(0, 1000 * 1000);

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
                ch.pipeline().addLast(new ServerHandler(channel, rateLimiter));
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
    byte[] key = row.getBinary(0);
    byte[] value = row.getBinary(1);
    ProtocolMessage message =
            ProtocolWriter.createStreamingWriteMessage(seq.getAndIncrement(), List.of(new Record<>(key, value)));
    byte[] serializedMessage = ProtocolMessage.serialize(message);
    rateLimiter.request(serializedMessage.length);
    writeToChannel(serializedMessage);
  }

  @Override
  public WriterCommitMessage commit() {
    return null;
  }

  @Override
  public void abort() {}

  @Override
  public void close() {
    synchronized (channel) {
        writeToChannel(ProtocolMessage.serialize( ProtocolWriter.createEndStreamMessage()));
        try {
          channel.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while waiting", e);
        }
      this.channel.get().close();
    }
  }
}
