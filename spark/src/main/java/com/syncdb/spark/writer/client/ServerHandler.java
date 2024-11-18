package com.syncdb.spark.writer.client;

import com.syncdb.client.ProtocolHandlerRegistry;
import com.syncdb.client.ProtocolMessageHandler;
import com.syncdb.client.reader.DefaultHandlers;
import com.syncdb.client.reader.ProtocolReader;
import com.syncdb.core.protocol.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.syncdb.core.util.ByteArrayUtils.convertToByteArray;

@Slf4j
public class ServerHandler extends ChannelInboundHandlerAdapter {
  private final AtomicLong producerBufferSize;
  private final ByteBuffer buffer;
  private final ProtocolHandlerRegistry registry;
  private final ProtocolReader reader;
  private final AtomicReference<Channel> channel;

  boolean sizeReader = true;
  int currentSize = 0;

  public ServerHandler(AtomicLong sendBuffer, AtomicReference<Channel> channel) {
    this.producerBufferSize = sendBuffer;
    this.buffer = ByteBuffer.allocate(1024 * 1024);
    this.channel = channel;

    this.registry = new ProtocolHandlerRegistry();
    registry.registerDefaults();
    registry.registerHandler(ProtocolMessage.MESSAGE_TYPE.REFRESH_BUFFER, new RefreshBufferHandler());
    registry.registerHandler(ProtocolMessage.MESSAGE_TYPE.NOOP, new NoopHandler());

    this.reader = new ProtocolReader(registry);
  }

  // todo: fix errors to exit instead of keep alive
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try{
      byte[] data = new byte[((ByteBuf) msg).writerIndex()];
      ((ByteBuf) msg).getBytes(0 , data);
      ByteBuffer message = ByteBuffer.wrap(data);
      this.onNext(ctx, message);
    }
    catch (Exception e){
      this.exceptionCaught(ctx, e);
    }
  }

  public void onNext(ChannelHandlerContext ctx, ByteBuffer data) {
    try {
      while (data.hasRemaining()) {
        byte currentByte = data.get();
        buffer.put(currentByte);
        if (sizeReader) {
          if (buffer.position() == 4) {
            currentSize = buffer.getInt(0);
            buffer.clear();
            sizeReader = false;
          }
        } else {
          if (buffer.position() == currentSize) {
            buffer.flip();

            this.reader.read(ProtocolMessage.deserialize(buffer.array()));
            buffer.clear();
            currentSize = 0;
            sizeReader = true;
          }
        }
      }
    } catch (Throwable e) {
      this.exceptionCaught(ctx, e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    log.error(cause.getMessage(), cause);
    synchronized (producerBufferSize) {
      producerBufferSize.notifyAll();
    }
    ctx.close();
  }

  public class RefreshBufferHandler extends DefaultHandlers.RefreshBufferHandler {
    @Override
    public void handle(ProtocolMessage message) throws Throwable {
      super.handle(message);
      synchronized (producerBufferSize) {
        producerBufferSize.addAndGet(this.bufferSize);
        producerBufferSize.notify();
      }
    }
  }

  public class NoopHandler extends DefaultHandlers.NoopHandler {
    @Override
    public void handle(ProtocolMessage message) {
      synchronized (channel){
        if (channel.get() != null && channel.get().isOpen()) {
          byte[] serializedMessage = ProtocolMessage.serialize(message);

          channel.get().writeAndFlush(Unpooled.copiedBuffer(convertToByteArray(serializedMessage.length)));
          channel.get().writeAndFlush(Unpooled.copiedBuffer(serializedMessage));
        }
      }
    }
  }
}
