package com.syncdb.spark.writer.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ServerHandler extends ChannelInboundHandlerAdapter {
    private final AtomicInteger batch;

    public ServerHandler(AtomicInteger batch) {
        this.batch = batch;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf message = (ByteBuf)msg;
        byte[] data = new byte[message.writerIndex()];
        message.getBytes(0, data);
        batch.addAndGet(Integer.parseInt(new String(data)));
        synchronized (batch) {
            batch.notify();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        synchronized (batch) {
            batch.notifyAll();
        }
        ctx.close();
    }
}
