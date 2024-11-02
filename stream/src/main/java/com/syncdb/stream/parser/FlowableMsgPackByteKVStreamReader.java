package com.syncdb.stream.parser;

import com.syncdb.core.models.Record;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static com.syncdb.stream.parser.ByteKVStreamParser.unpackByteByteRecord;

@Slf4j
public class FlowableMsgPackByteKVStreamReader extends Flowable<Record<byte[], byte[]>>
    implements FlowableTransformer<ByteBuffer, Record<byte[], byte[]>> {

  private final Publisher<ByteBuffer> source;
  private final Integer bufferSize;

  private FlowableMsgPackByteKVStreamReader(Publisher<ByteBuffer> source, Integer bufferSize) {
    this.source = source;
    this.bufferSize = bufferSize;
  }

  public static FlowableMsgPackByteKVStreamReader read(Integer bufferSize) {
    return new FlowableMsgPackByteKVStreamReader(null, bufferSize);
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super Record<byte[], byte[]>> subscriber) {
    ByteBuffer buffer;
    try {
      buffer = ByteBuffer.allocate(bufferSize);
    } catch (Exception e) {
      Exceptions.throwIfFatal(e);
      EmptySubscription.error(e, subscriber);
      return;
    }

    this.source.subscribe(new BufferSubscriber(subscriber, buffer));
  }

  public Publisher<Record<byte[], byte[]>> apply(Flowable<ByteBuffer> upstream) {
    return new FlowableMsgPackByteKVStreamReader(upstream, this.bufferSize);
  }

  public static class BufferSubscriber implements Subscription, Subscriber<ByteBuffer> {
    private final Subscriber<? super Record<byte[], byte[]>> downstream;
    private final ByteBuffer buffer;
    Subscription upstream;

    public BufferSubscriber(
        Subscriber<? super Record<byte[], byte[]>> downstream, ByteBuffer buffer) {
      this.downstream = downstream;
      this.buffer = buffer;
    }

    @Override
    public void onSubscribe(@NonNull Subscription upstream) {
      if (SubscriptionHelper.validate(this.upstream, upstream)) {
        this.upstream = upstream;
        this.downstream.onSubscribe(this);
      }
    }

    @Override
    @SneakyThrows
    public void onNext(ByteBuffer data) {
      try {
        while (data.hasRemaining()) {
          while (data.hasRemaining() && buffer.position() < buffer.limit()) {
            buffer.put(data.get());
          }
          buffer.limit(buffer.position());
          ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer.array(), 0, buffer.position());
          MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(inputStream);
          while (unpacker.hasNext()) {
            try {
              downstream.onNext(unpackByteByteRecord(unpacker));
            } catch (Exception e) {
              byte[] remaining = inputStream.readAllBytes();
              buffer.clear();
              buffer.put(remaining);
              break;
            }
          }
        }
      } catch (Exception e) {
        this.onError(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      buffer.clear();
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      if (buffer.position() < buffer.limit()) {
        this.downstream.onError(new RuntimeException("unexpected end of stream while parsing"));
      }
      this.buffer.clear();
      this.downstream.onComplete();
    }

    @Override
    public void request(long n) {
      this.upstream.request(n);
    }

    @Override
    public void cancel() {
      this.upstream.cancel();
    }
  }
}
