package com.syncdb.stream.parser;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;

import java.nio.ByteBuffer;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class FlowableDelimitedStreamReader extends Flowable<byte[]>
    implements FlowableTransformer<ByteBuffer, byte[]> {

  private final Publisher<ByteBuffer> source;
  private final Integer bufferSize;
  private final byte[] delimiter;

  private FlowableDelimitedStreamReader(
      Publisher<ByteBuffer> source, byte[] delimiter, Integer bufferSize) {
    this.source = source;
    this.delimiter = delimiter;
    this.bufferSize = bufferSize;
  }

  public static FlowableDelimitedStreamReader read(byte[] delimiter, Integer bufferSize) {
    return new FlowableDelimitedStreamReader(null, delimiter, bufferSize);
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super byte[]> subscriber) {
    ByteBuffer buffer;
    try {
      buffer = ByteBuffer.allocate(bufferSize);
    } catch (Exception e) {
      Exceptions.throwIfFatal(e);
      EmptySubscription.error(e, subscriber);
      return;
    }

    this.source.subscribe(new BufferSubscriber(subscriber, buffer, delimiter));
  }

  public Publisher<byte[]> apply(Flowable<ByteBuffer> upstream) {
    return new FlowableDelimitedStreamReader(upstream, this.delimiter, this.bufferSize);
  }

  public static class BufferSubscriber implements Subscription, Subscriber<ByteBuffer> {
    private final Subscriber<? super byte[]> downstream;
    private final ByteBuffer buffer;
    Subscription upstream;
    private final byte[] delimiter;

    public BufferSubscriber(
        Subscriber<? super byte[]> downstream, ByteBuffer buffer, byte[] delimiter) {
      this.downstream = downstream;
      this.buffer = buffer;
      this.delimiter = delimiter;
    }

    @Override
    public void onSubscribe(@NonNull Subscription upstream) {
      if (SubscriptionHelper.validate(this.upstream, upstream)) {
        this.upstream = upstream;
        this.downstream.onSubscribe(this);
      }
    }

    @Override
    public void onNext(ByteBuffer data) {
      try {
        int delimiterIndex = 0;
        while (data.hasRemaining()) {
          byte currentByte = data.get();

          if (currentByte == delimiter[delimiterIndex]) {
            delimiterIndex++;
            if (delimiterIndex == delimiter.length) {
              buffer.flip();
              this.downstream.onNext(buffer.array());
              buffer.clear();
              delimiterIndex = 0;
            }
          } else {
            if (delimiterIndex > 0) {
              for (int i = 0; i < delimiterIndex; i++) {
                buffer.put(delimiter[i]);
              }
              delimiterIndex = 0;
            }
            buffer.put(currentByte);
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
      if (buffer.position() > 0) {
        this.downstream.onError(new RuntimeException("unexpected end of stream while parsing"));
      }
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
