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
public class FlowableSizePrefixStreamReader extends Flowable<byte[]>
    implements FlowableTransformer<ByteBuffer, byte[]> {

  private final Publisher<ByteBuffer> source;
  private final Integer bufferSize;

  private FlowableSizePrefixStreamReader(Publisher<ByteBuffer> source, Integer bufferSize) {
    this.source = source;
    this.bufferSize = bufferSize;
  }

  public static FlowableSizePrefixStreamReader read(Integer bufferSize) {
    return new FlowableSizePrefixStreamReader(null, bufferSize);
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

    this.source.subscribe(new BufferSubscriber(subscriber, buffer));
  }

  public Publisher<byte[]> apply(Flowable<ByteBuffer> upstream) {
    return new FlowableSizePrefixStreamReader(upstream, this.bufferSize);
  }

  public static class BufferSubscriber implements Subscription, Subscriber<ByteBuffer> {
    private final Subscriber<? super byte[]> downstream;
    private final ByteBuffer buffer;
    Subscription upstream;

    public BufferSubscriber(Subscriber<? super byte[]> downstream, ByteBuffer buffer) {
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

    boolean sizeReader = true;
    int currentSize = 0;

    @Override
    public void onNext(ByteBuffer data) {
      try {
        while (data.hasRemaining()) {
          byte currentByte = data.get();
          buffer.put(currentByte);
          if (sizeReader) {
            if(buffer.position() == 4){
              currentSize = buffer.getInt(0);
              buffer.clear();
              sizeReader = false;
            }
          } else {
            if(buffer.position() == currentSize) {
              buffer.flip();
              this.downstream.onNext(buffer.array());
              buffer.clear();
              currentSize = 0;
              sizeReader = true;
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
