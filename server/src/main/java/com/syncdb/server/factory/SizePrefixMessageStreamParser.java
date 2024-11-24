package com.syncdb.server.factory;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.vertx.rxjava3.core.buffer.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.vertx.rxjava3.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class SizePrefixMessageStreamParser extends Flowable<List<byte[]>>
    implements FlowableTransformer<Message<byte[]>, List<byte[]>> {

  private final Publisher<Message<byte[]>> source;
  private final Integer bufferSize;

  private SizePrefixMessageStreamParser(Publisher<Message<byte[]>> source, Integer bufferSize) {
    this.source = source;
    this.bufferSize = bufferSize;
  }

  public static SizePrefixMessageStreamParser read(Integer bufferSize) {
    return new SizePrefixMessageStreamParser(null, bufferSize);
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super List<byte[]>> subscriber) {
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

  public Publisher<List<byte[]>> apply(Flowable<Message<byte[]>> upstream) {
    return new SizePrefixMessageStreamParser(upstream, this.bufferSize);
  }

  public static class BufferSubscriber implements Subscription, Subscriber<Message<byte[]>> {
    private final Subscriber<? super List<byte[]>> downstream;
    private final ByteBuffer buffer;
    Subscription upstream;

    public BufferSubscriber(Subscriber<? super List<byte[]>> downstream, ByteBuffer buffer) {
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
    public void onNext(Message<byte[]> consumerMessage) {
      byte[] data = consumerMessage.body();
      List<byte[]> messages = new ArrayList<>();
      try {
        for (int i = 0; i < data.length; i++) {
          byte currentByte = data[i];
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
              byte[] message = new byte[buffer.limit()];
              buffer.get(message);
              messages.add(message);
              buffer.clear();
              currentSize = 0;
              sizeReader = true;
            }
          }
        }
      } catch (Exception e) {
        this.onError(e);
      }
      this.downstream.onNext(messages);
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
