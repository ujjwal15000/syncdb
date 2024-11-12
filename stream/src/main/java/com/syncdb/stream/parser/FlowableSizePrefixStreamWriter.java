package com.syncdb.stream.parser;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.internal.disposables.SequentialDisposable;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public class FlowableSizePrefixStreamWriter extends Flowable<ByteBuffer>
    implements FlowableTransformer<byte[], ByteBuffer> {
  private final Publisher<byte[]> source;
  private final int blockSize;
  private final Scheduler scheduler = Schedulers.computation();
  private final Long flushTimeout;

  private FlowableSizePrefixStreamWriter(
      Publisher<byte[]> source, Integer blockSize, Long flushTimeout) {
    this.source = source;
    this.blockSize = blockSize;
    this.flushTimeout = flushTimeout;
  }

  public static FlowableSizePrefixStreamWriter write(
      Integer blockSize, Long flushTimeout) {
    return new FlowableSizePrefixStreamWriter(null, blockSize, flushTimeout);
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super ByteBuffer> subscriber) {
    ByteBuffer buffer;
    try {
      buffer = ByteBuffer.allocate(blockSize);
    } catch (Exception e) {
      Exceptions.throwIfFatal(e);
      EmptySubscription.error(e, subscriber);
      return;
    }
    BufferSubscriber parent =
        new BufferSubscriber(subscriber, buffer, scheduler.createWorker(), flushTimeout);
    parent.startTimeout();
    this.source.subscribe(parent);
  }

  public Publisher<ByteBuffer> apply(Flowable<byte[]> upstream) {
    return new FlowableSizePrefixStreamWriter(
        upstream, this.blockSize, this.flushTimeout);
  }

  public static class BufferSubscriber
          implements Subscription, TimeoutSupport, FlowableSubscriber<byte[]> {
    private final Subscriber<? super ByteBuffer> downstream;
    private final ByteBuffer buffer;
    Subscription upstream;
    private final Scheduler.Worker worker;
    private final SequentialDisposable task;
    private final Long flushTimeout;

    public BufferSubscriber(
        Subscriber<? super ByteBuffer> downstream,
        ByteBuffer buffer,
        Scheduler.Worker worker,
        Long flushTimeout) {
      this.downstream = downstream;
      this.buffer = buffer;
      this.worker = worker;
      this.task = new SequentialDisposable();
      this.flushTimeout = flushTimeout;
    }

    void startTimeout() {
      task.replace(worker.schedule(new TimeoutTask(this), flushTimeout, TimeUnit.MILLISECONDS));
    }

    @Override
    public void onSubscribe(@NonNull Subscription upstream) {
      if (SubscriptionHelper.validate(this.upstream, upstream)) {
        this.upstream = upstream;
        this.downstream.onSubscribe(this);
      }
    }

    @Override
    public void onNext(byte[] buffer) {
      if (!this.tryOnNext(buffer)) {
        this.upstream.request(1L);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      synchronized (buffer) {
        buffer.clear();
      }
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      try {
        synchronized (this) {
          if (buffer.position() > 0) {
            buffer.flip();
            this.downstream.onNext(buffer);
            buffer.clear();
          }
        }
      } catch (Exception e) {
        this.onError(e);
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

    public boolean tryOnNext(@NonNull byte[] data) {
      try {
        synchronized (this) {
          // integer size
          if (4 + data.length <= buffer.remaining()) {
            buffer.putInt(data.length);
            buffer.put(data);
            return false;
          }
        }

        this.task.get().dispose();
        synchronized (this) {
          if (buffer.position() > 0) {
            buffer.flip();
            this.downstream.onNext(buffer);
            buffer.clear();
          }
        }
        this.startTimeout();

        synchronized (this) {
          buffer.putInt(data.length);
          buffer.put(data);
        }
      } catch (Exception e) {
        this.onError(e);
      }
      return true;
    }

    @Override
    public void onTimeout() {
      try {
        synchronized (this) {
          if (buffer.position() > 0) {
            buffer.flip();
            this.downstream.onNext(buffer);
            buffer.clear();
          }
        }
      } catch (Exception e) {
        this.onError(e);
      }
    }

    private static final class TimeoutTask implements Runnable {
      final TimeoutSupport parent;

      TimeoutTask(TimeoutSupport parent) {
        this.parent = parent;
      }

      @Override
      public void run() {
        parent.onTimeout();
      }
    }
  }

  private interface TimeoutSupport {
    void onTimeout();
  }
}
