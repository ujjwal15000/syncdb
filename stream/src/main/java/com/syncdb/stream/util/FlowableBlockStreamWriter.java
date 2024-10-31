package com.syncdb.stream.util;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.internal.disposables.SequentialDisposable;
import io.reactivex.rxjava3.internal.operators.flowable.FlowableTimeoutTimed;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.reactivex.rxjava3.operators.ConditionalSubscriber;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class FlowableBlockStreamWriter extends Flowable<ByteBuffer>
    implements FlowableTransformer<byte[], ByteBuffer> {
  private static final Long DEFAULT_FLUSH_TIMEOUT_MILLIS = 2_000L;

  private final Publisher<byte[]> source;
  private final int blockSize;
  private final byte[] delimiter;
  private final Scheduler scheduler = Schedulers.computation();

  private FlowableBlockStreamWriter(Publisher<byte[]> source, Integer blockSize, byte[] delimiter) {
    this.source = source;
    this.blockSize = blockSize;
    this.delimiter = delimiter;
  }

  public static FlowableBlockStreamWriter write(Integer blockSize, byte[] delimiter) {
    return new FlowableBlockStreamWriter(null, blockSize, delimiter);
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
        new BufferSubscriber(subscriber, buffer, delimiter, scheduler.createWorker());
    parent.startTimeout();
    this.source.subscribe(parent);
  }

  public Publisher<ByteBuffer> apply(Flowable<byte[]> upstream) {
    return new FlowableBlockStreamWriter(upstream, this.blockSize, this.delimiter);
  }

  public static class BufferSubscriber
      implements Subscription, ConditionalSubscriber<byte[]>, TimeoutSupport {
    private final Subscriber<? super ByteBuffer> downstream;
    private ByteBuffer buffer;
    Subscription upstream;
    private final byte[] delimiter;
    private final Scheduler.Worker worker;
    private final SequentialDisposable task;
    private final AtomicLong timeoutLock;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition timeoutCondition = lock.newCondition();

    public BufferSubscriber(
        Subscriber<? super ByteBuffer> downstream,
        ByteBuffer buffer,
        byte[] delimiter,
        Scheduler.Worker worker) {
      this.downstream = downstream;
      this.buffer = buffer;
      this.delimiter = delimiter;
      this.worker = worker;
      this.task = new SequentialDisposable();
      this.timeoutLock = new AtomicLong(0);
    }

    void startTimeout() {
      task.replace(
          worker.schedule(
              new TimeoutTask(this), DEFAULT_FLUSH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
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
      buffer.clear();
      downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
      checkTimeoutLock();

      if (buffer.position() > 0) {
        this.flushBuffer();
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

    @Override
    public boolean tryOnNext(@NonNull byte[] data) {
      checkTimeoutLock();

      if (data.length + delimiter.length <= buffer.remaining()) {
        buffer.put(data);
        buffer.put(delimiter);
        return false;
      }

      this.task.get().dispose();
      this.flushBuffer();
      this.startTimeout();

      buffer.put(data);
      buffer.put(delimiter);
      return true;
    }

    @Override
    public void onTimeout() {
      timeoutLock.incrementAndGet();
      try {
        if (buffer.position() > 0) this.flushBuffer();
      } finally {
        timeoutLock.decrementAndGet();
      }
    }

    // todo: might me a better way to do this
    private void checkTimeoutLock() {
      if (!(timeoutLock.get() == 0L)) {
        task.replace(
                worker.schedule(
                        () -> downstream.onError(new RuntimeException("timeout waiting for final buffer to flush")),
                        10_000,
                        TimeUnit.MILLISECONDS));
        while (timeoutLock.get() > 0L) {}
        task.get().dispose();
      }
    }

    private void flushBuffer() {
      buffer.flip();
      this.downstream.onNext(buffer);
      buffer.clear();
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
