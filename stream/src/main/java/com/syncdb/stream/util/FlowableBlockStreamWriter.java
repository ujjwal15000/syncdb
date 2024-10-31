package com.syncdb.stream.util;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.exceptions.Exceptions;
import io.reactivex.rxjava3.internal.subscriptions.EmptySubscription;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.reactivex.rxjava3.operators.ConditionalSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;

@Slf4j
public class FlowableBlockStreamWriter extends Flowable<ByteBuffer>
        implements FlowableTransformer<byte[], ByteBuffer> {

    private final Publisher<byte[]> source;
    private final Integer blockSize;
    private final byte[] delimiter;

    private FlowableBlockStreamWriter(Publisher<byte[]> source, Integer blockSize, byte[] delimiter) {
        this.source = source;
        this.blockSize = blockSize;
        this.delimiter = delimiter;
    }

    public static FlowableBlockStreamWriter write(Integer blockSize, byte[] delimiter){
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

        this.source.subscribe(new BufferSubscriber(subscriber, buffer, delimiter));
    }

    public Publisher<ByteBuffer> apply(Flowable<byte[]> upstream) {
        return new FlowableBlockStreamWriter(upstream, this.blockSize, this.delimiter);
    }

    public static class BufferSubscriber implements Subscription, ConditionalSubscriber<byte[]> {
        private final Subscriber<? super ByteBuffer> downstream;
        private ByteBuffer buffer;
        Subscription upstream;
        private final byte[] delimiter;

        public BufferSubscriber(Subscriber<? super ByteBuffer> downstream, ByteBuffer buffer, byte[] delimiter) {
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
            if (buffer.position() > 0) {
                buffer.flip();
                this.downstream.onNext(buffer);
                buffer.clear();
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
            if (data.length + delimiter.length <= buffer.remaining()) {
                buffer.put(data);
                buffer.put(delimiter);
                return false;
            }
            buffer.flip();
            this.downstream.onNext(buffer);
            buffer.clear();

            buffer.put(data);
            buffer.put(delimiter);
            return true;
        }
    }
}
