package com.syncdb.stream.producer;

import com.syncdb.stream.models.Record;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.Getter;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

import static com.syncdb.stream.models.Record.EMPTY_RECORD;

public class StreamProducer<K, V> extends MockProducer<K, V> {
    private final Deque<Record<K, V>> recordDeque;

    @Getter
    private final Flowable<Record<K, V>> recordStream;
    private Boolean streamKilled = false;

    public StreamProducer(){
        super();
        this.recordDeque = new ConcurrentLinkedDeque<>();
        this.recordStream = generateStream();
    }

    @SuppressWarnings("unchecked")
    private Flowable<Record<K, V>> generateStream(){
        return Flowable.generate(emitter -> {
            if(streamKilled)
                emitter.onComplete();

            if(recordDeque.peek() != null)
                emitter.onNext(recordDeque.poll());
            else
                emitter.onNext((Record<K,V>)EMPTY_RECORD);

        });
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record){
        this.recordDeque.add(Record.<K,V>builder().key(record.key()).value(record.value()).build());
        return Single.just(new RecordMetadata(new TopicPartition("", 0), 0L, 0, 0L, 4, 4)).toFuture();
    }

    @Override
    public void close(){
        streamKilled = true;
        super.close();
    }
}
