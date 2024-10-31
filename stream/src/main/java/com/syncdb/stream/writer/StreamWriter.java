package com.syncdb.stream.writer;

import com.syncdb.stream.models.Record;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

public interface StreamWriter<K,V, M> {
    private Completable putStreamMetadata(M metadata){
        return Completable.complete();
    }

    Completable writeStream(Flowable<Record<K, V>> stream);
}
