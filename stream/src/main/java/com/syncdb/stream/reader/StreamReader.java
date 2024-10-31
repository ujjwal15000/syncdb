package com.syncdb.stream.reader;

import com.syncdb.stream.models.Record;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

public interface StreamReader<K, V, M> {
    Single<M> getStreamMetadata();

    Flowable<Record<K, V>> readStream(Long offset);
}
