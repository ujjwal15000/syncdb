package com.syncdb.stream.reader;

import io.reactivex.rxjava3.core.Single;

public interface StreamReader<K, V, M> {
    Single<M> getStreamMetadata();
}
