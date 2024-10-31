package com.syncdb.stream.writer;

import io.reactivex.rxjava3.core.Completable;

public interface StreamWriter<K,V, M> {
    private Completable putStreamMetadata(M metadata){
        return Completable.complete();
    }
}
