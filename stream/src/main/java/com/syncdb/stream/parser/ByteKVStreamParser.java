package com.syncdb.stream.parser;

import com.syncdb.core.models.Record;
import io.reactivex.rxjava3.core.Flowable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.InputStream;
import java.util.Objects;

@Slf4j
public class ByteKVStreamParser {
    // parses msgpack Record<byte[], byte[]> stream
    public Flowable<Record<byte[], byte[]>> parseStream(InputStream inputStream){
        return Flowable.generate(() -> MessagePack.newDefaultUnpacker(inputStream), (messageUnpacker, emitter) -> {
            while (messageUnpacker.hasNext()){
                emitter.onNext(unpackByteByteRecord(messageUnpacker));
            }
            emitter.onComplete();
        });
    }

    @SneakyThrows
    public static Record<byte[], byte[]> unpackByteByteRecord(MessageUnpacker unpacker){
        Record.RecordBuilder<byte[], byte[]> builder = Record.builder();

        int mapHeader = unpacker.unpackMapHeader();
        while (--mapHeader >= 0) {
            String unpackedString = unpacker.unpackString();
            int numBytes = unpacker.unpackBinaryHeader();
            byte[] bytes = new byte[numBytes];
            for (int i = 0; i < numBytes; i++) {
                bytes[i] = unpacker.unpackByte();
            }
            if(Objects.equals(unpackedString, "key")){
                builder.key(bytes);
            }
            else if(Objects.equals(unpackedString, "value")){
                builder.value(bytes);
            }
            else {
                throw new RuntimeException("unexpected unpacked string in record was expecting key or value!");
            }
        }
        return builder.build();
    }
}

