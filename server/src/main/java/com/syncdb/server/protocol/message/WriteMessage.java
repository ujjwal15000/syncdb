package com.syncdb.server.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.core.serde.serializer.ByteSerializer;
import com.syncdb.server.protocol.ProtocolMessage;

public class WriteMessage extends ProtocolMessage {
    private static final ByteDeserializer BYTE_DESERIALIZER =  new ByteDeserializer();
    private static final ByteSerializer BYTE_SERIALIZER =  new ByteSerializer();
    private final Record<byte[], byte[]> record;

    public WriteMessage(int seq, Record<byte[], byte[]> record) {
        super(MESSAGE_TYPE.WRITE, seq, Record.serialize(record, BYTE_SERIALIZER, BYTE_SERIALIZER));
        this.record = record;
    }

    public Record<byte[], byte[]> getRecord() {
        return Record.deserialize(getPayload(), BYTE_DESERIALIZER, BYTE_DESERIALIZER);
    }
}
