package com.syncdb.core.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.core.serde.serializer.ByteSerializer;
import com.syncdb.core.protocol.ProtocolMessage;

public class WriteMessage extends ProtocolMessage {
    private static final ByteDeserializer BYTE_DESERIALIZER =  new ByteDeserializer();
    private static final ByteSerializer BYTE_SERIALIZER =  new ByteSerializer();

    public WriteMessage(int seq, Record<byte[], byte[]> record) {
        super(MESSAGE_TYPE.WRITE, seq, Record.serialize(record, BYTE_SERIALIZER, BYTE_SERIALIZER));
    }

    public static Record<byte[], byte[]> getRecord(ProtocolMessage message) {
        return Record.deserialize(message.getPayload(), BYTE_DESERIALIZER, BYTE_DESERIALIZER);
    }
}
