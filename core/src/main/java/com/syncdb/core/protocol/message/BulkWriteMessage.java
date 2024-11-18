package com.syncdb.core.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.core.serde.deserializer.ByteDeserializer;
import com.syncdb.core.serde.serializer.ByteSerializer;
import com.syncdb.core.protocol.ProtocolMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BulkWriteMessage extends ProtocolMessage {
    private static final ByteDeserializer BYTE_DESERIALIZER =  new ByteDeserializer();
    private static final ByteSerializer BYTE_SERIALIZER =  new ByteSerializer();

    private final List<Record<byte[], byte[]>> records;

    public BulkWriteMessage(int seq, List<Record<byte[], byte[]>> records) {
        super(MESSAGE_TYPE.BULK_READ, seq, serializePayload(records));
        this.records = records;
    }

    private static byte[] serializePayload(List<Record<byte[], byte[]>> records) {
        List<byte[]> serializedRecords = new ArrayList<>();
        for(Record<byte[], byte[]> record: records)
            serializedRecords.add(Record.serialize(record, BYTE_SERIALIZER, BYTE_SERIALIZER));

        ByteBuffer buffer =
                ByteBuffer.allocate(
                        4 + serializedRecords.size() * 4 + serializedRecords.stream().map(r -> r.length).reduce(0, Integer::sum));
        buffer.putInt(serializedRecords.size());
        for (byte[] record: serializedRecords){
            buffer.putInt(record.length);
            buffer.put(record);
        }

        buffer.flip();
        byte[] res = buffer.array();
        buffer.clear();
        return res;
    }

    public static List<Record<byte[], byte[]>> deserializePayload(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        List<Record<byte[], byte[]>> res = new ArrayList<>(buffer.getInt());

        while (buffer.hasRemaining()){
            int len = buffer.getInt();
            byte[] key = new byte[len];
            res.add(Record.deserialize(key, BYTE_DESERIALIZER, BYTE_DESERIALIZER));
        }
        return res;
    }
}
