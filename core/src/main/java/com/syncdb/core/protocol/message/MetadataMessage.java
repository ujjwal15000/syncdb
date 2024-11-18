package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.ClientMetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class MetadataMessage extends ProtocolMessage {
    private final ClientMetadata clientMetadata;

    public MetadataMessage(ClientMetadata clientMetadata) {
        super(MESSAGE_TYPE.METADATA, 0, serializePayload(clientMetadata));
        this.clientMetadata = clientMetadata;
    }

    public static byte[] serializePayload(ClientMetadata clientMetadata){
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putInt(clientMetadata.getClientId().length());
        buffer.put(clientMetadata.getClientId().getBytes(StandardCharsets.UTF_8));
        buffer.putInt(clientMetadata.getNamespace().length());
        buffer.put(clientMetadata.getNamespace().getBytes(StandardCharsets.UTF_8));
        buffer.putInt(clientMetadata.getPartitionId());
        buffer.put((byte) (clientMetadata.getIsStreamWriter() ? 1 : 0));

        buffer.flip();
        byte[] res = buffer.array();
        buffer.clear();
        return res;
    }

    public static ClientMetadata deserializePayload(byte[] payload){
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int clientIdLen = buffer.getInt();
        byte[] clientId = new byte[clientIdLen];
        buffer.get(clientId);

        int namespaceLen = buffer.getInt();
        byte[] namespace = new byte[namespaceLen];
        buffer.get(namespace);

        int partitionId = buffer.getInt();
        Boolean streamWriter = buffer.get() == (byte) 1;
        return new ClientMetadata(new String(clientId), new String(namespace), partitionId, streamWriter);
    }
}
