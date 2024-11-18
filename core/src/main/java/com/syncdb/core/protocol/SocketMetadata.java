package com.syncdb.core.protocol;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Getter
public class SocketMetadata {
    private final String clientId;
    private final String namespace;
    private final Integer partitionId;
    private final Boolean streamWriter;

    public SocketMetadata(String clientId, String namespace, Integer partitionId, Boolean streamWriter) {
        this.clientId = clientId;
        this.namespace = namespace;
        this.partitionId = partitionId;
        this.streamWriter = streamWriter;
    }

    public static byte[] serialize(SocketMetadata socketMetadata){
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putInt(socketMetadata.clientId.length());
        buffer.put(socketMetadata.clientId.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(socketMetadata.namespace.length());
        buffer.put(socketMetadata.namespace.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(socketMetadata.partitionId);
        buffer.put((byte) (socketMetadata.streamWriter ? 1 : 0));

        buffer.flip();
        byte[] res = buffer.array();
        buffer.clear();
        return res;
    }

    public static SocketMetadata deserialize(byte[] payload){
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        int clientIdLen = buffer.getInt();
        byte[] clientId = new byte[clientIdLen];
        buffer.get(clientId);

        int namespaceLen = buffer.getInt();
        byte[] namespace = new byte[namespaceLen];
        buffer.get(namespace);

        int partitionId = buffer.getInt();
        Boolean streamWriter = buffer.get() == (byte) 1;
        return new SocketMetadata(new String(clientId), new String(namespace), partitionId, streamWriter);
    }
}
