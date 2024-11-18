package com.syncdb.server.protocol.message;

import com.syncdb.server.protocol.ProtocolMessage;
import com.syncdb.server.protocol.SocketMetadata;


public class MetadataMessage extends ProtocolMessage {
    private final SocketMetadata socketMetadata;

    public MetadataMessage(SocketMetadata socketMetadata) {
        super(MESSAGE_TYPE.METADATA, 0, SocketMetadata.serialize(socketMetadata));
        this.socketMetadata = socketMetadata;
    }
}
