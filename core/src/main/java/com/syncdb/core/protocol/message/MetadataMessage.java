package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.SocketMetadata;


public class MetadataMessage extends ProtocolMessage {
    private final SocketMetadata socketMetadata;

    public MetadataMessage(SocketMetadata socketMetadata) {
        super(MESSAGE_TYPE.METADATA, 0, SocketMetadata.serialize(socketMetadata));
        this.socketMetadata = socketMetadata;
    }
}
