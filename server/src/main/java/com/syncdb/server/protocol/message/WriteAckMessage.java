package com.syncdb.server.protocol.message;

import com.syncdb.server.protocol.ProtocolMessage;

public class WriteAckMessage extends ProtocolMessage {
    public WriteAckMessage(int seq) {
        super(MESSAGE_TYPE.WRITE_ACK, seq, new byte[0]);
    }
}
