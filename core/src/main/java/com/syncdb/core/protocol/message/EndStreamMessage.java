package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

public class EndStreamMessage extends ProtocolMessage {
    public EndStreamMessage() {
        super(ProtocolMessage.MESSAGE_TYPE.END_STREAM, -1, new byte[0]);
    }
}
