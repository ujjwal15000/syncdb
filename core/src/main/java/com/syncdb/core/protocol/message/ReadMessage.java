package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

public class ReadMessage extends ProtocolMessage {
    private final byte[] key;

    public ReadMessage(int seq, byte[] key) {
        super(MESSAGE_TYPE.READ, seq, key);
        this.key = key;
    }

    public static byte[] getKey(ProtocolMessage message) {
        return message.getPayload();
    }
}
