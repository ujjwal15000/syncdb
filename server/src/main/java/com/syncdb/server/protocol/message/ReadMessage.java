package com.syncdb.server.protocol.message;

import com.syncdb.core.models.Record;
import com.syncdb.server.protocol.ProtocolMessage;

public class ReadMessage extends ProtocolMessage {
    private final byte[] key;

    public ReadMessage(int seq, byte[] key) {
        super(MESSAGE_TYPE.READ, seq, key);
        this.key = key;
    }

    public byte[] getKey() {
        return this.getPayload();
    }
}
