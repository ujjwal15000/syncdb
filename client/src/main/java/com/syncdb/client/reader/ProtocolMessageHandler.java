package com.syncdb.client.reader;

import com.syncdb.core.protocol.ProtocolMessage;

public interface ProtocolMessageHandler {
    void handle(ProtocolMessage message) throws Throwable;
}
