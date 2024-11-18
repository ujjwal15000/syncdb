package com.syncdb.client;

import com.syncdb.core.protocol.ProtocolMessage;

public interface ProtocolMessageHandler {
    void handle(ProtocolMessage message) throws Throwable;
}
