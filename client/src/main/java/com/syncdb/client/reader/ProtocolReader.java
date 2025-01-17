package com.syncdb.client.reader;

import com.syncdb.core.protocol.ProtocolMessage;

public class ProtocolReader {
    private final ProtocolHandlerRegistry registry;

    public ProtocolReader(ProtocolHandlerRegistry registry) {
        this.registry = registry;
    }

    public void read(ProtocolMessage message) throws Throwable {
        ProtocolMessageHandler handler = registry.getHandler(message.getMessageType());
        if (handler != null) {
            handler.handle(message);
        } else {
            throw new RuntimeException(
                String.format("No handler registered for message type: %s", message.getMessageType().name()));
        }
    }
}
