package com.syncdb.client.reader;

import com.syncdb.client.ProtocolMessageHandler;
import com.syncdb.core.protocol.ProtocolMessage;

import java.util.HashMap;
import java.util.Map;

public class ProtocolHandlerRegistry {
    private final Map<ProtocolMessage.MESSAGE_TYPE, ProtocolMessageHandler> handlers = new HashMap<>();

    public void registerHandler(ProtocolMessage.MESSAGE_TYPE type, ProtocolMessageHandler handler) {
        handlers.put(type, handler);
    }

    public void registerDefaults(){
        handlers.put(ProtocolMessage.MESSAGE_TYPE.NOOP, new DefaultHandlers.NoopHandler());
        handlers.put(ProtocolMessage.MESSAGE_TYPE.READ_ACK, new DefaultHandlers.ReadAckHandler());
        handlers.put(ProtocolMessage.MESSAGE_TYPE.WRITE_ACK, new DefaultHandlers.WriteAckHandler());
        handlers.put(ProtocolMessage.MESSAGE_TYPE.ERROR, new DefaultHandlers.ErrorHandler());
    }

    public ProtocolMessageHandler getHandler(ProtocolMessage.MESSAGE_TYPE type) {
        return handlers.get(type);
    }
}
