package com.syncdb.client.reader;

import com.syncdb.core.models.Record;
import com.syncdb.core.protocol.ProtocolMessage;
import com.syncdb.core.protocol.message.*;
import java.util.List;

public class SocketHandlers {

    public static class ReadAckHandler implements ProtocolMessageHandler {
        protected List<Record<byte[], byte[]>> records;

        @Override
        public void handle(ProtocolMessage message) throws Throwable {
            if (message.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR) {
                throw ErrorMessage.getThrowable(message.getPayload());
            }
            this.records = ReadAckMessage.deserializePayload(message.getPayload());
        }
    }

    public static class NoopHandler implements ProtocolMessageHandler {
        @Override
        public void handle(ProtocolMessage message) {
        }
    }

    public static class ErrorHandler implements ProtocolMessageHandler {
        @Override
        public void handle(ProtocolMessage message) throws Throwable {
            throw ErrorMessage.getThrowable(message.getPayload());
        }
    }

    public static class WriteAckHandler implements ProtocolMessageHandler {
        protected Integer seq;

        @Override
        public void handle(ProtocolMessage message) throws Throwable {
            if (message.getMessageType() == ProtocolMessage.MESSAGE_TYPE.ERROR) {
                throw ErrorMessage.getThrowable(message.getPayload());
            }
            this.seq = message.getSeq();
        }
    }
}
