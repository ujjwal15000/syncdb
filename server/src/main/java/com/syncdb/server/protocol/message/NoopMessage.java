package com.syncdb.server.protocol.message;

import com.syncdb.server.protocol.ProtocolMessage;

public class NoopMessage extends ProtocolMessage {
  public NoopMessage() {
    super(MESSAGE_TYPE.NOOP, -1, new byte[0]);
  }
}
