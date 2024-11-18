package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

public class NoopMessage extends ProtocolMessage {
  public NoopMessage() {
    super(MESSAGE_TYPE.NOOP, -1, new byte[0]);
  }
}
