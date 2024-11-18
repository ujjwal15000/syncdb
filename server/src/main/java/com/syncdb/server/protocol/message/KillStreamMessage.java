package com.syncdb.server.protocol.message;

import com.syncdb.core.util.ByteArrayUtils;
import com.syncdb.server.protocol.ProtocolMessage;

public class KillStreamMessage extends ProtocolMessage {

  public KillStreamMessage() {
    super(MESSAGE_TYPE.KILL_STREAM, -1, new byte[0]);
  }
}
