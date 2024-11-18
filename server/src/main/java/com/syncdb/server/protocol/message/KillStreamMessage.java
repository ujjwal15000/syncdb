package com.syncdb.server.protocol.message;

import com.syncdb.core.util.ByteArrayUtils;
import com.syncdb.server.protocol.ProtocolMessage;

import java.nio.charset.StandardCharsets;

public class KillStreamMessage extends ProtocolMessage {

  public KillStreamMessage() {
    super(MESSAGE_TYPE.KILL_STREAM, -1, new byte[0]);
  }

  public KillStreamMessage(Throwable e) {
    super(MESSAGE_TYPE.KILL_STREAM, -1, e.getMessage().getBytes(StandardCharsets.UTF_8));
  }

  public KillStreamMessage(Exception e) {
    super(MESSAGE_TYPE.KILL_STREAM, -1, e.getMessage().getBytes(StandardCharsets.UTF_8));
  }
}
