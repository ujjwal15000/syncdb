package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

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
