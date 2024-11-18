package com.syncdb.core.protocol.message;

import com.syncdb.core.protocol.ProtocolMessage;

import java.nio.charset.StandardCharsets;

public class ErrorMessage extends ProtocolMessage {

  public ErrorMessage(int seq, Exception e) {
    super(MESSAGE_TYPE.ERROR, seq, e.getMessage().getBytes(StandardCharsets.UTF_8));
  }

  public ErrorMessage(int seq, Throwable e) {
    super(MESSAGE_TYPE.ERROR, seq, e.getMessage().getBytes(StandardCharsets.UTF_8));
  }
}
