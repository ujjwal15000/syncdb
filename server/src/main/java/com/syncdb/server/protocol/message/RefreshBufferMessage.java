package com.syncdb.server.protocol.message;

import com.syncdb.core.util.ByteArrayUtils;
import com.syncdb.server.protocol.ProtocolMessage;

public class RefreshBufferMessage extends ProtocolMessage {
  private final Long bufferSize;

  public RefreshBufferMessage(Long bufferSize) {
    super(MESSAGE_TYPE.REFRESH_BUFFER, -1, ByteArrayUtils.convertToByteArray(bufferSize));
    this.bufferSize = bufferSize;
  }
}
