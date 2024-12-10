package com.syncdb.core.util;

import java.util.Arrays;

public class ByteArrayWrapper {
  private final byte[] data;

  public ByteArrayWrapper(byte[] data) {
    this.data = data;
  }

  public static ByteArrayWrapper create(byte[] data) {
    return new ByteArrayWrapper(data);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    ByteArrayWrapper other = (ByteArrayWrapper) obj;
    return Arrays.equals(data, other.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
