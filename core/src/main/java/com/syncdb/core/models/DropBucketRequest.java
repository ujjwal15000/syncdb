package com.syncdb.core.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DropBucketRequest {
  private String namespace;
  private String name;
}
