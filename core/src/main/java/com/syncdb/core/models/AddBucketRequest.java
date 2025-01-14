package com.syncdb.core.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AddBucketRequest {
    private String namespace;
    private String name;
    private Integer ttl;
}
