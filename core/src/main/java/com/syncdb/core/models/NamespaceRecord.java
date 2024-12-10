package com.syncdb.core.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NamespaceRecord {
    private String namespace;
    private String key;
    private String value;
}
