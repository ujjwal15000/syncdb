package com.syncdb.core.protocol;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class ClientMetadata implements Serializable {
    private final String clientId;
    private final String namespace;
    private final Integer partitionId;
    private final Boolean isStreamWriter;

    public ClientMetadata(String clientId, String namespace, Integer partitionId, Boolean isStreamWriter) {
        this.clientId = clientId;
        this.namespace = namespace;
        this.partitionId = partitionId;
        this.isStreamWriter = isStreamWriter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ClientMetadata that = (ClientMetadata) obj;
        return Objects.equals(clientId, that.clientId) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(partitionId, that.partitionId) &&
                Objects.equals(isStreamWriter, that.isStreamWriter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, namespace, partitionId, isStreamWriter);
    }

}
