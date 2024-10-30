package com.syncdb.wal.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.msgpack.jackson.dataformat.MessagePackFactory;

@Slf4j
public class ObjectMapperUtils {
    public static ObjectMapper getMsgPackObjectMapper(){
        return new ObjectMapper(new MessagePackFactory());
    }
}
