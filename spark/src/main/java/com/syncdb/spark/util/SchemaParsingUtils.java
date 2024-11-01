package com.syncdb.spark.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class SchemaParsingUtils {

    public static <K, V> StructType getSchema(Class<K> keyClass, Class<V> valueClass) {
        DataType keyType = getDataTypeForClass(keyClass);
        DataType valueType = getDataTypeForClass(valueClass);

        return new StructType(new StructField[]{
                new StructField("key", keyType, true, Metadata.empty()),
                new StructField("value", valueType, true, Metadata.empty())
        });
    }

    public static DataType getDataTypeForClass(Class<?> clazz) {
        return JavaTypeInference.encoderFor(clazz).dataType();
    }
}
