// automatically generated by the FlatBuffers compiler, do not modify
package com.syncdb.core.flatbuffers;


import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings("unused")
public final class Record extends Table {
    public static void ValidateVersion() { Constants.FLATBUFFERS_24_3_25(); }
    public static Record getRootAsRecord(ByteBuffer _bb) { return getRootAsRecord(_bb, new Record()); }
    public static Record getRootAsRecord(ByteBuffer _bb, Record obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
    public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
    public Record __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

    public int key(int j) { int o = __offset(4); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
    public int keyLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
    public ByteVector keyVector() { return keyVector(new ByteVector()); }
    public ByteVector keyVector(ByteVector obj) { int o = __offset(4); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
    public ByteBuffer keyAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
    public ByteBuffer keyInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
    public int value(int j) { int o = __offset(6); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
    public int valueLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
    public ByteVector valueVector() { return valueVector(new ByteVector()); }
    public ByteVector valueVector(ByteVector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
    public ByteBuffer valueAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
    public ByteBuffer valueInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }

    public static int createRecord(FlatBufferBuilder builder,
                                   int keyOffset,
                                   int valueOffset) {
        builder.startTable(2);
        Record.addValue(builder, valueOffset);
        Record.addKey(builder, keyOffset);
        return Record.endRecord(builder);
    }

    public static void startRecord(FlatBufferBuilder builder) { builder.startTable(2); }
    public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(0, keyOffset, 0); }
    public static int createKeyVector(FlatBufferBuilder builder, byte[] data) { return builder.createByteVector(data); }
    public static int createKeyVector(FlatBufferBuilder builder, ByteBuffer data) { return builder.createByteVector(data); }
    public static void startKeyVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
    public static void addValue(FlatBufferBuilder builder, int valueOffset) { builder.addOffset(1, valueOffset, 0); }
    public static int createValueVector(FlatBufferBuilder builder, byte[] data) { return builder.createByteVector(data); }
    public static int createValueVector(FlatBufferBuilder builder, ByteBuffer data) { return builder.createByteVector(data); }
    public static void startValueVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
    public static int endRecord(FlatBufferBuilder builder) {
        int o = builder.endTable();
        return o;
    }
    public static void finishRecordBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
    public static void finishSizePrefixedRecordBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

    public static final class Vector extends BaseVector {
        public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

        public Record get(int j) { return get(new Record(), j); }
        public Record get(Record obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
    }
}

