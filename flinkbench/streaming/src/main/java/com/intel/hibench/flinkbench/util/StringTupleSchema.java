package com.intel.hibench.flinkbench.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Very simple serialization schema for strings of tuple.
 */
public class StringTupleSchema implements DeserializationSchema<Tuple2<String, String>>, SerializationSchema<Tuple2<String, String>> {

  private static final long serialVersionUID = 1L;

  @Override
  public Tuple2<String, String> deserialize(byte[] message) {
    byte[] str1 = new byte[message.length];
    byte[] str2 = new byte[message.length];
    int index = 0;
    for (int i = 0; i < message.length; i++) {

      if (message[i] == ' ') {
        index = i;
        break;
      }
      str1[i] = message[i];
    }
    for (int i = index + 1; i < message.length; i++) {
      str2[i - index - 1] = message[i];
    }
    return new Tuple2<String, String>(new String(str1, 0, index), new String(str2, 0, message.length - index - 1));
  }

  @Override
  public boolean isEndOfStream(Tuple2<String, String> nextElement) {
    return false;
  }

  @Override
  public byte[] serialize(Tuple2<String, String> element) {
    byte[] str1 = element.f0.getBytes();
    byte[] str2 = element.f1.getBytes();
    int len1 = str1.length;
    int len2 = str2.length;
    byte[] result = new byte[len1 + len2 + 1];
    System.arraycopy(str1, 0, result, 0, len1);
    result[len1] = ' ';
    for (int i = len1 + 1; i <= len1 + len2; i++) {
      result[i] = str2[i - len1 - 1];
    }
    return result;
  }

  @Override
  public TypeInformation<Tuple2<String, String>> getProducedType() {
    return new TupleTypeInfo<Tuple2<String, String>>(TypeExtractor.createTypeInfo(String.class), TypeExtractor.createTypeInfo(String.class));
  }
}
