package com.scalar.db.storage.blob;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonConvertor {
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
    mapper.registerModule(new JavaTimeModule());
  }

  public static <T extends BlobDatabaseObject> T deserialize(String json, Class<T> clazz) {
    try {
      return mapper.readValue(json, clazz);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the object.", e);
    }
  }

  public static String serialize(BlobDatabaseObject object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the object.", e);
    }
  }
}
