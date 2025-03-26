package com.scalar.db.storage.objectstorage;

public class ObjectStorageWrapperResponse {
  private final String value;
  private final String version;

  public ObjectStorageWrapperResponse(String value, String version) {
    this.value = value;
    this.version = version;
  }

  public String getValue() {
    return value;
  }

  public String getVersion() {
    return version;
  }
}
