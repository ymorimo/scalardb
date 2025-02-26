package com.scalar.db.storage.blob;

public class BlobClientWrapperResponse {
  private final String value;
  private final String eTag;

  public BlobClientWrapperResponse(String value, String eTag) {
    this.value = value;
    this.eTag = eTag;
  }

  public String getValue() {
    return value;
  }

  public String getETag() {
    return eTag;
  }
}
