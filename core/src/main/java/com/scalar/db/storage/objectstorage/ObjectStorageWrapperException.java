package com.scalar.db.storage.objectstorage;

public class ObjectStorageWrapperException extends Exception {
  private final StatusCode code;

  public ObjectStorageWrapperException(StatusCode code, String message) {
    super(message);
    this.code = code;
  }

  public ObjectStorageWrapperException(StatusCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public ObjectStorageWrapperException(StatusCode code, Throwable cause) {
    super(cause);
    this.code = code;
  }

  public StatusCode getCode() {
    return code;
  }

  public enum StatusCode {
    NOT_FOUND,
    ALREADY_EXISTS,
    CONFLICT,
  }
}
