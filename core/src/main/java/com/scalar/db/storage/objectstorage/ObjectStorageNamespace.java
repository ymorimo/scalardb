package com.scalar.db.storage.objectstorage;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageNamespace {
  private final String name;

  // The default constructor  is required by Jackson to deserialize JSON object
  public ObjectStorageNamespace() {
    this(null);
  }

  public ObjectStorageNamespace(@Nullable String name) {
    this.name = name != null ? name : "";
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ObjectStorageNamespace)) {
      return false;
    }
    ObjectStorageNamespace that = (ObjectStorageNamespace) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
