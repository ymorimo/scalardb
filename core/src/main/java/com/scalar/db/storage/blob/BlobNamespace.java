package com.scalar.db.storage.blob;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class BlobNamespace extends BlobDatabaseObject {
  private final String name;

  public BlobNamespace() {
    this(null);
  }

  public BlobNamespace(@Nullable String name) {
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
    if (!(o instanceof BlobNamespace)) {
      return false;
    }
    BlobNamespace that = (BlobNamespace) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
