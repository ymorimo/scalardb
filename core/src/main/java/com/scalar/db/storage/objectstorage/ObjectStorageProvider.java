package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class ObjectStorageProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return ObjectStorageConfig.STORAGE_NAME;
  }

  @Override
  public ObjectStorage createDistributedStorage(DatabaseConfig config) {
    return new ObjectStorage(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new CheckedDistributedStorageAdmin(new ObjectStorageAdmin(config), config);
  }
}
