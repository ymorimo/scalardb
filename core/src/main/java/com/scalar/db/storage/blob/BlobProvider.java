package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedStorageProvider;
import com.scalar.db.common.CheckedDistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;

public class BlobProvider implements DistributedStorageProvider {
  @Override
  public String getName() {
    return BlobConfig.STORAGE_NAME;
  }

  @Override
  public Blob createDistributedStorage(DatabaseConfig config) {
    return new Blob(config);
  }

  @Override
  public DistributedStorageAdmin createDistributedStorageAdmin(DatabaseConfig config) {
    return new CheckedDistributedStorageAdmin(new BlobAdmin(config), config);
  }
}
