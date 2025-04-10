package com.scalar.db.storage.objectstorage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CloudStorageWrapper implements ObjectStorageWrapper {
  public static final String STORAGE_TYPE = "gcs";
  private final Storage storage;
  private final String bucket;

  public CloudStorageWrapper(Storage storage, String bucket) {
    this.storage = storage;
    this.bucket = bucket;
  }

  @Override
  public ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException {
    Blob blob = storage.get(BlobId.of(bucket, key));
    if (blob == null) {
      throw new ObjectStorageWrapperException(
          ObjectStorageWrapperException.StatusCode.NOT_FOUND, "Object not found: " + key);
    }
    return new ObjectStorageWrapperResponse(
        new String(blob.getContent(), StandardCharsets.UTF_8),
        String.valueOf(blob.getGeneration()));
  }

  @Override
  public Set<String> getKeys(String prefix) {
    Iterable<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll();
    return StreamSupport.stream(blobs.spliterator(), false)
        .map(Blob::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      storage.create(
          BlobInfo.newBuilder(BlobId.of(bucket, key)).build(),
          object.getBytes(StandardCharsets.UTF_8),
          Storage.BlobTargetOption.doesNotExist());
    } catch (StorageException e) {
      if (e.getCode() == 412) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
    }
  }

  @Override
  public void upsert(String key, String object) {
    storage.create(
        BlobInfo.newBuilder(BlobId.of(bucket, key)).build(),
        object.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public boolean updateIfVersionMatches(String key, String object, String version) {
    try {
      storage.create(
          BlobInfo.newBuilder(BlobId.of(bucket, key)).build(),
          object.getBytes(StandardCharsets.UTF_8),
          Storage.BlobTargetOption.generationMatch(Long.parseLong(version)));
      return true;
    } catch (StorageException e) {
      if (e.getCode() == 412) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      storage.delete(BlobId.of(bucket, key));
    } catch (StorageException e) {
      if (e.getCode() == 404) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public boolean deleteIfVersionMatches(String key, String version) {
    try {
      return storage.delete(
          BlobId.of(bucket, key),
          Storage.BlobSourceOption.generationMatch(Long.parseLong(version)));
    } catch (StorageException e) {
      if (e.getCode() == 412) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      storage.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close the storage", e);
    }
  }
}
