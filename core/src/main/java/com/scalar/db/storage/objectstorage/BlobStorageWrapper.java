package com.scalar.db.storage.objectstorage;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobDownloadContentResponse;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobStorageWrapper implements ObjectStorageWrapper {
  public static final String STORAGE_TYPE = "blob";

  private final BlobContainerClient client;

  public BlobStorageWrapper(BlobContainerClient client) {
    this.client = client;
  }

  public ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(null, null, Duration.ofSeconds(5), null);
      String data = response.getValue().toString();
      String eTag = response.getHeaders().getValue(HttpHeaderName.ETAG);
      return new ObjectStorageWrapperResponse(data, eTag);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  public Set<String> getKeys(String prefix) {
    return client
        .listBlobs(new ListBlobsOptions().setPrefix(prefix), Duration.ofSeconds(5))
        .stream()
        .map(BlobItem::getName)
        .collect(Collectors.toSet());
  }

  public void insert(String key, String object) throws ObjectStorageWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.upload(BinaryData.fromString(object), false);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_ALREADY_EXISTS)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
    }
  }

  public boolean updateIfVersionMatches(String key, String object, String version) {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.uploadWithResponse(
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfMatch(version)),
          Duration.ofSeconds(5),
          null);
      return true;
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        return false;
      }
      throw e;
    }
  }

  public void delete(String key) throws ObjectStorageWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.delete();
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  public boolean deleteIfVersionMatches(String key, String version) {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.deleteWithResponse(
          null, new BlobRequestConditions().setIfMatch(version), Duration.ofSeconds(5), null);
      return true;
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        return false;
      }
      throw e;
    }
  }

  public void close() {
    // Do nothing
  }
}
