package com.scalar.db.storage.blob;

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

public class BlobClientWrapper {
  private final BlobContainerClient client;

  public BlobClientWrapper(BlobContainerClient client) {
    this.client = client;
  }

  public BlobClientWrapperResponse get(String key) throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(null, null, Duration.ofSeconds(5), null);
      String data = response.getValue().toString();
      String eTag = response.getHeaders().getValue(HttpHeaderName.ETAG);
      return new BlobClientWrapperResponse(data, eTag);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new BlobClientWrapperException(BlobClientWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  public void insert(String key, String value) throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.upload(BinaryData.fromString(value), false);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_ALREADY_EXISTS)) {
        throw new BlobClientWrapperException(
            BlobClientWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
    }
  }

  public boolean compareAndSwap(String key, String value, String eTag)
      throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.uploadWithResponse(
          new BlobParallelUploadOptions(BinaryData.fromString(value))
              .setRequestConditions(new BlobRequestConditions().setIfMatch(eTag)),
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

  public void delete(String key) throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.delete();
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new BlobClientWrapperException(BlobClientWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  public void deleteIfExists(String key) throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    blobClient.deleteIfExists();
  }

  public boolean compareAndDelete(String key, String eTag) throws BlobClientWrapperException {
    BlobClient blobClient = client.getBlobClient(key);
    try {
      blobClient.deleteWithResponse(
          null, new BlobRequestConditions().setIfMatch(eTag), Duration.ofSeconds(5), null);
      return true;
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        return false;
      }
      throw e;
    }
  }

  public Set<String> listKeys(String prefix) {
    return client.listBlobs(new ListBlobsOptions().setPrefix(prefix), null).stream()
        .map(BlobItem::getName)
        .collect(Collectors.toSet());
  }

  public void close() {
    // Do nothing
  }
}
