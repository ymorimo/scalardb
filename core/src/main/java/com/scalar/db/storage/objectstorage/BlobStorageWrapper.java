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
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobStorageWrapper implements ObjectStorageWrapper {
  public static final String STORAGE_TYPE = "blob";

  private final BlobContainerClient client;
  private final long blockSize;
  private final int maxUploadConcurrency;
  private final long maxSingleUploadSize;
  private final int timeoutInSeconds;

  public BlobStorageWrapper(BlobContainerClient client, ObjectStorageConfig config) {
    this.client = client;
    this.blockSize = config.getBlockSize();
    this.maxUploadConcurrency = config.getMaxUploadConcurrency();
    this.maxSingleUploadSize = config.getMaxSingleUploadSize();
    this.timeoutInSeconds = config.getTimeoutInSeconds();
  }

  @Override
  public ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobDownloadContentResponse response =
          blobClient.downloadContentWithResponse(
              null, null, Duration.ofSeconds(timeoutInSeconds), null);
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

  @Override
  public Set<String> getKeys(String prefix) {
    return client
        .listBlobs(new ListBlobsOptions().setPrefix(prefix), Duration.ofSeconds(timeoutInSeconds))
        .stream()
        .map(BlobItem::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"))
              .setParallelTransferOptions(
                  new ParallelTransferOptions()
                      .setBlockSizeLong(blockSize)
                      .setMaxConcurrency(maxUploadConcurrency)
                      .setMaxSingleUploadSizeLong(maxSingleUploadSize));
      blobClient.uploadWithResponse(options, Duration.ofSeconds(timeoutInSeconds), null);
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_ALREADY_EXISTS)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
    }
  }

  @Override
  public void upsert(String key, String object) {
    BlobClient blobClient = client.getBlobClient(key);
    BlobParallelUploadOptions options =
        new BlobParallelUploadOptions(BinaryData.fromString(object))
            .setParallelTransferOptions(
                new ParallelTransferOptions()
                    .setBlockSizeLong(blockSize)
                    .setMaxConcurrency(maxUploadConcurrency)
                    .setMaxSingleUploadSizeLong(maxSingleUploadSize));
    blobClient.uploadWithResponse(options, Duration.ofSeconds(timeoutInSeconds), null);
  }

  @Override
  public boolean updateIfVersionMatches(String key, String object, String version) {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      BlobParallelUploadOptions options =
          new BlobParallelUploadOptions(BinaryData.fromString(object))
              .setRequestConditions(new BlobRequestConditions().setIfMatch(version))
              .setParallelTransferOptions(
                  new ParallelTransferOptions()
                      .setBlockSizeLong(blockSize)
                      .setMaxConcurrency(maxUploadConcurrency)
                      .setMaxSingleUploadSizeLong(maxSingleUploadSize));
      blobClient.uploadWithResponse(options, Duration.ofSeconds(timeoutInSeconds), null);
      return true;
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.CONDITION_NOT_MET)
          || e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.delete();
    } catch (BlobStorageException e) {
      if (e.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public boolean deleteIfVersionMatches(String key, String version) {
    try {
      BlobClient blobClient = client.getBlobClient(key);
      blobClient.deleteWithResponse(
          null,
          new BlobRequestConditions().setIfMatch(version),
          Duration.ofSeconds(timeoutInSeconds),
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

  @Override
  public void close() {
    // Do nothing
  }
}
