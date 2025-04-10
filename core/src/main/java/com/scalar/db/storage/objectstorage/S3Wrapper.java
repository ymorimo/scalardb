package com.scalar.db.storage.objectstorage;

import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Wrapper implements ObjectStorageWrapper {
  public static final String STORAGE_TYPE = "s3";
  private final S3Client client;
  private final String bucket;

  public S3Wrapper(S3Client client, String bucket) {
    this.client = client;
    this.bucket = bucket;
  }

  @Override
  public ObjectStorageWrapperResponse get(String key) throws ObjectStorageWrapperException {
    try {
      ResponseBytes<GetObjectResponse> response =
          client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build());
      String data = response.asUtf8String();
      String eTag = response.response().eTag();
      return new ObjectStorageWrapperResponse(data, eTag);
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public Set<String> getKeys(String prefix) {
    return client
        .listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
        .contents()
        .stream()
        .map(S3Object::key)
        .collect(Collectors.toSet());
  }

  @Override
  public void insert(String key, String object) throws ObjectStorageWrapperException {
    try {
      client.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch("*").build(),
          RequestBody.fromString(object));
    } catch (S3Exception e) {
      if (e.statusCode() == 409) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.CONFLICT, e);
      }
      if (e.statusCode() == 412) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS, e);
      }
      throw e;
    }
  }

  @Override
  public void upsert(String key, String object) {
    client.putObject(
        PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromString(object));
  }

  @Override
  public boolean updateIfVersionMatches(String key, String object, String version) {
    try {
      client.putObject(
          PutObjectRequest.builder().bucket(bucket).key(key).ifMatch(version).build(),
          RequestBody.fromString(object));
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404 || e.statusCode() == 409 || e.statusCode() == 412) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void delete(String key) throws ObjectStorageWrapperException {
    try {
      client.deleteObject(
          software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build());
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new ObjectStorageWrapperException(
            ObjectStorageWrapperException.StatusCode.NOT_FOUND, e);
      }
      throw e;
    }
  }

  @Override
  public boolean deleteIfVersionMatches(String key, String version) {
    try {
      client.deleteObject(
          software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .ifMatch(version)
              .build());
      return true;
    } catch (S3Exception e) {
      if (e.statusCode() == 404 || e.statusCode() == 409 || e.statusCode() == 412) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
