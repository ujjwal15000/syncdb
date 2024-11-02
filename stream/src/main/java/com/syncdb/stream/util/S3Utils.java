package com.syncdb.stream.util;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Slf4j
public class S3Utils {

  public static S3AsyncClient getClient(String region) {
    SdkAsyncHttpClient crtHttpClient =
        AwsCrtAsyncHttpClient.builder()
            .connectionMaxIdleTime(Duration.ofSeconds(20))
            .maxConcurrency(50)
            .build();
    return S3AsyncClient.builder()
        .forcePathStyle(true)
        .httpClient(crtHttpClient)
        .region(Region.of(region))
        .build();
  }

  public static Flowable<ByteBuffer> getS3ObjectFlowableStream(
      S3AsyncClient s3AsyncClient, String bucket, String key) {
    return Single.fromCompletionStage(
            s3AsyncClient.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                AsyncResponseTransformer.toPublisher()))
        .flatMapPublisher(Flowable::fromPublisher);
  }

  public static Single<InputStream> getS3ObjectInputStream(
          S3AsyncClient s3AsyncClient, String bucket, String key) {
    return Single.fromCompletionStage(
                    s3AsyncClient.getObject(
                            GetObjectRequest.builder().bucket(bucket).key(key).build(),
                            AsyncResponseTransformer.toBlockingInputStream()))
            .map(r -> r);
  }


  public static Single<byte[]> getS3Object(
      S3AsyncClient s3AsyncClient, String bucket, String key) {
    return Single.fromFuture(
            s3AsyncClient.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                AsyncResponseTransformer.toBytes()))
        .map(BytesWrapper::asByteArray);
  }

  public static Completable putS3Object(
      S3AsyncClient s3AsyncClient, String bucket, String key, byte[] object) {
    return Completable.fromFuture(
        s3AsyncClient.putObject(
            PutObjectRequest.builder().bucket(bucket).key(key).build(),
            AsyncRequestBody.fromBytes(object)));
  }

  public static Completable createBucket(S3AsyncClient s3AsyncClient, String bucket) {
    return Completable.fromFuture(
            s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()))
        .onErrorResumeNext(
            e -> {
              if (e instanceof ExecutionException
                  && e.getCause() instanceof BucketAlreadyExistsException)
                return Completable.complete();
              return Completable.error(e);
            });
  }
}
