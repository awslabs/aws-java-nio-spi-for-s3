/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateSessionRequest;
import software.amazon.awssdk.services.s3.model.CreateSessionResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletePublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.DeletePublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyStatusRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyStatusResponse;
import software.amazon.awssdk.services.s3.model.GetBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketRequestPaymentRequest;
import software.amazon.awssdk.services.s3.model.GetBucketRequestPaymentResponse;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTorrentRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTorrentResponse;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketAnalyticsConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketIntelligentTieringConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketIntelligentTieringConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketInventoryConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketInventoryConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketMetricsConfigurationsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketMetricsConfigurationsResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAccelerateConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAclResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAnalyticsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAnalyticsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.PutBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketInventoryConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketInventoryConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketMetricsConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketMetricsConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketRequestPaymentRequest;
import software.amazon.awssdk.services.s3.model.PutBucketRequestPaymentResponse;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectAclResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.RestoreObjectResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.WriteGetObjectResponseRequest;
import software.amazon.awssdk.services.s3.model.WriteGetObjectResponseResponse;
import software.amazon.awssdk.services.s3.paginators.ListDirectoryBucketsPublisher;
import software.amazon.awssdk.services.s3.paginators.ListMultipartUploadsPublisher;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsPublisher;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.services.s3.paginators.ListPartsPublisher;
import software.amazon.awssdk.services.s3.waiters.S3AsyncWaiter;

public class CacheableS3Client implements S3AsyncClient {

    private final S3AsyncClient client;
    private boolean closed = false;

    public CacheableS3Client(S3AsyncClient client) {
        this.client = client;
    }


    @Override
    public String serviceName() {
        return client.serviceName();
    }

    @Override
    public void close() {
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public S3AsyncClient getClient() {
        return client;
    }

    @Override
    public S3Utilities utilities() {
        return client.utilities();
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(
            AbortMultipartUploadRequest abortMultipartUploadRequest) {
        return client.abortMultipartUpload(abortMultipartUploadRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(
            Consumer<AbortMultipartUploadRequest.Builder> abortMultipartUploadRequest) {
        return client.abortMultipartUpload(abortMultipartUploadRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
            CompleteMultipartUploadRequest completeMultipartUploadRequest) {
        return client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
            Consumer<CompleteMultipartUploadRequest.Builder> completeMultipartUploadRequest) {
        return client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    @Override
    public CompletableFuture<CopyObjectResponse> copyObject(CopyObjectRequest copyObjectRequest) {
        return client.copyObject(copyObjectRequest);
    }

    @Override
    public CompletableFuture<CopyObjectResponse> copyObject(Consumer<CopyObjectRequest.Builder> copyObjectRequest) {
        return client.copyObject(copyObjectRequest);
    }

    @Override
    public CompletableFuture<CreateBucketResponse> createBucket(CreateBucketRequest createBucketRequest) {
        return client.createBucket(createBucketRequest);
    }

    @Override
    public CompletableFuture<CreateBucketResponse> createBucket(Consumer<CreateBucketRequest.Builder> createBucketRequest) {
        return client.createBucket(createBucketRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(
            CreateMultipartUploadRequest createMultipartUploadRequest) {
        return client.createMultipartUpload(createMultipartUploadRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(
            Consumer<CreateMultipartUploadRequest.Builder> createMultipartUploadRequest) {
        return client.createMultipartUpload(createMultipartUploadRequest);
    }

    @Override
    public CompletableFuture<CreateSessionResponse> createSession(CreateSessionRequest createSessionRequest) {
        return client.createSession(createSessionRequest);
    }

    @Override
    public CompletableFuture<CreateSessionResponse> createSession(Consumer<CreateSessionRequest.Builder> createSessionRequest) {
        return client.createSession(createSessionRequest);
    }

    @Override
    public CompletableFuture<DeleteBucketResponse> deleteBucket(DeleteBucketRequest deleteBucketRequest) {
        return client.deleteBucket(deleteBucketRequest);
    }

    @Override
    public CompletableFuture<DeleteBucketResponse> deleteBucket(Consumer<DeleteBucketRequest.Builder> deleteBucketRequest) {
        return client.deleteBucket(deleteBucketRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketAnalyticsConfigurationResponse> deleteBucketAnalyticsConfiguration(
            DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest) {
        return client.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketAnalyticsConfigurationResponse> deleteBucketAnalyticsConfiguration(
            Consumer<DeleteBucketAnalyticsConfigurationRequest.Builder> deleteBucketAnalyticsConfigurationRequest) {
        return client.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest);
    }

    @Override
    public CompletableFuture<DeleteBucketCorsResponse> deleteBucketCors(DeleteBucketCorsRequest deleteBucketCorsRequest) {
        return client.deleteBucketCors(deleteBucketCorsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketCorsResponse> deleteBucketCors(
            Consumer<DeleteBucketCorsRequest.Builder> deleteBucketCorsRequest) {
        return client.deleteBucketCors(deleteBucketCorsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketEncryptionResponse> deleteBucketEncryption(
            DeleteBucketEncryptionRequest deleteBucketEncryptionRequest) {
        return client.deleteBucketEncryption(deleteBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketEncryptionResponse> deleteBucketEncryption(
            Consumer<DeleteBucketEncryptionRequest.Builder> deleteBucketEncryptionRequest) {
        return client.deleteBucketEncryption(deleteBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketIntelligentTieringConfigurationResponse> deleteBucketIntelligentTieringConfiguration(
            DeleteBucketIntelligentTieringConfigurationRequest deleteBucketIntelligentTieringConfigurationRequest) {
        return client.deleteBucketIntelligentTieringConfiguration(deleteBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketIntelligentTieringConfigurationResponse> deleteBucketIntelligentTieringConfiguration(
            Consumer<DeleteBucketIntelligentTieringConfigurationRequest.Builder>
                    deleteBucketIntelligentTieringConfigurationRequest) {
        return client.deleteBucketIntelligentTieringConfiguration(deleteBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketInventoryConfigurationResponse> deleteBucketInventoryConfiguration(
            DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest) {
        return client.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketInventoryConfigurationResponse> deleteBucketInventoryConfiguration(
            Consumer<DeleteBucketInventoryConfigurationRequest.Builder> deleteBucketInventoryConfigurationRequest) {
        return client.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketLifecycleResponse> deleteBucketLifecycle(
            DeleteBucketLifecycleRequest deleteBucketLifecycleRequest) {
        return client.deleteBucketLifecycle(deleteBucketLifecycleRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketLifecycleResponse> deleteBucketLifecycle(
            Consumer<DeleteBucketLifecycleRequest.Builder> deleteBucketLifecycleRequest) {
        return client.deleteBucketLifecycle(deleteBucketLifecycleRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketMetricsConfigurationResponse> deleteBucketMetricsConfiguration(
            DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest) {
        return client.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketMetricsConfigurationResponse> deleteBucketMetricsConfiguration(
            Consumer<DeleteBucketMetricsConfigurationRequest.Builder> deleteBucketMetricsConfigurationRequest) {
        return client.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketOwnershipControlsResponse> deleteBucketOwnershipControls(
            DeleteBucketOwnershipControlsRequest deleteBucketOwnershipControlsRequest) {
        return client.deleteBucketOwnershipControls(deleteBucketOwnershipControlsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketOwnershipControlsResponse> deleteBucketOwnershipControls(
            Consumer<DeleteBucketOwnershipControlsRequest.Builder> deleteBucketOwnershipControlsRequest) {
        return client.deleteBucketOwnershipControls(deleteBucketOwnershipControlsRequest);
    }

    @Override
    public CompletableFuture<DeleteBucketPolicyResponse> deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) {
        return client.deleteBucketPolicy(deleteBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketPolicyResponse> deleteBucketPolicy(
            Consumer<DeleteBucketPolicyRequest.Builder> deleteBucketPolicyRequest) {
        return client.deleteBucketPolicy(deleteBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketReplicationResponse> deleteBucketReplication(
            DeleteBucketReplicationRequest deleteBucketReplicationRequest) {
        return client.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketReplicationResponse> deleteBucketReplication(
            Consumer<DeleteBucketReplicationRequest.Builder> deleteBucketReplicationRequest) {
        return client.deleteBucketReplication(deleteBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketTaggingResponse> deleteBucketTagging(
            DeleteBucketTaggingRequest deleteBucketTaggingRequest) {
        return client.deleteBucketTagging(deleteBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketTaggingResponse> deleteBucketTagging(
            Consumer<DeleteBucketTaggingRequest.Builder> deleteBucketTaggingRequest) {
        return client.deleteBucketTagging(deleteBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketWebsiteResponse> deleteBucketWebsite(
            DeleteBucketWebsiteRequest deleteBucketWebsiteRequest) {
        return client.deleteBucketWebsite(deleteBucketWebsiteRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteBucketWebsiteResponse> deleteBucketWebsite(
            Consumer<DeleteBucketWebsiteRequest.Builder> deleteBucketWebsiteRequest) {
        return client.deleteBucketWebsite(deleteBucketWebsiteRequest);
    }

    @Override
    public CompletableFuture<DeleteObjectResponse> deleteObject(DeleteObjectRequest deleteObjectRequest) {
        return client.deleteObject(deleteObjectRequest);
    }

    @Override
    public CompletableFuture<DeleteObjectResponse> deleteObject(Consumer<DeleteObjectRequest.Builder> deleteObjectRequest) {
        return client.deleteObject(deleteObjectRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteObjectTaggingResponse> deleteObjectTagging(
            DeleteObjectTaggingRequest deleteObjectTaggingRequest) {
        return client.deleteObjectTagging(deleteObjectTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeleteObjectTaggingResponse> deleteObjectTagging(
            Consumer<DeleteObjectTaggingRequest.Builder> deleteObjectTaggingRequest) {
        return client.deleteObjectTagging(deleteObjectTaggingRequest);
    }

    @Override
    public CompletableFuture<DeleteObjectsResponse> deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
        return client.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public CompletableFuture<DeleteObjectsResponse> deleteObjects(Consumer<DeleteObjectsRequest.Builder> deleteObjectsRequest) {
        return client.deleteObjects(deleteObjectsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeletePublicAccessBlockResponse> deletePublicAccessBlock(
            DeletePublicAccessBlockRequest deletePublicAccessBlockRequest) {
        return client.deletePublicAccessBlock(deletePublicAccessBlockRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<DeletePublicAccessBlockResponse> deletePublicAccessBlock(
            Consumer<DeletePublicAccessBlockRequest.Builder> deletePublicAccessBlockRequest) {
        return client.deletePublicAccessBlock(deletePublicAccessBlockRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketAccelerateConfigurationResponse> getBucketAccelerateConfiguration(
            GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest) {
        return client.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketAccelerateConfigurationResponse> getBucketAccelerateConfiguration(
            Consumer<GetBucketAccelerateConfigurationRequest.Builder> getBucketAccelerateConfigurationRequest) {
        return client.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest);
    }

    @Override
    public CompletableFuture<GetBucketAclResponse> getBucketAcl(GetBucketAclRequest getBucketAclRequest) {
        return client.getBucketAcl(getBucketAclRequest);
    }

    @Override
    public CompletableFuture<GetBucketAclResponse> getBucketAcl(Consumer<GetBucketAclRequest.Builder> getBucketAclRequest) {
        return client.getBucketAcl(getBucketAclRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketAnalyticsConfigurationResponse> getBucketAnalyticsConfiguration(
            GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest) {
        return client.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketAnalyticsConfigurationResponse> getBucketAnalyticsConfiguration(
            Consumer<GetBucketAnalyticsConfigurationRequest.Builder> getBucketAnalyticsConfigurationRequest) {
        return client.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest);
    }

    @Override
    public CompletableFuture<GetBucketCorsResponse> getBucketCors(GetBucketCorsRequest getBucketCorsRequest) {
        return client.getBucketCors(getBucketCorsRequest);
    }

    @Override
    public CompletableFuture<GetBucketCorsResponse> getBucketCors(Consumer<GetBucketCorsRequest.Builder> getBucketCorsRequest) {
        return client.getBucketCors(getBucketCorsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketEncryptionResponse> getBucketEncryption(
            GetBucketEncryptionRequest getBucketEncryptionRequest) {
        return client.getBucketEncryption(getBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketEncryptionResponse> getBucketEncryption(
            Consumer<GetBucketEncryptionRequest.Builder> getBucketEncryptionRequest) {
        return client.getBucketEncryption(getBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketIntelligentTieringConfigurationResponse> getBucketIntelligentTieringConfiguration(
            GetBucketIntelligentTieringConfigurationRequest getBucketIntelligentTieringConfigurationRequest) {
        return client.getBucketIntelligentTieringConfiguration(getBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketIntelligentTieringConfigurationResponse> getBucketIntelligentTieringConfiguration(
            Consumer<GetBucketIntelligentTieringConfigurationRequest.Builder> getBucketIntelligentTieringConfigurationRequest) {
        return client.getBucketIntelligentTieringConfiguration(getBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketInventoryConfigurationResponse> getBucketInventoryConfiguration(
            GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest) {
        return client.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketInventoryConfigurationResponse> getBucketInventoryConfiguration(
            Consumer<GetBucketInventoryConfigurationRequest.Builder> getBucketInventoryConfigurationRequest) {
        return client.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketLifecycleConfigurationResponse> getBucketLifecycleConfiguration(
            GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
        return client.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketLifecycleConfigurationResponse> getBucketLifecycleConfiguration(
            Consumer<GetBucketLifecycleConfigurationRequest.Builder> getBucketLifecycleConfigurationRequest) {
        return client.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest);
    }

    @Override
    public CompletableFuture<GetBucketLocationResponse> getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) {
        return client.getBucketLocation(getBucketLocationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketLocationResponse> getBucketLocation(
            Consumer<GetBucketLocationRequest.Builder> getBucketLocationRequest) {
        return client.getBucketLocation(getBucketLocationRequest);
    }

    @Override
    public CompletableFuture<GetBucketLoggingResponse> getBucketLogging(
            GetBucketLoggingRequest getBucketLoggingRequest) {
        return client.getBucketLogging(getBucketLoggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketLoggingResponse> getBucketLogging(
            Consumer<GetBucketLoggingRequest.Builder> getBucketLoggingRequest) {
        return client.getBucketLogging(getBucketLoggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketMetricsConfigurationResponse> getBucketMetricsConfiguration(
            GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest) {
        return client.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketMetricsConfigurationResponse> getBucketMetricsConfiguration(
            Consumer<GetBucketMetricsConfigurationRequest.Builder> getBucketMetricsConfigurationRequest) {
        return client.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketNotificationConfigurationResponse> getBucketNotificationConfiguration(
            GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest) {
        return client.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketNotificationConfigurationResponse> getBucketNotificationConfiguration(
            Consumer<GetBucketNotificationConfigurationRequest.Builder> getBucketNotificationConfigurationRequest) {
        return client.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketOwnershipControlsResponse> getBucketOwnershipControls(
            GetBucketOwnershipControlsRequest getBucketOwnershipControlsRequest) {
        return client.getBucketOwnershipControls(getBucketOwnershipControlsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketOwnershipControlsResponse> getBucketOwnershipControls(
            Consumer<GetBucketOwnershipControlsRequest.Builder> getBucketOwnershipControlsRequest) {
        return client.getBucketOwnershipControls(getBucketOwnershipControlsRequest);
    }

    @Override
    public CompletableFuture<GetBucketPolicyResponse> getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) {
        return client.getBucketPolicy(getBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketPolicyResponse> getBucketPolicy(
            Consumer<GetBucketPolicyRequest.Builder> getBucketPolicyRequest) {
        return client.getBucketPolicy(getBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketPolicyStatusResponse> getBucketPolicyStatus(
            GetBucketPolicyStatusRequest getBucketPolicyStatusRequest) {
        return client.getBucketPolicyStatus(getBucketPolicyStatusRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketPolicyStatusResponse> getBucketPolicyStatus(
            Consumer<GetBucketPolicyStatusRequest.Builder> getBucketPolicyStatusRequest) {
        return client.getBucketPolicyStatus(getBucketPolicyStatusRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketReplicationResponse> getBucketReplication(
            GetBucketReplicationRequest getBucketReplicationRequest) {
        return client.getBucketReplication(getBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketReplicationResponse> getBucketReplication(
            Consumer<GetBucketReplicationRequest.Builder> getBucketReplicationRequest) {
        return client.getBucketReplication(getBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketRequestPaymentResponse> getBucketRequestPayment(
            GetBucketRequestPaymentRequest getBucketRequestPaymentRequest) {
        return client.getBucketRequestPayment(getBucketRequestPaymentRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketRequestPaymentResponse> getBucketRequestPayment(
            Consumer<GetBucketRequestPaymentRequest.Builder> getBucketRequestPaymentRequest) {
        return client.getBucketRequestPayment(getBucketRequestPaymentRequest);
    }

    @Override
    public CompletableFuture<GetBucketTaggingResponse> getBucketTagging(GetBucketTaggingRequest getBucketTaggingRequest) {
        return client.getBucketTagging(getBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketTaggingResponse> getBucketTagging(
            Consumer<GetBucketTaggingRequest.Builder> getBucketTaggingRequest) {
        return client.getBucketTagging(getBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketVersioningResponse> getBucketVersioning(
            GetBucketVersioningRequest getBucketVersioningRequest) {
        return client.getBucketVersioning(getBucketVersioningRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketVersioningResponse> getBucketVersioning(
            Consumer<GetBucketVersioningRequest.Builder> getBucketVersioningRequest) {
        return client.getBucketVersioning(getBucketVersioningRequest);
    }

    @Override
    public CompletableFuture<GetBucketWebsiteResponse> getBucketWebsite(GetBucketWebsiteRequest getBucketWebsiteRequest) {
        return client.getBucketWebsite(getBucketWebsiteRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetBucketWebsiteResponse> getBucketWebsite(
            Consumer<GetBucketWebsiteRequest.Builder> getBucketWebsiteRequest) {
        return client.getBucketWebsite(getBucketWebsiteRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public <ReturnT> CompletableFuture<ReturnT> getObject(GetObjectRequest getObjectRequest,
                                              AsyncResponseTransformer<GetObjectResponse, ReturnT> asyncResponseTransformer) {
        return client.getObject(getObjectRequest, asyncResponseTransformer);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public <ReturnT> CompletableFuture<ReturnT> getObject(Consumer<GetObjectRequest.Builder> getObjectRequest,
                                              AsyncResponseTransformer<GetObjectResponse, ReturnT> asyncResponseTransformer) {
        return client.getObject(getObjectRequest, asyncResponseTransformer);
    }

    @Override
    public CompletableFuture<GetObjectResponse> getObject(GetObjectRequest getObjectRequest, Path destinationPath) {
        return client.getObject(getObjectRequest, destinationPath);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectResponse> getObject(Consumer<GetObjectRequest.Builder> getObjectRequest,
                                                          Path destinationPath) {
        return client.getObject(getObjectRequest, destinationPath);
    }

    @Override
    public CompletableFuture<GetObjectAclResponse> getObjectAcl(GetObjectAclRequest getObjectAclRequest) {
        return client.getObjectAcl(getObjectAclRequest);
    }

    @Override
    public CompletableFuture<GetObjectAclResponse> getObjectAcl(Consumer<GetObjectAclRequest.Builder> getObjectAclRequest) {
        return client.getObjectAcl(getObjectAclRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectAttributesResponse> getObjectAttributes(
            GetObjectAttributesRequest getObjectAttributesRequest) {
        return client.getObjectAttributes(getObjectAttributesRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectAttributesResponse> getObjectAttributes(
            Consumer<GetObjectAttributesRequest.Builder> getObjectAttributesRequest) {
        return client.getObjectAttributes(getObjectAttributesRequest);
    }

    @Override
    public CompletableFuture<GetObjectLegalHoldResponse> getObjectLegalHold(GetObjectLegalHoldRequest getObjectLegalHoldRequest) {
        return client.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectLegalHoldResponse> getObjectLegalHold(
            Consumer<GetObjectLegalHoldRequest.Builder> getObjectLegalHoldRequest) {
        return client.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectLockConfigurationResponse> getObjectLockConfiguration(
            GetObjectLockConfigurationRequest getObjectLockConfigurationRequest) {
        return client.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectLockConfigurationResponse> getObjectLockConfiguration(
            Consumer<GetObjectLockConfigurationRequest.Builder> getObjectLockConfigurationRequest) {
        return client.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @Override
    public CompletableFuture<GetObjectRetentionResponse> getObjectRetention(GetObjectRetentionRequest getObjectRetentionRequest) {
        return client.getObjectRetention(getObjectRetentionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectRetentionResponse> getObjectRetention(
            Consumer<GetObjectRetentionRequest.Builder> getObjectRetentionRequest) {
        return client.getObjectRetention(getObjectRetentionRequest);
    }

    @Override
    public CompletableFuture<GetObjectTaggingResponse> getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest) {
        return client.getObjectTagging(getObjectTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectTaggingResponse> getObjectTagging(
            Consumer<GetObjectTaggingRequest.Builder> getObjectTaggingRequest) {
        return client.getObjectTagging(getObjectTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public <ReturnT> CompletableFuture<ReturnT> getObjectTorrent(
            GetObjectTorrentRequest getObjectTorrentRequest,
            AsyncResponseTransformer<GetObjectTorrentResponse, ReturnT> asyncResponseTransformer) {
        return client.getObjectTorrent(getObjectTorrentRequest, asyncResponseTransformer);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public <ReturnT> CompletableFuture<ReturnT> getObjectTorrent(
            Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest,
            AsyncResponseTransformer<GetObjectTorrentResponse, ReturnT> asyncResponseTransformer) {
        return client.getObjectTorrent(getObjectTorrentRequest, asyncResponseTransformer);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectTorrentResponse> getObjectTorrent(GetObjectTorrentRequest getObjectTorrentRequest,
                                                                        Path destinationPath) {
        return client.getObjectTorrent(getObjectTorrentRequest, destinationPath);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetObjectTorrentResponse> getObjectTorrent(
            Consumer<GetObjectTorrentRequest.Builder> getObjectTorrentRequest, Path destinationPath) {
        return client.getObjectTorrent(getObjectTorrentRequest, destinationPath);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetPublicAccessBlockResponse> getPublicAccessBlock(
            GetPublicAccessBlockRequest getPublicAccessBlockRequest) {
        return client.getPublicAccessBlock(getPublicAccessBlockRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<GetPublicAccessBlockResponse> getPublicAccessBlock(
            Consumer<GetPublicAccessBlockRequest.Builder> getPublicAccessBlockRequest) {
        return client.getPublicAccessBlock(getPublicAccessBlockRequest);
    }

    @Override
    public CompletableFuture<HeadBucketResponse> headBucket(HeadBucketRequest headBucketRequest) {
        return client.headBucket(headBucketRequest);
    }

    @Override
    public CompletableFuture<HeadBucketResponse> headBucket(Consumer<HeadBucketRequest.Builder> headBucketRequest) {
        return client.headBucket(headBucketRequest);
    }

    @Override
    public CompletableFuture<HeadObjectResponse> headObject(HeadObjectRequest headObjectRequest) {
        return client.headObject(headObjectRequest);
    }

    @Override
    public CompletableFuture<HeadObjectResponse> headObject(Consumer<HeadObjectRequest.Builder> headObjectRequest) {
        return client.headObject(headObjectRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketAnalyticsConfigurationsResponse> listBucketAnalyticsConfigurations(
            ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest) {
        return client.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketAnalyticsConfigurationsResponse> listBucketAnalyticsConfigurations(
            Consumer<ListBucketAnalyticsConfigurationsRequest.Builder> listBucketAnalyticsConfigurationsRequest) {
        return client.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketIntelligentTieringConfigurationsResponse> listBucketIntelligentTieringConfigurations(
            ListBucketIntelligentTieringConfigurationsRequest listBucketIntelligentTieringConfigurationsRequest) {
        return client.listBucketIntelligentTieringConfigurations(listBucketIntelligentTieringConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketIntelligentTieringConfigurationsResponse> listBucketIntelligentTieringConfigurations(
            Consumer<ListBucketIntelligentTieringConfigurationsRequest.Builder>
                    listBucketIntelligentTieringConfigurationsRequest) {
        return client.listBucketIntelligentTieringConfigurations(listBucketIntelligentTieringConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketInventoryConfigurationsResponse> listBucketInventoryConfigurations(
            ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest) {
        return client.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketInventoryConfigurationsResponse> listBucketInventoryConfigurations(
            Consumer<ListBucketInventoryConfigurationsRequest.Builder> listBucketInventoryConfigurationsRequest) {
        return client.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketMetricsConfigurationsResponse> listBucketMetricsConfigurations(
            ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest) {
        return client.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListBucketMetricsConfigurationsResponse> listBucketMetricsConfigurations(
            Consumer<ListBucketMetricsConfigurationsRequest.Builder> listBucketMetricsConfigurationsRequest) {
        return client.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest);
    }

    @Override
    public CompletableFuture<ListBucketsResponse> listBuckets(ListBucketsRequest listBucketsRequest) {
        return client.listBuckets(listBucketsRequest);
    }

    @Override
    public CompletableFuture<ListBucketsResponse> listBuckets(Consumer<ListBucketsRequest.Builder> listBucketsRequest) {
        return client.listBuckets(listBucketsRequest);
    }

    @Override
    public CompletableFuture<ListBucketsResponse> listBuckets() {
        return client.listBuckets();
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListDirectoryBucketsResponse> listDirectoryBuckets(
            ListDirectoryBucketsRequest listDirectoryBucketsRequest) {
        return client.listDirectoryBuckets(listDirectoryBucketsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListDirectoryBucketsResponse> listDirectoryBuckets(
            Consumer<ListDirectoryBucketsRequest.Builder> listDirectoryBucketsRequest) {
        return client.listDirectoryBuckets(listDirectoryBucketsRequest);
    }

    @Override
    public ListDirectoryBucketsPublisher listDirectoryBucketsPaginator(ListDirectoryBucketsRequest listDirectoryBucketsRequest) {
        return client.listDirectoryBucketsPaginator(listDirectoryBucketsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public ListDirectoryBucketsPublisher listDirectoryBucketsPaginator(
            Consumer<ListDirectoryBucketsRequest.Builder> listDirectoryBucketsRequest) {
        return client.listDirectoryBucketsPaginator(listDirectoryBucketsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListMultipartUploadsResponse> listMultipartUploads(
            ListMultipartUploadsRequest listMultipartUploadsRequest) {
        return client.listMultipartUploads(listMultipartUploadsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListMultipartUploadsResponse> listMultipartUploads(
            Consumer<ListMultipartUploadsRequest.Builder> listMultipartUploadsRequest) {
        return client.listMultipartUploads(listMultipartUploadsRequest);
    }

    @Override
    public ListMultipartUploadsPublisher listMultipartUploadsPaginator(ListMultipartUploadsRequest listMultipartUploadsRequest) {
        return client.listMultipartUploadsPaginator(listMultipartUploadsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public ListMultipartUploadsPublisher listMultipartUploadsPaginator(
            Consumer<ListMultipartUploadsRequest.Builder> listMultipartUploadsRequest) {
        return client.listMultipartUploadsPaginator(listMultipartUploadsRequest);
    }

    @Override
    public CompletableFuture<ListObjectVersionsResponse> listObjectVersions(ListObjectVersionsRequest listObjectVersionsRequest) {
        return client.listObjectVersions(listObjectVersionsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<ListObjectVersionsResponse> listObjectVersions(
            Consumer<ListObjectVersionsRequest.Builder> listObjectVersionsRequest) {
        return client.listObjectVersions(listObjectVersionsRequest);
    }

    @Override
    public ListObjectVersionsPublisher listObjectVersionsPaginator(ListObjectVersionsRequest listObjectVersionsRequest) {
        return client.listObjectVersionsPaginator(listObjectVersionsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public ListObjectVersionsPublisher listObjectVersionsPaginator(
            Consumer<ListObjectVersionsRequest.Builder> listObjectVersionsRequest) {
        return client.listObjectVersionsPaginator(listObjectVersionsRequest);
    }

    @Override
    public CompletableFuture<ListObjectsResponse> listObjects(ListObjectsRequest listObjectsRequest) {
        return client.listObjects(listObjectsRequest);
    }

    @Override
    public CompletableFuture<ListObjectsResponse> listObjects(Consumer<ListObjectsRequest.Builder> listObjectsRequest) {
        return client.listObjects(listObjectsRequest);
    }

    @Override
    public CompletableFuture<ListObjectsV2Response> listObjectsV2(ListObjectsV2Request listObjectsV2Request) {
        return client.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public CompletableFuture<ListObjectsV2Response> listObjectsV2(Consumer<ListObjectsV2Request.Builder> listObjectsV2Request) {
        return client.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public ListObjectsV2Publisher listObjectsV2Paginator(ListObjectsV2Request listObjectsV2Request) {
        return client.listObjectsV2Paginator(listObjectsV2Request);
    }

    @Override
    public ListObjectsV2Publisher listObjectsV2Paginator(Consumer<ListObjectsV2Request.Builder> listObjectsV2Request) {
        return client.listObjectsV2Paginator(listObjectsV2Request);
    }

    @Override
    public CompletableFuture<ListPartsResponse> listParts(ListPartsRequest listPartsRequest) {
        return client.listParts(listPartsRequest);
    }

    @Override
    public CompletableFuture<ListPartsResponse> listParts(Consumer<ListPartsRequest.Builder> listPartsRequest) {
        return client.listParts(listPartsRequest);
    }

    @Override
    public ListPartsPublisher listPartsPaginator(ListPartsRequest listPartsRequest) {
        return client.listPartsPaginator(listPartsRequest);
    }

    @Override
    public ListPartsPublisher listPartsPaginator(Consumer<ListPartsRequest.Builder> listPartsRequest) {
        return client.listPartsPaginator(listPartsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketAccelerateConfigurationResponse> putBucketAccelerateConfiguration(
            PutBucketAccelerateConfigurationRequest putBucketAccelerateConfigurationRequest) {
        return client.putBucketAccelerateConfiguration(putBucketAccelerateConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketAccelerateConfigurationResponse> putBucketAccelerateConfiguration(
            Consumer<PutBucketAccelerateConfigurationRequest.Builder> putBucketAccelerateConfigurationRequest) {
        return client.putBucketAccelerateConfiguration(putBucketAccelerateConfigurationRequest);
    }

    @Override
    public CompletableFuture<PutBucketAclResponse> putBucketAcl(PutBucketAclRequest putBucketAclRequest) {
        return client.putBucketAcl(putBucketAclRequest);
    }

    @Override
    public CompletableFuture<PutBucketAclResponse> putBucketAcl(Consumer<PutBucketAclRequest.Builder> putBucketAclRequest) {
        return client.putBucketAcl(putBucketAclRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketAnalyticsConfigurationResponse> putBucketAnalyticsConfiguration(
            PutBucketAnalyticsConfigurationRequest putBucketAnalyticsConfigurationRequest) {
        return client.putBucketAnalyticsConfiguration(putBucketAnalyticsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketAnalyticsConfigurationResponse> putBucketAnalyticsConfiguration(
            Consumer<PutBucketAnalyticsConfigurationRequest.Builder> putBucketAnalyticsConfigurationRequest) {
        return client.putBucketAnalyticsConfiguration(putBucketAnalyticsConfigurationRequest);
    }

    @Override
    public CompletableFuture<PutBucketCorsResponse> putBucketCors(PutBucketCorsRequest putBucketCorsRequest) {
        return client.putBucketCors(putBucketCorsRequest);
    }

    @Override
    public CompletableFuture<PutBucketCorsResponse> putBucketCors(Consumer<PutBucketCorsRequest.Builder> putBucketCorsRequest) {
        return client.putBucketCors(putBucketCorsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketEncryptionResponse> putBucketEncryption(
            PutBucketEncryptionRequest putBucketEncryptionRequest) {
        return client.putBucketEncryption(putBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketEncryptionResponse> putBucketEncryption(
            Consumer<PutBucketEncryptionRequest.Builder> putBucketEncryptionRequest) {
        return client.putBucketEncryption(putBucketEncryptionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketIntelligentTieringConfigurationResponse> putBucketIntelligentTieringConfiguration(
            PutBucketIntelligentTieringConfigurationRequest putBucketIntelligentTieringConfigurationRequest) {
        return client.putBucketIntelligentTieringConfiguration(putBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketIntelligentTieringConfigurationResponse> putBucketIntelligentTieringConfiguration(
            Consumer<PutBucketIntelligentTieringConfigurationRequest.Builder> putBucketIntelligentTieringConfigurationRequest) {
        return client.putBucketIntelligentTieringConfiguration(putBucketIntelligentTieringConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketInventoryConfigurationResponse> putBucketInventoryConfiguration(
            PutBucketInventoryConfigurationRequest putBucketInventoryConfigurationRequest) {
        return client.putBucketInventoryConfiguration(putBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketInventoryConfigurationResponse> putBucketInventoryConfiguration(
            Consumer<PutBucketInventoryConfigurationRequest.Builder> putBucketInventoryConfigurationRequest) {
        return client.putBucketInventoryConfiguration(putBucketInventoryConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketLifecycleConfigurationResponse> putBucketLifecycleConfiguration(
            PutBucketLifecycleConfigurationRequest putBucketLifecycleConfigurationRequest) {
        return client.putBucketLifecycleConfiguration(putBucketLifecycleConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketLifecycleConfigurationResponse> putBucketLifecycleConfiguration(
            Consumer<PutBucketLifecycleConfigurationRequest.Builder> putBucketLifecycleConfigurationRequest) {
        return client.putBucketLifecycleConfiguration(putBucketLifecycleConfigurationRequest);
    }

    @Override
    public CompletableFuture<PutBucketLoggingResponse> putBucketLogging(PutBucketLoggingRequest putBucketLoggingRequest) {
        return client.putBucketLogging(putBucketLoggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketLoggingResponse> putBucketLogging(
            Consumer<PutBucketLoggingRequest.Builder> putBucketLoggingRequest) {
        return client.putBucketLogging(putBucketLoggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketMetricsConfigurationResponse> putBucketMetricsConfiguration(
            PutBucketMetricsConfigurationRequest putBucketMetricsConfigurationRequest) {
        return client.putBucketMetricsConfiguration(putBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketMetricsConfigurationResponse> putBucketMetricsConfiguration(
            Consumer<PutBucketMetricsConfigurationRequest.Builder> putBucketMetricsConfigurationRequest) {
        return client.putBucketMetricsConfiguration(putBucketMetricsConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketNotificationConfigurationResponse> putBucketNotificationConfiguration(
            PutBucketNotificationConfigurationRequest putBucketNotificationConfigurationRequest) {
        return client.putBucketNotificationConfiguration(putBucketNotificationConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketNotificationConfigurationResponse> putBucketNotificationConfiguration(
            Consumer<PutBucketNotificationConfigurationRequest.Builder> putBucketNotificationConfigurationRequest) {
        return client.putBucketNotificationConfiguration(putBucketNotificationConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketOwnershipControlsResponse> putBucketOwnershipControls(
            PutBucketOwnershipControlsRequest putBucketOwnershipControlsRequest) {
        return client.putBucketOwnershipControls(putBucketOwnershipControlsRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketOwnershipControlsResponse> putBucketOwnershipControls(
            Consumer<PutBucketOwnershipControlsRequest.Builder> putBucketOwnershipControlsRequest) {
        return client.putBucketOwnershipControls(putBucketOwnershipControlsRequest);
    }

    @Override
    public CompletableFuture<PutBucketPolicyResponse> putBucketPolicy(PutBucketPolicyRequest putBucketPolicyRequest) {
        return client.putBucketPolicy(putBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketPolicyResponse> putBucketPolicy(
            Consumer<PutBucketPolicyRequest.Builder> putBucketPolicyRequest) {
        return client.putBucketPolicy(putBucketPolicyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketReplicationResponse> putBucketReplication(
            PutBucketReplicationRequest putBucketReplicationRequest) {
        return client.putBucketReplication(putBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketReplicationResponse> putBucketReplication(
            Consumer<PutBucketReplicationRequest.Builder> putBucketReplicationRequest) {
        return client.putBucketReplication(putBucketReplicationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketRequestPaymentResponse> putBucketRequestPayment(
            PutBucketRequestPaymentRequest putBucketRequestPaymentRequest) {
        return client.putBucketRequestPayment(putBucketRequestPaymentRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketRequestPaymentResponse> putBucketRequestPayment(
            Consumer<PutBucketRequestPaymentRequest.Builder> putBucketRequestPaymentRequest) {
        return client.putBucketRequestPayment(putBucketRequestPaymentRequest);
    }

    @Override
    public CompletableFuture<PutBucketTaggingResponse> putBucketTagging(PutBucketTaggingRequest putBucketTaggingRequest) {
        return client.putBucketTagging(putBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketTaggingResponse> putBucketTagging(
            Consumer<PutBucketTaggingRequest.Builder> putBucketTaggingRequest) {
        return client.putBucketTagging(putBucketTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketVersioningResponse> putBucketVersioning(
            PutBucketVersioningRequest putBucketVersioningRequest) {
        return client.putBucketVersioning(putBucketVersioningRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketVersioningResponse> putBucketVersioning(
            Consumer<PutBucketVersioningRequest.Builder> putBucketVersioningRequest) {
        return client.putBucketVersioning(putBucketVersioningRequest);
    }

    @Override
    public CompletableFuture<PutBucketWebsiteResponse> putBucketWebsite(PutBucketWebsiteRequest putBucketWebsiteRequest) {
        return client.putBucketWebsite(putBucketWebsiteRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutBucketWebsiteResponse> putBucketWebsite(
            Consumer<PutBucketWebsiteRequest.Builder> putBucketWebsiteRequest) {
        return client.putBucketWebsite(putBucketWebsiteRequest);
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(PutObjectRequest putObjectRequest, AsyncRequestBody requestBody) {
        return client.putObject(putObjectRequest, requestBody);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectResponse> putObject(Consumer<PutObjectRequest.Builder> putObjectRequest,
                                                          AsyncRequestBody requestBody) {
        return client.putObject(putObjectRequest, requestBody);
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(PutObjectRequest putObjectRequest, Path sourcePath) {
        return client.putObject(putObjectRequest, sourcePath);
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(Consumer<PutObjectRequest.Builder> putObjectRequest, Path sourcePath) {
        return client.putObject(putObjectRequest, sourcePath);
    }

    @Override
    public CompletableFuture<PutObjectAclResponse> putObjectAcl(PutObjectAclRequest putObjectAclRequest) {
        return client.putObjectAcl(putObjectAclRequest);
    }

    @Override
    public CompletableFuture<PutObjectAclResponse> putObjectAcl(Consumer<PutObjectAclRequest.Builder> putObjectAclRequest) {
        return client.putObjectAcl(putObjectAclRequest);
    }

    @Override
    public CompletableFuture<PutObjectLegalHoldResponse> putObjectLegalHold(PutObjectLegalHoldRequest putObjectLegalHoldRequest) {
        return client.putObjectLegalHold(putObjectLegalHoldRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectLegalHoldResponse> putObjectLegalHold(
            Consumer<PutObjectLegalHoldRequest.Builder> putObjectLegalHoldRequest) {
        return client.putObjectLegalHold(putObjectLegalHoldRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectLockConfigurationResponse> putObjectLockConfiguration(
            PutObjectLockConfigurationRequest putObjectLockConfigurationRequest) {
        return client.putObjectLockConfiguration(putObjectLockConfigurationRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectLockConfigurationResponse> putObjectLockConfiguration(
            Consumer<PutObjectLockConfigurationRequest.Builder> putObjectLockConfigurationRequest) {
        return client.putObjectLockConfiguration(putObjectLockConfigurationRequest);
    }

    @Override
    public CompletableFuture<PutObjectRetentionResponse> putObjectRetention(PutObjectRetentionRequest putObjectRetentionRequest) {
        return client.putObjectRetention(putObjectRetentionRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectRetentionResponse> putObjectRetention(
            Consumer<PutObjectRetentionRequest.Builder> putObjectRetentionRequest) {
        return client.putObjectRetention(putObjectRetentionRequest);
    }

    @Override
    public CompletableFuture<PutObjectTaggingResponse> putObjectTagging(PutObjectTaggingRequest putObjectTaggingRequest) {
        return client.putObjectTagging(putObjectTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutObjectTaggingResponse> putObjectTagging(
            Consumer<PutObjectTaggingRequest.Builder> putObjectTaggingRequest) {
        return client.putObjectTagging(putObjectTaggingRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutPublicAccessBlockResponse> putPublicAccessBlock(
            PutPublicAccessBlockRequest putPublicAccessBlockRequest) {
        return client.putPublicAccessBlock(putPublicAccessBlockRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<PutPublicAccessBlockResponse> putPublicAccessBlock(
            Consumer<PutPublicAccessBlockRequest.Builder> putPublicAccessBlockRequest) {
        return client.putPublicAccessBlock(putPublicAccessBlockRequest);
    }

    @Override
    public CompletableFuture<RestoreObjectResponse> restoreObject(RestoreObjectRequest restoreObjectRequest) {
        return client.restoreObject(restoreObjectRequest);
    }

    @Override
    public CompletableFuture<RestoreObjectResponse> restoreObject(Consumer<RestoreObjectRequest.Builder> restoreObjectRequest) {
        return client.restoreObject(restoreObjectRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<Void> selectObjectContent(SelectObjectContentRequest selectObjectContentRequest,
                                                       SelectObjectContentResponseHandler asyncResponseHandler) {
        return client.selectObjectContent(selectObjectContentRequest, asyncResponseHandler);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<Void> selectObjectContent(Consumer<SelectObjectContentRequest.Builder> selectObjectContentRequest,
                                                       SelectObjectContentResponseHandler asyncResponseHandler) {
        return client.selectObjectContent(selectObjectContentRequest, asyncResponseHandler);
    }

    @Override
    public CompletableFuture<UploadPartResponse> uploadPart(UploadPartRequest uploadPartRequest, AsyncRequestBody requestBody) {
        return client.uploadPart(uploadPartRequest, requestBody);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<UploadPartResponse> uploadPart(Consumer<UploadPartRequest.Builder> uploadPartRequest,
                                                            AsyncRequestBody requestBody) {
        return client.uploadPart(uploadPartRequest, requestBody);
    }

    @Override
    public CompletableFuture<UploadPartResponse> uploadPart(UploadPartRequest uploadPartRequest, Path sourcePath) {
        return client.uploadPart(uploadPartRequest, sourcePath);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<UploadPartResponse> uploadPart(Consumer<UploadPartRequest.Builder> uploadPartRequest,
                                                            Path sourcePath) {
        return client.uploadPart(uploadPartRequest, sourcePath);
    }

    @Override
    public CompletableFuture<UploadPartCopyResponse> uploadPartCopy(UploadPartCopyRequest uploadPartCopyRequest) {
        return client.uploadPartCopy(uploadPartCopyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<UploadPartCopyResponse> uploadPartCopy(
            Consumer<UploadPartCopyRequest.Builder> uploadPartCopyRequest) {
        return client.uploadPartCopy(uploadPartCopyRequest);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<WriteGetObjectResponseResponse> writeGetObjectResponse(
            WriteGetObjectResponseRequest writeGetObjectResponseRequest, AsyncRequestBody requestBody) {
        return client.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<WriteGetObjectResponseResponse> writeGetObjectResponse(
            Consumer<WriteGetObjectResponseRequest.Builder> writeGetObjectResponseRequest, AsyncRequestBody requestBody) {
        return client.writeGetObjectResponse(writeGetObjectResponseRequest, requestBody);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<WriteGetObjectResponseResponse> writeGetObjectResponse(
            WriteGetObjectResponseRequest writeGetObjectResponseRequest, Path sourcePath) {
        return client.writeGetObjectResponse(writeGetObjectResponseRequest, sourcePath);
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public CompletableFuture<WriteGetObjectResponseResponse> writeGetObjectResponse(
            Consumer<WriteGetObjectResponseRequest.Builder> writeGetObjectResponseRequest, Path sourcePath) {
        return client.writeGetObjectResponse(writeGetObjectResponseRequest, sourcePath);
    }

    @Override
    public S3AsyncWaiter waiter() {
        return client.waiter();
    }

    @Override
    public software.amazon.awssdk.services.s3.S3ServiceClientConfiguration serviceClientConfiguration() {
        return client.serviceClientConfiguration();
    }
}
