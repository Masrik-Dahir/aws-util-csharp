using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Metadata for a single S3 object.</summary>
public sealed record S3Object(
    string Bucket,
    string Key,
    long? Size = null,
    DateTime? LastModified = null,
    string? ETag = null);

/// <summary>Metadata for a specific version of an S3 object.</summary>
public sealed record S3ObjectVersion(
    string Bucket,
    string Key,
    string VersionId,
    bool IsLatest = false,
    DateTime? LastModified = null,
    string? ETag = null,
    long? Size = null,
    bool IsDeleteMarker = false);

/// <summary>A time-limited pre-signed URL for an S3 object.</summary>
public sealed record PresignedUrl(
    string Url,
    string Bucket,
    string Key,
    int ExpiresIn);

/// <summary>Result of a HeadObject operation.</summary>
public sealed record HeadObjectResult(
    bool? DeleteMarker = null,
    string? AcceptRanges = null,
    string? Expiration = null,
    string? Restore = null,
    string? ArchiveStatus = null,
    DateTime? LastModified = null,
    long? ContentLength = null,
    string? ETag = null,
    int? MissingMeta = null,
    string? VersionId = null,
    string? CacheControl = null,
    string? ContentDisposition = null,
    string? ContentEncoding = null,
    string? ContentLanguage = null,
    string? ContentType = null,
    string? WebsiteRedirectLocation = null,
    string? ServerSideEncryption = null,
    Dictionary<string, string>? Metadata = null,
    string? StorageClass = null,
    string? RequestCharged = null,
    string? ReplicationStatus = null,
    int? PartsCount = null,
    int? TagCount = null,
    string? ObjectLockMode = null,
    DateTime? ObjectLockRetainUntilDate = null,
    string? ObjectLockLegalHoldStatus = null);

/// <summary>Result of a PutObject operation.</summary>
public sealed record PutObjectResult(
    string? Expiration = null,
    string? ETag = null,
    string? ChecksumCrc32 = null,
    string? ChecksumCrc32C = null,
    string? ChecksumSha1 = null,
    string? ChecksumSha256 = null,
    string? ServerSideEncryption = null,
    string? VersionId = null,
    string? SseKmsKeyId = null,
    string? SseKmsEncryptionContext = null,
    bool? BucketKeyEnabled = null,
    string? RequestCharged = null);

/// <summary>Result of a GetObject operation.</summary>
public sealed record GetObjectResult(
    byte[] Body,
    string? ContentType = null,
    long? ContentLength = null,
    string? ETag = null,
    string? VersionId = null,
    DateTime? LastModified = null,
    Dictionary<string, string>? Metadata = null);

/// <summary>Result of a ListObjectsV2 operation (paginated).</summary>
public sealed record ListObjectsResult(
    List<S3Object> Objects,
    bool IsTruncated = false,
    string? NextContinuationToken = null,
    int KeyCount = 0);

/// <summary>Result of a DeleteObjects operation.</summary>
public sealed record DeleteObjectsResult(
    List<DeletedObject>? Deleted = null,
    List<DeleteError>? Errors = null);

/// <summary>Result of a CopyObject operation.</summary>
public sealed record CopyObjectResult(
    string? ETag = null,
    DateTime? LastModified = null,
    string? VersionId = null,
    string? ServerSideEncryption = null,
    string? RequestCharged = null);

/// <summary>Result of a CreateBucket operation.</summary>
public sealed record CreateBucketResult(string? Location = null);

/// <summary>Result of a ListBuckets operation.</summary>
public sealed record ListBucketsResult(
    List<S3BucketInfo> Buckets,
    string? OwnerDisplayName = null,
    string? OwnerId = null);

/// <summary>Basic bucket metadata.</summary>
public sealed record S3BucketInfo(
    string BucketName,
    DateTime CreationDate);

/// <summary>Result of GetBucketLocation.</summary>
public sealed record GetBucketLocationResult(string? LocationConstraint = null);

/// <summary>Result of GetBucketVersioning.</summary>
public sealed record GetBucketVersioningResult(
    string? Status = null,
    string? MfaDelete = null);

/// <summary>Result of GetBucketPolicy.</summary>
public sealed record GetBucketPolicyResult(string? Policy = null);

/// <summary>Result of GetBucketEncryption.</summary>
public sealed record GetBucketEncryptionResult(
    ServerSideEncryptionConfiguration? Configuration = null);

/// <summary>Result of GetBucketTagging.</summary>
public sealed record GetBucketTaggingResult(
    List<Tag>? Tags = null);

/// <summary>Result of CreateMultipartUpload.</summary>
public sealed record CreateMultipartUploadResult(
    string? Bucket = null,
    string? Key = null,
    string? UploadId = null,
    string? ServerSideEncryption = null,
    string? SseKmsKeyId = null,
    bool? BucketKeyEnabled = null);

/// <summary>Result of UploadPart.</summary>
public sealed record UploadPartResult(
    string? ETag = null,
    string? ServerSideEncryption = null,
    int PartNumber = 0);

/// <summary>Result of CompleteMultipartUpload.</summary>
public sealed record CompleteMultipartUploadResult(
    string? Location = null,
    string? Bucket = null,
    string? Key = null,
    string? ETag = null,
    string? VersionId = null,
    string? ServerSideEncryption = null);

/// <summary>Result of a BatchCopy operation.</summary>
public sealed record BatchCopyResult(int Succeeded, int Failed, List<string>? Errors = null);

/// <summary>Specification for a single copy operation within BatchCopy.</summary>
public sealed record CopySpec(
    string SrcBucket,
    string SrcKey,
    string DstBucket,
    string DstKey);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon S3.
/// Port of the Python <c>aws_util.s3</c> module.
/// </summary>
public static class S3Service
{
    private static AmazonS3Client GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonS3Client>(region);

    // -----------------------------------------------------------------------
    // Upload / Download
    // -----------------------------------------------------------------------

    /// <summary>
    /// Upload a local file to S3.
    /// </summary>
    public static async Task UploadFileAsync(
        string bucket,
        string key,
        string filePath,
        string? contentType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            FilePath = filePath
        };
        if (contentType != null)
            request.ContentType = contentType;

        try
        {
            await client.PutObjectAsync(request);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to upload '{filePath}' to s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Download an S3 object and return its contents as bytes.
    /// </summary>
    public static async Task<byte[]> DownloadBytesAsync(
        string bucket,
        string key,
        string? versionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (versionId != null)
            request.VersionId = versionId;

        try
        {
            using var resp = await client.GetObjectAsync(request);
            using var ms = new MemoryStream();
            await resp.ResponseStream.CopyToAsync(ms);
            return ms.ToArray();
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to download s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Download an S3 object to a local file.
    /// </summary>
    public static async Task DownloadFileAsync(
        string bucket,
        string key,
        string destPath,
        string? versionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (versionId != null)
            request.VersionId = versionId;

        try
        {
            using var resp = await client.GetObjectAsync(request);
            await resp.WriteResponseStreamToFileAsync(destPath, false, CancellationToken.None);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to download s3://{bucket}/{key} to '{destPath}'");
        }
    }

    // -----------------------------------------------------------------------
    // GetObject / PutObject
    // -----------------------------------------------------------------------

    /// <summary>
    /// Fetch an S3 object and return the full response including body bytes.
    /// </summary>
    public static async Task<GetObjectResult> GetObjectAsync(
        string bucket,
        string key,
        string? versionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (versionId != null)
            request.VersionId = versionId;

        try
        {
            using var resp = await client.GetObjectAsync(request);
            using var ms = new MemoryStream();
            await resp.ResponseStream.CopyToAsync(ms);

            return new GetObjectResult(
                Body: ms.ToArray(),
                ContentType: resp.Headers.ContentType,
                ContentLength: resp.ContentLength,
                ETag: resp.ETag?.Trim('"'),
                VersionId: resp.VersionId,
                LastModified: resp.LastModified,
                Metadata: resp.Metadata.Keys.Count > 0
                    ? resp.Metadata.Keys
                        .ToDictionary(k => k, k => resp.Metadata[k])
                    : null);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Put an object into S3 from bytes or a stream.
    /// </summary>
    public static async Task<PutObjectResult> PutObjectAsync(
        string bucket,
        string key,
        byte[]? body = null,
        Stream? inputStream = null,
        string? contentType = null,
        Dictionary<string, string>? metadata = null,
        string? storageClass = null,
        string? serverSideEncryption = null,
        string? tagging = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutObjectRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (body != null)
            request.InputStream = new MemoryStream(body);
        else if (inputStream != null)
            request.InputStream = inputStream;
        if (contentType != null)
            request.ContentType = contentType;
        if (storageClass != null)
            request.StorageClass = new S3StorageClass(storageClass);
        if (serverSideEncryption != null)
            request.ServerSideEncryptionMethod = new ServerSideEncryptionMethod(serverSideEncryption);
        if (tagging != null)
            request.TagSet = ParseTagging(tagging);
        if (metadata != null)
        {
            foreach (var kv in metadata)
                request.Metadata.Add(kv.Key, kv.Value);
        }

        try
        {
            var resp = await client.PutObjectAsync(request);
            return new PutObjectResult(
                Expiration: resp.Expiration?.ExpiryDate.ToString(),
                ETag: resp.ETag?.Trim('"'),
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value,
                VersionId: resp.VersionId,
                SseKmsKeyId: resp.ServerSideEncryptionKeyManagementServiceKeyId,
                BucketKeyEnabled: resp.BucketKeyEnabled,
                RequestCharged: resp.RequestCharged?.Value);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put object s3://{bucket}/{key}");
        }
    }

    // -----------------------------------------------------------------------
    // List / Head / Exists
    // -----------------------------------------------------------------------

    /// <summary>
    /// List objects in a bucket, optionally filtered by prefix. Auto-paginates.
    /// </summary>
    public static async Task<List<S3Object>> ListObjectsAsync(
        string bucket,
        string prefix = "",
        int? maxKeys = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var objects = new List<S3Object>();
        var request = new ListObjectsV2Request
        {
            BucketName = bucket,
            Prefix = prefix
        };
        if (maxKeys.HasValue)
            request.MaxKeys = maxKeys.Value;

        try
        {
            ListObjectsV2Response resp;
            do
            {
                resp = await client.ListObjectsV2Async(request);
                if (resp.S3Objects != null)
                {
                    foreach (var obj in resp.S3Objects)
                    {
                        objects.Add(new S3Object(
                            Bucket: bucket,
                            Key: obj.Key,
                            Size: obj.Size,
                            LastModified: obj.LastModified,
                            ETag: obj.ETag?.Trim('"')));
                    }
                }
                request.ContinuationToken = resp.NextContinuationToken;
            } while (resp.IsTruncated ?? false);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list objects in s3://{bucket}/{prefix}");
        }

        return objects;
    }

    /// <summary>
    /// Single-page ListObjectsV2 call returning the raw result with pagination token.
    /// </summary>
    public static async Task<ListObjectsResult> ListObjectsV2Async(
        string bucket,
        string? prefix = null,
        string? delimiter = null,
        int? maxKeys = null,
        string? continuationToken = null,
        string? startAfter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListObjectsV2Request { BucketName = bucket };
        if (prefix != null) request.Prefix = prefix;
        if (delimiter != null) request.Delimiter = delimiter;
        if (maxKeys.HasValue) request.MaxKeys = maxKeys.Value;
        if (continuationToken != null) request.ContinuationToken = continuationToken;
        if (startAfter != null) request.StartAfter = startAfter;

        try
        {
            var resp = await client.ListObjectsV2Async(request);
            var objs = resp.S3Objects?.Select(o => new S3Object(
                Bucket: bucket,
                Key: o.Key,
                Size: o.Size,
                LastModified: o.LastModified,
                ETag: o.ETag?.Trim('"'))).ToList() ?? [];

            return new ListObjectsResult(
                Objects: objs,
                IsTruncated: resp.IsTruncated ?? false,
                NextContinuationToken: resp.NextContinuationToken,
                KeyCount: resp.KeyCount ?? 0);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list objects v2 in s3://{bucket}");
        }
    }

    /// <summary>
    /// Head object -- check existence and retrieve metadata without downloading the body.
    /// </summary>
    public static async Task<HeadObjectResult> HeadObjectAsync(
        string bucket,
        string key,
        string? versionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetObjectMetadataRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (versionId != null)
            request.VersionId = versionId;

        try
        {
            var resp = await client.GetObjectMetadataAsync(request);
            return new HeadObjectResult(
                DeleteMarker: resp.DeleteMarker == "true",
                LastModified: resp.LastModified,
                ContentLength: resp.ContentLength,
                ETag: resp.ETag?.Trim('"'),
                VersionId: resp.VersionId,
                ContentType: resp.Headers.ContentType,
                ContentEncoding: resp.Headers.ContentEncoding,
                ContentDisposition: resp.Headers.ContentDisposition,
                CacheControl: resp.Headers.CacheControl,
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value,
                StorageClass: resp.StorageClass?.Value,
                Expiration: resp.Expiration?.ExpiryDate.ToString(),
                Restore: resp.RestoreInProgress != null
                    ? $"ongoing-request=\"{(resp.RestoreInProgress.Value ? "true" : "false")}\""
                    : null,
                PartsCount: resp.PartsCount,
                Metadata: resp.Metadata.Keys.Count > 0
                    ? resp.Metadata.Keys
                        .ToDictionary(k => k, k => resp.Metadata[k])
                    : null,
                ObjectLockMode: resp.ObjectLockMode?.Value,
                ObjectLockRetainUntilDate: resp.ObjectLockRetainUntilDate != DateTime.MinValue
                    ? resp.ObjectLockRetainUntilDate
                    : null,
                ObjectLockLegalHoldStatus: resp.ObjectLockLegalHoldStatus?.Value);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to head object s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Check whether an S3 object exists without downloading it.
    /// </summary>
    public static async Task<bool> ObjectExistsAsync(
        string bucket,
        string key,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
            {
                BucketName = bucket,
                Key = key
            });
            return true;
        }
        catch (AmazonS3Exception exc) when (exc.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to check existence of s3://{bucket}/{key}");
        }
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// <summary>
    /// Delete a single object from S3.
    /// </summary>
    public static async Task DeleteObjectAsync(
        string bucket,
        string key,
        string? versionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteObjectRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (versionId != null)
            request.VersionId = versionId;

        try
        {
            await client.DeleteObjectAsync(request);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Delete multiple objects from S3 in a single request (up to 1000).
    /// </summary>
    public static async Task<DeleteObjectsResult> DeleteObjectsAsync(
        string bucket,
        List<KeyVersion> keys,
        bool quiet = true,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteObjectsRequest
        {
            BucketName = bucket,
            Quiet = quiet,
            Objects = keys
        };

        try
        {
            var resp = await client.DeleteObjectsAsync(request);
            return new DeleteObjectsResult(
                Deleted: resp.DeletedObjects,
                Errors: resp.DeleteErrors);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete objects from s3://{bucket}");
        }
    }

    /// <summary>
    /// Delete all S3 objects whose key starts with the given prefix.
    /// Uses batch delete (up to 1000 objects per request).
    /// </summary>
    public static async Task<int> DeletePrefixAsync(
        string bucket,
        string prefix,
        RegionEndpoint? region = null)
    {
        var objects = await ListObjectsAsync(bucket, prefix, region: region);
        if (objects.Count == 0) return 0;

        var deletedCount = 0;
        // Process in chunks of 1000 (S3 batch delete limit)
        for (var i = 0; i < objects.Count; i += 1000)
        {
            var batch = objects.Skip(i).Take(1000)
                .Select(o => new KeyVersion { Key = o.Key })
                .ToList();

            await DeleteObjectsAsync(bucket, batch, quiet: true, region: region);
            deletedCount += batch.Count;
        }

        return deletedCount;
    }

    // -----------------------------------------------------------------------
    // Copy / Move
    // -----------------------------------------------------------------------

    /// <summary>
    /// Server-side copy of an S3 object (no data transfer through the client).
    /// </summary>
    public static async Task<CopyObjectResult> CopyObjectAsync(
        string srcBucket,
        string srcKey,
        string dstBucket,
        string dstKey,
        string? storageClass = null,
        string? serverSideEncryption = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new Amazon.S3.Model.CopyObjectRequest
        {
            SourceBucket = srcBucket,
            SourceKey = srcKey,
            DestinationBucket = dstBucket,
            DestinationKey = dstKey
        };
        if (storageClass != null)
            request.StorageClass = new S3StorageClass(storageClass);
        if (serverSideEncryption != null)
            request.ServerSideEncryptionMethod = new ServerSideEncryptionMethod(serverSideEncryption);

        try
        {
            var resp = await client.CopyObjectAsync(request);
            return new CopyObjectResult(
                ETag: resp.ETag?.Trim('"'),
                LastModified: string.IsNullOrEmpty(resp.LastModified)
                    ? null
                    : DateTime.Parse(resp.LastModified),
                VersionId: resp.VersionId,
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value,
                RequestCharged: resp.RequestCharged?.Value);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to copy s3://{srcBucket}/{srcKey} -> s3://{dstBucket}/{dstKey}");
        }
    }

    /// <summary>
    /// Move an S3 object by copying it then deleting the source.
    /// </summary>
    public static async Task MoveObjectAsync(
        string srcBucket,
        string srcKey,
        string dstBucket,
        string dstKey,
        RegionEndpoint? region = null)
    {
        await CopyObjectAsync(srcBucket, srcKey, dstBucket, dstKey, region: region);
        await DeleteObjectAsync(srcBucket, srcKey, region: region);
    }

    /// <summary>
    /// Copy multiple S3 objects concurrently.
    /// </summary>
    public static async Task<BatchCopyResult> BatchCopyAsync(
        List<CopySpec> copies,
        int maxConcurrency = 20,
        RegionEndpoint? region = null)
    {
        var errors = new List<string>();
        var succeeded = 0;

        using var semaphore = new SemaphoreSlim(maxConcurrency);
        var tasks = copies.Select(async spec =>
        {
            await semaphore.WaitAsync();
            try
            {
                await CopyObjectAsync(
                    spec.SrcBucket, spec.SrcKey,
                    spec.DstBucket, spec.DstKey,
                    region: region);
                Interlocked.Increment(ref succeeded);
            }
            catch (Exception exc)
            {
                lock (errors)
                {
                    errors.Add(
                        $"s3://{spec.SrcBucket}/{spec.SrcKey}: {exc.Message}");
                }
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);

        return new BatchCopyResult(
            Succeeded: succeeded,
            Failed: errors.Count,
            Errors: errors.Count > 0 ? errors : null);
    }

    // -----------------------------------------------------------------------
    // Pre-signed URLs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Generate a pre-signed URL for an S3 object.
    /// </summary>
    public static PresignedUrl GeneratePresignedUrl(
        string bucket,
        string key,
        int expiresInSeconds = 3600,
        HttpVerb verb = HttpVerb.GET,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var url = client.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = bucket,
                Key = key,
                Expires = DateTime.UtcNow.AddSeconds(expiresInSeconds),
                Verb = verb
            });

            return new PresignedUrl(
                Url: url,
                Bucket: bucket,
                Key: key,
                ExpiresIn: expiresInSeconds);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to generate pre-signed URL for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Generate a pre-signed URL asynchronously (wraps sync SDK call).
    /// </summary>
    public static Task<PresignedUrl> GeneratePresignedUrlAsync(
        string bucket,
        string key,
        int expiresInSeconds = 3600,
        HttpVerb verb = HttpVerb.GET,
        RegionEndpoint? region = null)
    {
        return Task.FromResult(GeneratePresignedUrl(
            bucket, key, expiresInSeconds, verb, region));
    }

    // -----------------------------------------------------------------------
    // Bucket operations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create an S3 bucket.
    /// </summary>
    public static async Task<CreateBucketResult> CreateBucketAsync(
        string bucket,
        string? locationConstraint = null,
        bool? objectLockEnabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutBucketRequest { BucketName = bucket };

        if (locationConstraint != null)
        {
            request.BucketRegionName = locationConstraint;
        }
        if (objectLockEnabled == true)
        {
            request.ObjectLockEnabledForBucket = true;
        }

        try
        {
            var resp = await client.PutBucketAsync(request);
            return new CreateBucketResult(Location: resp.ResponseMetadata?.RequestId);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create bucket '{bucket}'");
        }
    }

    /// <summary>
    /// Delete an S3 bucket. The bucket must be empty.
    /// </summary>
    public static async Task DeleteBucketAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBucketAsync(new DeleteBucketRequest
            {
                BucketName = bucket
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bucket '{bucket}'");
        }
    }

    /// <summary>
    /// List all S3 buckets owned by the authenticated sender.
    /// </summary>
    public static async Task<ListBucketsResult> ListBucketsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListBucketsAsync();
            var buckets = resp.Buckets
                .Select(b => new S3BucketInfo(b.BucketName, b.CreationDate ?? DateTime.MinValue))
                .ToList();

            return new ListBucketsResult(
                Buckets: buckets,
                OwnerDisplayName: resp.Owner?.DisplayName,
                OwnerId: resp.Owner?.Id);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list buckets");
        }
    }

    // -----------------------------------------------------------------------
    // Bucket Location / Versioning
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get the region/location constraint for a bucket.
    /// </summary>
    public static async Task<GetBucketLocationResult> GetBucketLocationAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBucketLocationAsync(
                new GetBucketLocationRequest { BucketName = bucket });
            return new GetBucketLocationResult(
                LocationConstraint: resp.Location?.Value);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bucket location for '{bucket}'");
        }
    }

    /// <summary>
    /// Get the versioning state of a bucket.
    /// </summary>
    public static async Task<GetBucketVersioningResult> GetBucketVersioningAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBucketVersioningAsync(
                new GetBucketVersioningRequest { BucketName = bucket });
            return new GetBucketVersioningResult(
                Status: resp.VersioningConfig?.Status?.Value,
                MfaDelete: resp.VersioningConfig?.EnableMfaDelete?.ToString());
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bucket versioning for '{bucket}'");
        }
    }

    /// <summary>
    /// Enable or suspend versioning on a bucket.
    /// </summary>
    public static async Task PutBucketVersioningAsync(
        string bucket,
        string status,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutBucketVersioningAsync(new PutBucketVersioningRequest
            {
                BucketName = bucket,
                VersioningConfig = new S3BucketVersioningConfig
                {
                    Status = new VersionStatus(status)
                }
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bucket versioning for '{bucket}'");
        }
    }

    // -----------------------------------------------------------------------
    // Bucket Policy
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get the policy for a bucket.
    /// </summary>
    public static async Task<GetBucketPolicyResult> GetBucketPolicyAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBucketPolicyAsync(
                new GetBucketPolicyRequest { BucketName = bucket });
            return new GetBucketPolicyResult(Policy: resp.Policy);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bucket policy for '{bucket}'");
        }
    }

    /// <summary>
    /// Set the policy for a bucket.
    /// </summary>
    public static async Task PutBucketPolicyAsync(
        string bucket,
        string policy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutBucketPolicyAsync(new PutBucketPolicyRequest
            {
                BucketName = bucket,
                Policy = policy
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bucket policy for '{bucket}'");
        }
    }

    /// <summary>
    /// Delete the policy for a bucket.
    /// </summary>
    public static async Task DeleteBucketPolicyAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBucketPolicyAsync(
                new DeleteBucketPolicyRequest { BucketName = bucket });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bucket policy for '{bucket}'");
        }
    }

    // -----------------------------------------------------------------------
    // Bucket Encryption
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get the default encryption configuration for a bucket.
    /// </summary>
    public static async Task<GetBucketEncryptionResult> GetBucketEncryptionAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBucketEncryptionAsync(
                new GetBucketEncryptionRequest { BucketName = bucket });
            return new GetBucketEncryptionResult(
                Configuration: resp.ServerSideEncryptionConfiguration);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bucket encryption for '{bucket}'");
        }
    }

    /// <summary>
    /// Set the default encryption configuration for a bucket.
    /// </summary>
    public static async Task PutBucketEncryptionAsync(
        string bucket,
        ServerSideEncryptionConfiguration configuration,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutBucketEncryptionAsync(new PutBucketEncryptionRequest
            {
                BucketName = bucket,
                ServerSideEncryptionConfiguration = configuration
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bucket encryption for '{bucket}'");
        }
    }

    /// <summary>
    /// Delete the default encryption configuration for a bucket.
    /// </summary>
    public static async Task DeleteBucketEncryptionAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBucketEncryptionAsync(
                new DeleteBucketEncryptionRequest { BucketName = bucket });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bucket encryption for '{bucket}'");
        }
    }

    // -----------------------------------------------------------------------
    // Bucket Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get the tag set for a bucket.
    /// </summary>
    public static async Task<GetBucketTaggingResult> GetBucketTaggingAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBucketTaggingAsync(
                new GetBucketTaggingRequest { BucketName = bucket });
            return new GetBucketTaggingResult(Tags: resp.TagSet);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bucket tagging for '{bucket}'");
        }
    }

    /// <summary>
    /// Set the tag set for a bucket.
    /// </summary>
    public static async Task PutBucketTaggingAsync(
        string bucket,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutBucketTaggingAsync(new PutBucketTaggingRequest
            {
                BucketName = bucket,
                TagSet = tags
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bucket tagging for '{bucket}'");
        }
    }

    /// <summary>
    /// Delete the tag set for a bucket.
    /// </summary>
    public static async Task DeleteBucketTaggingAsync(
        string bucket,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBucketTaggingAsync(
                new DeleteBucketTaggingRequest { BucketName = bucket });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bucket tagging for '{bucket}'");
        }
    }

    // -----------------------------------------------------------------------
    // Multipart Upload
    // -----------------------------------------------------------------------

    /// <summary>
    /// Initiate a multipart upload.
    /// </summary>
    public static async Task<CreateMultipartUploadResult> CreateMultipartUploadAsync(
        string bucket,
        string key,
        string? contentType = null,
        string? storageClass = null,
        string? serverSideEncryption = null,
        Dictionary<string, string>? metadata = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InitiateMultipartUploadRequest
        {
            BucketName = bucket,
            Key = key
        };
        if (contentType != null) request.ContentType = contentType;
        if (storageClass != null) request.StorageClass = new S3StorageClass(storageClass);
        if (serverSideEncryption != null)
            request.ServerSideEncryptionMethod = new ServerSideEncryptionMethod(serverSideEncryption);
        if (metadata != null)
        {
            foreach (var kv in metadata)
                request.Metadata.Add(kv.Key, kv.Value);
        }

        try
        {
            var resp = await client.InitiateMultipartUploadAsync(request);
            return new CreateMultipartUploadResult(
                Bucket: resp.BucketName,
                Key: resp.Key,
                UploadId: resp.UploadId,
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value,
                SseKmsKeyId: resp.ServerSideEncryptionKeyManagementServiceKeyId,
                BucketKeyEnabled: resp.BucketKeyEnabled);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create multipart upload for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Upload a single part of a multipart upload.
    /// </summary>
    public static async Task<UploadPartResult> UploadPartAsync(
        string bucket,
        string key,
        string uploadId,
        int partNumber,
        byte[]? body = null,
        Stream? inputStream = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new Amazon.S3.Model.UploadPartRequest
        {
            BucketName = bucket,
            Key = key,
            UploadId = uploadId,
            PartNumber = partNumber
        };
        if (body != null)
            request.InputStream = new MemoryStream(body);
        else if (inputStream != null)
            request.InputStream = inputStream;

        try
        {
            var resp = await client.UploadPartAsync(request);
            return new UploadPartResult(
                ETag: resp.ETag?.Trim('"'),
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value,
                PartNumber: partNumber);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to upload part {partNumber} for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Complete a multipart upload.
    /// </summary>
    public static async Task<CompleteMultipartUploadResult> CompleteMultipartUploadAsync(
        string bucket,
        string key,
        string uploadId,
        List<PartETag> parts,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CompleteMultipartUploadAsync(
                new CompleteMultipartUploadRequest
                {
                    BucketName = bucket,
                    Key = key,
                    UploadId = uploadId,
                    PartETags = parts
                });
            return new CompleteMultipartUploadResult(
                Location: resp.Location,
                Bucket: resp.BucketName,
                Key: resp.Key,
                ETag: resp.ETag?.Trim('"'),
                VersionId: resp.VersionId,
                ServerSideEncryption: resp.ServerSideEncryptionMethod?.Value);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to complete multipart upload for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Abort a multipart upload.
    /// </summary>
    public static async Task AbortMultipartUploadAsync(
        string bucket,
        string key,
        string uploadId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = uploadId
            });
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to abort multipart upload for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Upload a large file using multipart upload with parallel part uploads.
    /// Prefer this over <see cref="UploadFileAsync"/> for files over 100 MB.
    /// </summary>
    public static async Task MultipartUploadFileAsync(
        string bucket,
        string key,
        string filePath,
        int partSizeMb = 50,
        int maxConcurrency = 10,
        string? contentType = null,
        RegionEndpoint? region = null)
    {
        if (partSizeMb < 5)
            throw new ArgumentException("partSizeMb must be at least 5 MB", nameof(partSizeMb));

        var partSizeBytes = (long)partSizeMb * 1024 * 1024;
        var fileInfo = new FileInfo(filePath);
        if (!fileInfo.Exists)
            throw new FileNotFoundException($"File not found: {filePath}");

        var mpu = await CreateMultipartUploadAsync(
            bucket, key, contentType: contentType, region: region);
        var uploadId = mpu.UploadId!;

        try
        {
            var partETags = new List<PartETag>();
            var chunks = new List<(int PartNumber, long Offset, int Length)>();
            var partNumber = 1;
            var remaining = fileInfo.Length;
            long offset = 0;

            while (remaining > 0)
            {
                var length = (int)Math.Min(remaining, partSizeBytes);
                chunks.Add((partNumber, offset, length));
                offset += length;
                remaining -= length;
                partNumber++;
            }

            using var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = chunks.Select(async chunk =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var buffer = new byte[chunk.Length];
                    using var fs = new FileStream(filePath, FileMode.Open,
                        FileAccess.Read, FileShare.Read);
                    fs.Seek(chunk.Offset, SeekOrigin.Begin);
                    var bytesRead = await fs.ReadAsync(buffer, 0, chunk.Length);

                    var result = await UploadPartAsync(
                        bucket, key, uploadId, chunk.PartNumber,
                        body: buffer[..bytesRead], region: region);

                    lock (partETags)
                    {
                        partETags.Add(new PartETag(chunk.PartNumber, result.ETag));
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            });

            await Task.WhenAll(tasks);
            partETags.Sort((a, b) => (a.PartNumber ?? 0).CompareTo(b.PartNumber ?? 0));

            await CompleteMultipartUploadAsync(
                bucket, key, uploadId, partETags, region: region);
        }
        catch (Exception)
        {
            await AbortMultipartUploadAsync(bucket, key, uploadId, region: region);
            throw;
        }
    }

    // -----------------------------------------------------------------------
    // Object versions
    // -----------------------------------------------------------------------

    /// <summary>
    /// List all versions of objects in a versioned S3 bucket.
    /// </summary>
    public static async Task<List<S3ObjectVersion>> ListObjectVersionsAsync(
        string bucket,
        string prefix = "",
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var versions = new List<S3ObjectVersion>();
        var request = new ListVersionsRequest
        {
            BucketName = bucket,
            Prefix = prefix
        };

        try
        {
            ListVersionsResponse resp;
            do
            {
                resp = await client.ListVersionsAsync(request);
                if (resp.Versions != null)
                {
                    foreach (var v in resp.Versions)
                    {
                        versions.Add(new S3ObjectVersion(
                            Bucket: bucket,
                            Key: v.Key,
                            VersionId: v.VersionId ?? "null",
                            IsLatest: v.IsLatest ?? false,
                            LastModified: v.LastModified,
                            ETag: v.ETag?.Trim('"'),
                            Size: v.Size,
                            IsDeleteMarker: v.IsDeleteMarker ?? false));
                    }
                }
                request.KeyMarker = resp.NextKeyMarker;
                request.VersionIdMarker = resp.NextVersionIdMarker;
            } while (resp.IsTruncated ?? false);
        }
        catch (AmazonS3Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list versions in s3://{bucket}/{prefix}");
        }

        return versions;
    }

    // -----------------------------------------------------------------------
    // Convenience helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Download an S3 object and return its contents as a UTF-8 string.
    /// </summary>
    public static async Task<string> DownloadAsTextAsync(
        string bucket,
        string key,
        RegionEndpoint? region = null)
    {
        var bytes = await DownloadBytesAsync(bucket, key, region: region);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Upload raw bytes to S3 (convenience wrapper around PutObject).
    /// </summary>
    public static async Task UploadBytesAsync(
        string bucket,
        string key,
        byte[] data,
        string? contentType = null,
        RegionEndpoint? region = null)
    {
        await PutObjectAsync(bucket, key, body: data,
            contentType: contentType, region: region);
    }

    /// <summary>
    /// Rename an S3 object by copying to the new key then deleting the original.
    /// </summary>
    public static async Task RenameObjectAsync(
        string bucket,
        string oldKey,
        string newKey,
        RegionEndpoint? region = null)
    {
        await CopyObjectAsync(bucket, oldKey, bucket, newKey, region: region);
        await DeleteObjectAsync(bucket, oldKey, region: region);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// <summary>
    /// Parse a URL-encoded tagging string like "Key1=Value1&amp;Key2=Value2"
    /// into a list of Tag objects.
    /// </summary>
    private static List<Tag> ParseTagging(string tagging)
    {
        var tags = new List<Tag>();
        if (string.IsNullOrWhiteSpace(tagging)) return tags;

        foreach (var pair in tagging.Split('&'))
        {
            var parts = pair.Split('=', 2);
            if (parts.Length == 2)
            {
                tags.Add(new Tag
                {
                    Key = Uri.UnescapeDataString(parts[0]),
                    Value = Uri.UnescapeDataString(parts[1])
                });
            }
        }

        return tags;
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="UploadFileAsync"/>.</summary>
    public static void UploadFile(string bucket, string key, string filePath, string? contentType = null, RegionEndpoint? region = null)
        => UploadFileAsync(bucket, key, filePath, contentType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DownloadBytesAsync"/>.</summary>
    public static byte[] DownloadBytes(string bucket, string key, string? versionId = null, RegionEndpoint? region = null)
        => DownloadBytesAsync(bucket, key, versionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DownloadFileAsync"/>.</summary>
    public static void DownloadFile(string bucket, string key, string destPath, string? versionId = null, RegionEndpoint? region = null)
        => DownloadFileAsync(bucket, key, destPath, versionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetObjectAsync"/>.</summary>
    public static GetObjectResult GetObject(string bucket, string key, string? versionId = null, RegionEndpoint? region = null)
        => GetObjectAsync(bucket, key, versionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutObjectAsync"/>.</summary>
    public static PutObjectResult PutObject(string bucket, string key, byte[]? body = null, Stream? inputStream = null, string? contentType = null, Dictionary<string, string>? metadata = null, string? storageClass = null, string? serverSideEncryption = null, string? tagging = null, RegionEndpoint? region = null)
        => PutObjectAsync(bucket, key, body, inputStream, contentType, metadata, storageClass, serverSideEncryption, tagging, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListObjectsAsync"/>.</summary>
    public static List<S3Object> ListObjects(string bucket, string prefix = "", int? maxKeys = null, RegionEndpoint? region = null)
        => ListObjectsAsync(bucket, prefix, maxKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListObjectsV2Async"/>.</summary>
    public static ListObjectsResult ListObjectsV2(string bucket, string? prefix = null, string? delimiter = null, int? maxKeys = null, string? continuationToken = null, string? startAfter = null, RegionEndpoint? region = null)
        => ListObjectsV2Async(bucket, prefix, delimiter, maxKeys, continuationToken, startAfter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="HeadObjectAsync"/>.</summary>
    public static HeadObjectResult HeadObject(string bucket, string key, string? versionId = null, RegionEndpoint? region = null)
        => HeadObjectAsync(bucket, key, versionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ObjectExistsAsync"/>.</summary>
    public static bool ObjectExists(string bucket, string key, RegionEndpoint? region = null)
        => ObjectExistsAsync(bucket, key, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteObjectAsync"/>.</summary>
    public static void DeleteObject(string bucket, string key, string? versionId = null, RegionEndpoint? region = null)
        => DeleteObjectAsync(bucket, key, versionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteObjectsAsync"/>.</summary>
    public static DeleteObjectsResult DeleteObjects(string bucket, List<KeyVersion> keys, bool quiet = true, RegionEndpoint? region = null)
        => DeleteObjectsAsync(bucket, keys, quiet, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePrefixAsync"/>.</summary>
    public static int DeletePrefix(string bucket, string prefix, RegionEndpoint? region = null)
        => DeletePrefixAsync(bucket, prefix, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CopyObjectAsync"/>.</summary>
    public static CopyObjectResult CopyObject(string srcBucket, string srcKey, string dstBucket, string dstKey, string? storageClass = null, string? serverSideEncryption = null, RegionEndpoint? region = null)
        => CopyObjectAsync(srcBucket, srcKey, dstBucket, dstKey, storageClass, serverSideEncryption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MoveObjectAsync"/>.</summary>
    public static void MoveObject(string srcBucket, string srcKey, string dstBucket, string dstKey, RegionEndpoint? region = null)
        => MoveObjectAsync(srcBucket, srcKey, dstBucket, dstKey, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchCopyAsync"/>.</summary>
    public static BatchCopyResult BatchCopy(List<CopySpec> copies, int maxConcurrency = 20, RegionEndpoint? region = null)
        => BatchCopyAsync(copies, maxConcurrency, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateBucketAsync"/>.</summary>
    public static CreateBucketResult CreateBucket(string bucket, string? locationConstraint = null, bool? objectLockEnabled = null, RegionEndpoint? region = null)
        => CreateBucketAsync(bucket, locationConstraint, objectLockEnabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBucketAsync"/>.</summary>
    public static void DeleteBucket(string bucket, RegionEndpoint? region = null)
        => DeleteBucketAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListBucketsAsync"/>.</summary>
    public static ListBucketsResult ListBuckets(RegionEndpoint? region = null)
        => ListBucketsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBucketLocationAsync"/>.</summary>
    public static GetBucketLocationResult GetBucketLocation(string bucket, RegionEndpoint? region = null)
        => GetBucketLocationAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBucketVersioningAsync"/>.</summary>
    public static GetBucketVersioningResult GetBucketVersioning(string bucket, RegionEndpoint? region = null)
        => GetBucketVersioningAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutBucketVersioningAsync"/>.</summary>
    public static void PutBucketVersioning(string bucket, string status, RegionEndpoint? region = null)
        => PutBucketVersioningAsync(bucket, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBucketPolicyAsync"/>.</summary>
    public static GetBucketPolicyResult GetBucketPolicy(string bucket, RegionEndpoint? region = null)
        => GetBucketPolicyAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutBucketPolicyAsync"/>.</summary>
    public static void PutBucketPolicy(string bucket, string policy, RegionEndpoint? region = null)
        => PutBucketPolicyAsync(bucket, policy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBucketPolicyAsync"/>.</summary>
    public static void DeleteBucketPolicy(string bucket, RegionEndpoint? region = null)
        => DeleteBucketPolicyAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBucketEncryptionAsync"/>.</summary>
    public static GetBucketEncryptionResult GetBucketEncryption(string bucket, RegionEndpoint? region = null)
        => GetBucketEncryptionAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutBucketEncryptionAsync"/>.</summary>
    public static void PutBucketEncryption(string bucket, ServerSideEncryptionConfiguration configuration, RegionEndpoint? region = null)
        => PutBucketEncryptionAsync(bucket, configuration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBucketEncryptionAsync"/>.</summary>
    public static void DeleteBucketEncryption(string bucket, RegionEndpoint? region = null)
        => DeleteBucketEncryptionAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBucketTaggingAsync"/>.</summary>
    public static GetBucketTaggingResult GetBucketTagging(string bucket, RegionEndpoint? region = null)
        => GetBucketTaggingAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutBucketTaggingAsync"/>.</summary>
    public static void PutBucketTagging(string bucket, List<Tag> tags, RegionEndpoint? region = null)
        => PutBucketTaggingAsync(bucket, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBucketTaggingAsync"/>.</summary>
    public static void DeleteBucketTagging(string bucket, RegionEndpoint? region = null)
        => DeleteBucketTaggingAsync(bucket, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateMultipartUploadAsync"/>.</summary>
    public static CreateMultipartUploadResult CreateMultipartUpload(string bucket, string key, string? contentType = null, string? storageClass = null, string? serverSideEncryption = null, Dictionary<string, string>? metadata = null, RegionEndpoint? region = null)
        => CreateMultipartUploadAsync(bucket, key, contentType, storageClass, serverSideEncryption, metadata, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UploadPartAsync"/>.</summary>
    public static UploadPartResult UploadPart(string bucket, string key, string uploadId, int partNumber, byte[]? body = null, Stream? inputStream = null, RegionEndpoint? region = null)
        => UploadPartAsync(bucket, key, uploadId, partNumber, body, inputStream, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CompleteMultipartUploadAsync"/>.</summary>
    public static CompleteMultipartUploadResult CompleteMultipartUpload(string bucket, string key, string uploadId, List<PartETag> parts, RegionEndpoint? region = null)
        => CompleteMultipartUploadAsync(bucket, key, uploadId, parts, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AbortMultipartUploadAsync"/>.</summary>
    public static void AbortMultipartUpload(string bucket, string key, string uploadId, RegionEndpoint? region = null)
        => AbortMultipartUploadAsync(bucket, key, uploadId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MultipartUploadFileAsync"/>.</summary>
    public static void MultipartUploadFile(string bucket, string key, string filePath, int partSizeMb = 50, int maxConcurrency = 10, string? contentType = null, RegionEndpoint? region = null)
        => MultipartUploadFileAsync(bucket, key, filePath, partSizeMb, maxConcurrency, contentType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListObjectVersionsAsync"/>.</summary>
    public static List<S3ObjectVersion> ListObjectVersions(string bucket, string prefix = "", RegionEndpoint? region = null)
        => ListObjectVersionsAsync(bucket, prefix, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DownloadAsTextAsync"/>.</summary>
    public static string DownloadAsText(string bucket, string key, RegionEndpoint? region = null)
        => DownloadAsTextAsync(bucket, key, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UploadBytesAsync"/>.</summary>
    public static void UploadBytes(string bucket, string key, byte[] data, string? contentType = null, RegionEndpoint? region = null)
        => UploadBytesAsync(bucket, key, data, contentType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RenameObjectAsync"/>.</summary>
    public static void RenameObject(string bucket, string oldKey, string newKey, RegionEndpoint? region = null)
        => RenameObjectAsync(bucket, oldKey, newKey, region).GetAwaiter().GetResult();

}
