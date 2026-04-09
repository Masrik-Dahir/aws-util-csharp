using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.S3;
using Amazon.S3.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of processing an S3 event into DynamoDB.</summary>
public sealed record S3EventToDynamoDbResult(
    string? Bucket = null,
    string? Key = null,
    string? TableName = null,
    bool Success = true);

/// <summary>Result of archiving DynamoDB stream records to S3.</summary>
public sealed record DynamoDbStreamToS3ArchiveResult(
    int RecordsArchived = 0,
    string? Bucket = null,
    string? KeyPrefix = null,
    string? ArchiveKey = null);

/// <summary>Result of bulk loading CSV from S3 to DynamoDB.</summary>
public sealed record S3CsvToDynamoDbBulkResult(
    int TotalRows = 0,
    int SuccessCount = 0,
    int FailureCount = 0,
    string? TableName = null);

/// <summary>Result of transforming Kinesis records for Firehose.</summary>
public sealed record KinesisToFirehoseTransformerResult(
    int RecordsTransformed = 0,
    int RecordsFailed = 0,
    string? DeliveryStreamName = null);

/// <summary>Result of cross-region S3 replication.</summary>
public sealed record CrossRegionS3ReplicatorResult(
    string? SourceBucket = null,
    string? SourceKey = null,
    string? DestinationBucket = null,
    string? DestinationKey = null,
    bool Success = true);

/// <summary>ETL job status entry.</summary>
public sealed record EtlStatusTrackerResult(
    string? JobId = null,
    string? Status = null,
    string? TableName = null,
    DateTime? UpdatedAt = null);

/// <summary>Result of managing a multipart upload.</summary>
public sealed record S3MultipartUploadManagerResult(
    string? Bucket = null,
    string? Key = null,
    string? UploadId = null,
    string? ETag = null,
    string? Location = null,
    bool Completed = false);

/// <summary>Result of managing data lake partitions.</summary>
public sealed record DataLakePartitionManagerResult(
    string? Bucket = null,
    string? Prefix = null,
    int PartitionsCreated = 0,
    List<string>? PartitionPaths = null);

/// <summary>Result of repairing partitions.</summary>
public sealed record RepairPartitionsResult(
    int PartitionsRepaired = 0,
    int PartitionsMissing = 0,
    List<string>? RepairedPaths = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Data flow and ETL orchestration combining S3, DynamoDB, Kinesis, and Firehose.
/// </summary>
public static class DataFlowEtlService
{
    private static AmazonS3Client GetS3Client(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonS3Client>(region);

    private static AmazonDynamoDBClient GetDdbClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonDynamoDBClient>(region);

    private static AmazonKinesisFirehoseClient GetFirehoseClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKinesisFirehoseClient>(region);

    /// <summary>
    /// Read an object from S3 (triggered by an S3 event) and write its JSON content
    /// as an item into DynamoDB.
    /// </summary>
    public static async Task<S3EventToDynamoDbResult> S3EventToDynamoDbAsync(
        string bucket,
        string key,
        string tableName,
        string partitionKeyName = "pk",
        string? sortKeyName = null,
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);
        var ddb = GetDdbClient(region);

        try
        {
            var getResp = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            });

            using var reader = new StreamReader(getResp.ResponseStream);
            var content = await reader.ReadToEndAsync();
            var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(content)
                       ?? new Dictionary<string, JsonElement>();

            var item = new Dictionary<string, AttributeValue>();
            foreach (var kv in data)
            {
                item[kv.Key] = new AttributeValue { S = kv.Value.ToString() };
            }

            if (!item.ContainsKey(partitionKeyName))
                item[partitionKeyName] = new AttributeValue { S = key };

            await ddb.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = item
            });

            return new S3EventToDynamoDbResult(
                Bucket: bucket,
                Key: key,
                TableName: tableName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to process S3 event s3://{bucket}/{key} to DynamoDB table '{tableName}'");
        }
    }

    /// <summary>
    /// Archive DynamoDB stream records to S3 as newline-delimited JSON.
    /// </summary>
    public static async Task<DynamoDbStreamToS3ArchiveResult> DynamoDbStreamToS3ArchiveAsync(
        List<Dictionary<string, AttributeValue>> records,
        string bucket,
        string keyPrefix,
        string? archiveKey = null,
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);

        try
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ");
            var finalKey = archiveKey ?? $"{keyPrefix.TrimEnd('/')}/{timestamp}.jsonl";

            var sb = new StringBuilder();
            foreach (var record in records)
            {
                var dict = new Dictionary<string, string>();
                foreach (var kv in record)
                {
                    dict[kv.Key] = kv.Value.S ?? kv.Value.N ?? kv.Value.BOOL.ToString();
                }
                sb.AppendLine(JsonSerializer.Serialize(dict));
            }

            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = finalKey,
                ContentBody = sb.ToString(),
                ContentType = "application/x-ndjson"
            });

            return new DynamoDbStreamToS3ArchiveResult(
                RecordsArchived: records.Count,
                Bucket: bucket,
                KeyPrefix: keyPrefix,
                ArchiveKey: finalKey);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to archive DynamoDB stream records to s3://{bucket}/{keyPrefix}");
        }
    }

    /// <summary>
    /// Bulk load a CSV file from S3 into a DynamoDB table using BatchWriteItem.
    /// The first line of the CSV is treated as headers.
    /// </summary>
    public static async Task<S3CsvToDynamoDbBulkResult> S3CsvToDynamoDbBulkAsync(
        string bucket,
        string key,
        string tableName,
        int batchSize = 25,
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);
        var ddb = GetDdbClient(region);

        try
        {
            var getResp = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            });

            using var reader = new StreamReader(getResp.ResponseStream);
            var content = await reader.ReadToEndAsync();
            var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);

            if (lines.Length < 2)
                return new S3CsvToDynamoDbBulkResult(TableName: tableName);

            var headers = lines[0].Split(',').Select(h => h.Trim()).ToArray();
            var totalRows = lines.Length - 1;
            var successCount = 0;
            var failureCount = 0;

            var batch = new List<WriteRequest>();
            for (var i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');
                var item = new Dictionary<string, AttributeValue>();
                for (var j = 0; j < headers.Length && j < values.Length; j++)
                {
                    item[headers[j]] = new AttributeValue { S = values[j].Trim() };
                }

                batch.Add(new WriteRequest
                {
                    PutRequest = new PutRequest { Item = item }
                });

                if (batch.Count >= batchSize || i == lines.Length - 1)
                {
                    try
                    {
                        await ddb.BatchWriteItemAsync(new BatchWriteItemRequest
                        {
                            RequestItems = new Dictionary<string, List<WriteRequest>>
                            {
                                [tableName] = new List<WriteRequest>(batch)
                            }
                        });
                        successCount += batch.Count;
                    }
                    catch (Exception)
                    {
                        failureCount += batch.Count;
                    }
                    batch.Clear();
                }
            }

            return new S3CsvToDynamoDbBulkResult(
                TotalRows: totalRows,
                SuccessCount: successCount,
                FailureCount: failureCount,
                TableName: tableName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to bulk load CSV s3://{bucket}/{key} to DynamoDB table '{tableName}'");
        }
    }

    /// <summary>
    /// Transform a list of raw data records and put them into a Firehose delivery stream.
    /// </summary>
    public static async Task<KinesisToFirehoseTransformerResult> KinesisToFirehoseTransformerAsync(
        string deliveryStreamName,
        List<string> records,
        Func<string, string>? transformer = null,
        RegionEndpoint? region = null)
    {
        var firehose = GetFirehoseClient(region);

        try
        {
            var transformed = new List<Amazon.KinesisFirehose.Model.Record>();
            var failedCount = 0;

            foreach (var raw in records)
            {
                try
                {
                    var data = transformer != null ? transformer(raw) : raw;
                    transformed.Add(new Amazon.KinesisFirehose.Model.Record
                    {
                        Data = new MemoryStream(Encoding.UTF8.GetBytes(data))
                    });
                }
                catch (Exception)
                {
                    failedCount++;
                }
            }

            if (transformed.Count > 0)
            {
                await firehose.PutRecordBatchAsync(new PutRecordBatchRequest
                {
                    DeliveryStreamName = deliveryStreamName,
                    Records = transformed
                });
            }

            return new KinesisToFirehoseTransformerResult(
                RecordsTransformed: transformed.Count,
                RecordsFailed: failedCount,
                DeliveryStreamName: deliveryStreamName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to transform records for Firehose stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Replicate an S3 object from one region to another.
    /// </summary>
    public static async Task<CrossRegionS3ReplicatorResult> CrossRegionS3ReplicatorAsync(
        string sourceBucket,
        string sourceKey,
        string destinationBucket,
        string? destinationKey = null,
        RegionEndpoint? sourceRegion = null,
        RegionEndpoint? destinationRegion = null)
    {
        var srcClient = GetS3Client(sourceRegion);
        var dstClient = GetS3Client(destinationRegion);
        var destKey = destinationKey ?? sourceKey;

        try
        {
            var getResp = await srcClient.GetObjectAsync(new GetObjectRequest
            {
                BucketName = sourceBucket,
                Key = sourceKey
            });

            await dstClient.PutObjectAsync(new PutObjectRequest
            {
                BucketName = destinationBucket,
                Key = destKey,
                InputStream = getResp.ResponseStream,
                ContentType = getResp.Headers.ContentType
            });

            return new CrossRegionS3ReplicatorResult(
                SourceBucket: sourceBucket,
                SourceKey: sourceKey,
                DestinationBucket: destinationBucket,
                DestinationKey: destKey);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to replicate s3://{sourceBucket}/{sourceKey} to " +
                $"s3://{destinationBucket}/{destKey}");
        }
    }

    /// <summary>
    /// Track ETL job status in a DynamoDB table.
    /// </summary>
    public static async Task<EtlStatusTrackerResult> EtlStatusTrackerAsync(
        string tableName,
        string jobId,
        string status,
        Dictionary<string, string>? metadata = null,
        RegionEndpoint? region = null)
    {
        var ddb = GetDdbClient(region);
        var now = DateTime.UtcNow;

        try
        {
            var item = new Dictionary<string, AttributeValue>
            {
                ["jobId"] = new AttributeValue { S = jobId },
                ["status"] = new AttributeValue { S = status },
                ["updatedAt"] = new AttributeValue { S = now.ToString("o") }
            };

            if (metadata != null)
            {
                foreach (var kv in metadata)
                {
                    item[kv.Key] = new AttributeValue { S = kv.Value };
                }
            }

            await ddb.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = item
            });

            return new EtlStatusTrackerResult(
                JobId: jobId,
                Status: status,
                TableName: tableName,
                UpdatedAt: now);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to track ETL status for job '{jobId}' in table '{tableName}'");
        }
    }

    /// <summary>
    /// Manage an S3 multipart upload: create, upload parts from a stream, and complete.
    /// </summary>
    public static async Task<S3MultipartUploadManagerResult> S3MultipartUploadManagerAsync(
        string bucket,
        string key,
        Stream content,
        long partSizeBytes = 5 * 1024 * 1024,
        string? contentType = null,
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);

        try
        {
            var initResp = await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest
                {
                    BucketName = bucket,
                    Key = key,
                    ContentType = contentType
                });

            var uploadId = initResp.UploadId;
            var partETags = new List<PartETag>();
            var partNumber = 1;
            var buffer = new byte[partSizeBytes];

            try
            {
                int bytesRead;
                while ((bytesRead = await content.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    using var partStream = new MemoryStream(buffer, 0, bytesRead);
                    var uploadResp = await s3.UploadPartAsync(new UploadPartRequest
                    {
                        BucketName = bucket,
                        Key = key,
                        UploadId = uploadId,
                        PartNumber = partNumber,
                        InputStream = partStream
                    });
                    partETags.Add(new PartETag(partNumber, uploadResp.ETag));
                    partNumber++;
                }

                var completeResp = await s3.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = bucket,
                        Key = key,
                        UploadId = uploadId,
                        PartETags = partETags
                    });

                return new S3MultipartUploadManagerResult(
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                    ETag: completeResp.ETag,
                    Location: completeResp.Location,
                    Completed: true);
            }
            catch (Exception)
            {
                await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                {
                    BucketName = bucket,
                    Key = key,
                    UploadId = uploadId
                });
                throw;
            }
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to manage multipart upload for s3://{bucket}/{key}");
        }
    }

    /// <summary>
    /// Create date-based partition prefixes in an S3 data lake bucket.
    /// </summary>
    public static async Task<DataLakePartitionManagerResult> DataLakePartitionManagerAsync(
        string bucket,
        string prefix,
        DateTime startDate,
        DateTime endDate,
        string partitionFormat = "year={0:yyyy}/month={0:MM}/day={0:dd}",
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);

        try
        {
            var paths = new List<string>();
            var current = startDate.Date;
            var end = endDate.Date;

            while (current <= end)
            {
                var partitionPath =
                    $"{prefix.TrimEnd('/')}/{string.Format(partitionFormat, current)}/";
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = partitionPath,
                    ContentBody = string.Empty
                });
                paths.Add(partitionPath);
                current = current.AddDays(1);
            }

            return new DataLakePartitionManagerResult(
                Bucket: bucket,
                Prefix: prefix,
                PartitionsCreated: paths.Count,
                PartitionPaths: paths);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to manage data lake partitions in s3://{bucket}/{prefix}");
        }
    }

    /// <summary>
    /// Detect and repair missing date-based partitions in an S3 data lake.
    /// </summary>
    public static async Task<RepairPartitionsResult> RepairPartitionsAsync(
        string bucket,
        string prefix,
        DateTime startDate,
        DateTime endDate,
        string partitionFormat = "year={0:yyyy}/month={0:MM}/day={0:dd}",
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);

        try
        {
            var missing = new List<string>();
            var current = startDate.Date;
            var end = endDate.Date;

            while (current <= end)
            {
                var partitionPath =
                    $"{prefix.TrimEnd('/')}/{string.Format(partitionFormat, current)}/";

                var listResp = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = bucket,
                    Prefix = partitionPath,
                    MaxKeys = 1
                });

                if (listResp.KeyCount == 0)
                    missing.Add(partitionPath);

                current = current.AddDays(1);
            }

            var repaired = new List<string>();
            foreach (var path in missing)
            {
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = path,
                    ContentBody = string.Empty
                });
                repaired.Add(path);
            }

            return new RepairPartitionsResult(
                PartitionsRepaired: repaired.Count,
                PartitionsMissing: missing.Count,
                RepairedPaths: repaired);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to repair partitions in s3://{bucket}/{prefix}");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="S3EventToDynamoDbAsync"/>.</summary>
    public static S3EventToDynamoDbResult S3EventToDynamoDb(string bucket, string key, string tableName, string partitionKeyName = "pk", string? sortKeyName = null, RegionEndpoint? region = null)
        => S3EventToDynamoDbAsync(bucket, key, tableName, partitionKeyName, sortKeyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DynamoDbStreamToS3ArchiveAsync"/>.</summary>
    public static DynamoDbStreamToS3ArchiveResult DynamoDbStreamToS3Archive(List<Dictionary<string, AttributeValue>> records, string bucket, string keyPrefix, string? archiveKey = null, RegionEndpoint? region = null)
        => DynamoDbStreamToS3ArchiveAsync(records, bucket, keyPrefix, archiveKey, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="S3CsvToDynamoDbBulkAsync"/>.</summary>
    public static S3CsvToDynamoDbBulkResult S3CsvToDynamoDbBulk(string bucket, string key, string tableName, int batchSize = 25, RegionEndpoint? region = null)
        => S3CsvToDynamoDbBulkAsync(bucket, key, tableName, batchSize, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="KinesisToFirehoseTransformerAsync"/>.</summary>
    public static KinesisToFirehoseTransformerResult KinesisToFirehoseTransformer(string deliveryStreamName, List<string> records, Func<string, string>? transformer = null, RegionEndpoint? region = null)
        => KinesisToFirehoseTransformerAsync(deliveryStreamName, records, transformer, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CrossRegionS3ReplicatorAsync"/>.</summary>
    public static CrossRegionS3ReplicatorResult CrossRegionS3Replicator(string sourceBucket, string sourceKey, string destinationBucket, string? destinationKey = null, RegionEndpoint? sourceRegion = null, RegionEndpoint? destinationRegion = null)
        => CrossRegionS3ReplicatorAsync(sourceBucket, sourceKey, destinationBucket, destinationKey, sourceRegion, destinationRegion).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EtlStatusTrackerAsync"/>.</summary>
    public static EtlStatusTrackerResult EtlStatusTracker(string tableName, string jobId, string status, Dictionary<string, string>? metadata = null, RegionEndpoint? region = null)
        => EtlStatusTrackerAsync(tableName, jobId, status, metadata, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="S3MultipartUploadManagerAsync"/>.</summary>
    public static S3MultipartUploadManagerResult S3MultipartUploadManager(string bucket, string key, Stream content, long partSizeBytes = 5 * 1024 * 1024, string? contentType = null, RegionEndpoint? region = null)
        => S3MultipartUploadManagerAsync(bucket, key, content, partSizeBytes, contentType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DataLakePartitionManagerAsync"/>.</summary>
    public static DataLakePartitionManagerResult DataLakePartitionManager(string bucket, string prefix, DateTime startDate, DateTime endDate, string partitionFormat = "year={0:yyyy}/month={0:MM}/day={0:dd}", RegionEndpoint? region = null)
        => DataLakePartitionManagerAsync(bucket, prefix, startDate, endDate, partitionFormat, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RepairPartitionsAsync"/>.</summary>
    public static RepairPartitionsResult RepairPartitions(string bucket, string prefix, DateTime startDate, DateTime endDate, string partitionFormat = "year={0:yyyy}/month={0:MM}/day={0:dd}", RegionEndpoint? region = null)
        => RepairPartitionsAsync(bucket, prefix, startDate, endDate, partitionFormat, region).GetAwaiter().GetResult();

}
