using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateStreamResult(string? StreamName = null);

public sealed record DeleteStreamResult(string? StreamName = null);

public sealed record DescribeStreamResult(
    string? StreamName = null,
    string? StreamARN = null,
    string? StreamStatus = null,
    int? RetentionPeriodHours = null,
    List<Shard>? Shards = null,
    bool? HasMoreShards = null,
    string? EncryptionType = null,
    string? KeyId = null,
    string? StreamModeDetails = null);

public sealed record DescribeStreamSummaryResult(
    string? StreamName = null,
    string? StreamARN = null,
    string? StreamStatus = null,
    string? StreamModeDetails = null,
    int? RetentionPeriodHours = null,
    int? OpenShardCount = null,
    int? ConsumerCount = null,
    DateTime? StreamCreationTimestamp = null,
    string? EncryptionType = null,
    string? KeyId = null);

public sealed record ListStreamsResult(
    List<string>? StreamNames = null,
    bool? HasMoreStreams = null,
    List<StreamSummary>? StreamSummaries = null,
    string? NextToken = null);

public sealed record PutRecordResult(
    string? ShardId = null,
    string? SequenceNumber = null,
    string? EncryptionType = null);

public sealed record PutRecordsResult(
    int? FailedRecordCount = null,
    List<PutRecordsResultEntry>? Records = null,
    string? EncryptionType = null);

public sealed record GetRecordsResult(
    List<Record>? Records = null,
    string? NextShardIterator = null,
    long? MillisBehindLatest = null,
    List<ChildShard>? ChildShards = null);

public sealed record GetShardIteratorResult(
    string? ShardIterator = null);

public sealed record ListShardsResult(
    List<Shard>? Shards = null,
    string? NextToken = null);

public sealed record MergeShardsResult(string? StreamName = null);

public sealed record SplitShardResult(string? StreamName = null);

public sealed record UpdateShardCountResult(
    string? StreamName = null,
    string? StreamARN = null,
    int? CurrentShardCount = null,
    int? TargetShardCount = null);

public sealed record IncreaseStreamRetentionPeriodResult(string? StreamName = null);

public sealed record DecreaseStreamRetentionPeriodResult(string? StreamName = null);

public sealed record EnableEnhancedMonitoringResult(
    string? StreamName = null,
    string? StreamARN = null,
    List<string>? CurrentShardLevelMetrics = null,
    List<string>? DesiredShardLevelMetrics = null);

public sealed record DisableEnhancedMonitoringResult(
    string? StreamName = null,
    string? StreamARN = null,
    List<string>? CurrentShardLevelMetrics = null,
    List<string>? DesiredShardLevelMetrics = null);

public sealed record ListTagsForStreamResult(
    List<Amazon.Kinesis.Model.Tag>? Tags = null,
    bool? HasMoreTags = null);

public sealed record RegisterStreamConsumerResult(
    string? ConsumerName = null,
    string? ConsumerARN = null,
    string? ConsumerStatus = null,
    DateTime? ConsumerCreationTimestamp = null);

public sealed record DescribeStreamConsumerResult(
    string? ConsumerName = null,
    string? ConsumerARN = null,
    string? ConsumerStatus = null,
    DateTime? ConsumerCreationTimestamp = null,
    string? StreamARN = null);

public sealed record ListStreamConsumersResult(
    List<Consumer>? Consumers = null,
    string? NextToken = null);

public sealed record UpdateStreamModeResult(string? StreamARN = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Kinesis Data Streams.
/// </summary>
public static class KinesisService
{
    private static AmazonKinesisClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKinesisClient>(region);

    /// <summary>
    /// Create a new Kinesis stream.
    /// </summary>
    public static async Task<CreateStreamResult> CreateStreamAsync(
        string streamName,
        int? shardCount = null,
        StreamModeDetails? streamModeDetails = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateStreamRequest { StreamName = streamName };
        if (shardCount.HasValue) request.ShardCount = shardCount.Value;
        if (streamModeDetails != null) request.StreamModeDetails = streamModeDetails;

        try
        {
            await client.CreateStreamAsync(request);
            return new CreateStreamResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create stream '{streamName}'");
        }
    }

    /// <summary>
    /// Delete a Kinesis stream.
    /// </summary>
    public static async Task<DeleteStreamResult> DeleteStreamAsync(
        string streamName,
        string? streamARN = null,
        bool? enforceConsumerDeletion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteStreamRequest { StreamName = streamName };
        if (streamARN != null) request.StreamARN = streamARN;
        if (enforceConsumerDeletion.HasValue)
            request.EnforceConsumerDeletion = enforceConsumerDeletion.Value;

        try
        {
            await client.DeleteStreamAsync(request);
            return new DeleteStreamResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete stream '{streamName}'");
        }
    }

    /// <summary>
    /// Describe a Kinesis stream.
    /// </summary>
    public static async Task<DescribeStreamResult> DescribeStreamAsync(
        string streamName,
        string? streamARN = null,
        int? limit = null,
        string? exclusiveStartShardId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStreamRequest { StreamName = streamName };
        if (streamARN != null) request.StreamARN = streamARN;
        if (limit.HasValue) request.Limit = limit.Value;
        if (exclusiveStartShardId != null)
            request.ExclusiveStartShardId = exclusiveStartShardId;

        try
        {
            var resp = await client.DescribeStreamAsync(request);
            var d = resp.StreamDescription;
            return new DescribeStreamResult(
                StreamName: d.StreamName,
                StreamARN: d.StreamARN,
                StreamStatus: d.StreamStatus?.Value,
                RetentionPeriodHours: d.RetentionPeriodHours,
                Shards: d.Shards,
                HasMoreShards: d.HasMoreShards,
                EncryptionType: d.EncryptionType?.Value,
                KeyId: d.KeyId,
                StreamModeDetails: d.StreamModeDetails?.StreamMode?.Value);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe stream '{streamName}'");
        }
    }

    /// <summary>
    /// Describe a Kinesis stream summary.
    /// </summary>
    public static async Task<DescribeStreamSummaryResult> DescribeStreamSummaryAsync(
        string streamName,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStreamSummaryRequest { StreamName = streamName };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.DescribeStreamSummaryAsync(request);
            var d = resp.StreamDescriptionSummary;
            return new DescribeStreamSummaryResult(
                StreamName: d.StreamName,
                StreamARN: d.StreamARN,
                StreamStatus: d.StreamStatus?.Value,
                StreamModeDetails: d.StreamModeDetails?.StreamMode?.Value,
                RetentionPeriodHours: d.RetentionPeriodHours,
                OpenShardCount: d.OpenShardCount,
                ConsumerCount: d.ConsumerCount,
                StreamCreationTimestamp: d.StreamCreationTimestamp,
                EncryptionType: d.EncryptionType?.Value,
                KeyId: d.KeyId);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe stream summary for '{streamName}'");
        }
    }

    /// <summary>
    /// List Kinesis streams.
    /// </summary>
    public static async Task<ListStreamsResult> ListStreamsAsync(
        int? limit = null,
        string? exclusiveStartStreamName = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStreamsRequest();
        if (limit.HasValue) request.Limit = limit.Value;
        if (exclusiveStartStreamName != null)
            request.ExclusiveStartStreamName = exclusiveStartStreamName;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStreamsAsync(request);
            return new ListStreamsResult(
                StreamNames: resp.StreamNames,
                HasMoreStreams: resp.HasMoreStreams,
                StreamSummaries: resp.StreamSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list streams");
        }
    }

    /// <summary>
    /// Put a single record into a Kinesis stream.
    /// </summary>
    public static async Task<PutRecordResult> PutRecordAsync(
        string streamName,
        MemoryStream data,
        string partitionKey,
        string? streamARN = null,
        string? explicitHashKey = null,
        string? sequenceNumberForOrdering = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutRecordRequest
        {
            StreamName = streamName,
            Data = data,
            PartitionKey = partitionKey
        };
        if (streamARN != null) request.StreamARN = streamARN;
        if (explicitHashKey != null) request.ExplicitHashKey = explicitHashKey;
        if (sequenceNumberForOrdering != null)
            request.SequenceNumberForOrdering = sequenceNumberForOrdering;

        try
        {
            var resp = await client.PutRecordAsync(request);
            return new PutRecordResult(
                ShardId: resp.ShardId,
                SequenceNumber: resp.SequenceNumber,
                EncryptionType: resp.EncryptionType?.Value);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put record to stream '{streamName}'");
        }
    }

    /// <summary>
    /// Put multiple records into a Kinesis stream.
    /// </summary>
    public static async Task<PutRecordsResult> PutRecordsAsync(
        string streamName,
        List<PutRecordsRequestEntry> records,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutRecordsRequest
        {
            StreamName = streamName,
            Records = records
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.PutRecordsAsync(request);
            return new PutRecordsResult(
                FailedRecordCount: resp.FailedRecordCount,
                Records: resp.Records,
                EncryptionType: resp.EncryptionType?.Value);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put records to stream '{streamName}'");
        }
    }

    /// <summary>
    /// Get records from a Kinesis shard iterator.
    /// </summary>
    public static async Task<GetRecordsResult> GetRecordsAsync(
        string shardIterator,
        int? limit = null,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRecordsRequest { ShardIterator = shardIterator };
        if (limit.HasValue) request.Limit = limit.Value;
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.GetRecordsAsync(request);
            return new GetRecordsResult(
                Records: resp.Records,
                NextShardIterator: resp.NextShardIterator,
                MillisBehindLatest: resp.MillisBehindLatest,
                ChildShards: resp.ChildShards);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get records");
        }
    }

    /// <summary>
    /// Get a shard iterator for reading from a Kinesis stream.
    /// </summary>
    public static async Task<GetShardIteratorResult> GetShardIteratorAsync(
        string streamName,
        string shardId,
        string shardIteratorType,
        string? streamARN = null,
        string? startingSequenceNumber = null,
        DateTime? timestamp = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetShardIteratorRequest
        {
            StreamName = streamName,
            ShardId = shardId,
            ShardIteratorType = new ShardIteratorType(shardIteratorType)
        };
        if (streamARN != null) request.StreamARN = streamARN;
        if (startingSequenceNumber != null)
            request.StartingSequenceNumber = startingSequenceNumber;
        if (timestamp.HasValue) request.Timestamp = timestamp.Value;

        try
        {
            var resp = await client.GetShardIteratorAsync(request);
            return new GetShardIteratorResult(ShardIterator: resp.ShardIterator);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get shard iterator for stream '{streamName}'");
        }
    }

    /// <summary>
    /// List shards in a Kinesis stream.
    /// </summary>
    public static async Task<ListShardsResult> ListShardsAsync(
        string? streamName = null,
        string? streamARN = null,
        string? nextToken = null,
        string? exclusiveStartShardId = null,
        int? maxResults = null,
        DateTime? streamCreationTimestamp = null,
        ShardFilter? shardFilter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListShardsRequest();
        if (streamName != null) request.StreamName = streamName;
        if (streamARN != null) request.StreamARN = streamARN;
        if (nextToken != null) request.NextToken = nextToken;
        if (exclusiveStartShardId != null)
            request.ExclusiveStartShardId = exclusiveStartShardId;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (streamCreationTimestamp.HasValue)
            request.StreamCreationTimestamp = streamCreationTimestamp.Value;
        if (shardFilter != null) request.ShardFilter = shardFilter;

        try
        {
            var resp = await client.ListShardsAsync(request);
            return new ListShardsResult(
                Shards: resp.Shards,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list shards");
        }
    }

    /// <summary>
    /// Merge two adjacent shards in a Kinesis stream.
    /// </summary>
    public static async Task<MergeShardsResult> MergeShardsAsync(
        string streamName,
        string shardToMerge,
        string adjacentShardToMerge,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new MergeShardsRequest
        {
            StreamName = streamName,
            ShardToMerge = shardToMerge,
            AdjacentShardToMerge = adjacentShardToMerge
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.MergeShardsAsync(request);
            return new MergeShardsResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to merge shards in stream '{streamName}'");
        }
    }

    /// <summary>
    /// Split a shard in a Kinesis stream.
    /// </summary>
    public static async Task<SplitShardResult> SplitShardAsync(
        string streamName,
        string shardToSplit,
        string newStartingHashKey,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SplitShardRequest
        {
            StreamName = streamName,
            ShardToSplit = shardToSplit,
            NewStartingHashKey = newStartingHashKey
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.SplitShardAsync(request);
            return new SplitShardResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to split shard in stream '{streamName}'");
        }
    }

    /// <summary>
    /// Update the shard count of a Kinesis stream.
    /// </summary>
    public static async Task<UpdateShardCountResult> UpdateShardCountAsync(
        string streamName,
        int targetShardCount,
        string scalingType = "UNIFORM_SCALING",
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateShardCountRequest
        {
            StreamName = streamName,
            TargetShardCount = targetShardCount,
            ScalingType = new ScalingType(scalingType)
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.UpdateShardCountAsync(request);
            return new UpdateShardCountResult(
                StreamName: resp.StreamName,
                StreamARN: resp.StreamARN,
                CurrentShardCount: resp.CurrentShardCount,
                TargetShardCount: resp.TargetShardCount);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update shard count for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Increase the retention period of a Kinesis stream.
    /// </summary>
    public static async Task<IncreaseStreamRetentionPeriodResult> IncreaseStreamRetentionPeriodAsync(
        string streamName,
        int retentionPeriodHours,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new IncreaseStreamRetentionPeriodRequest
        {
            StreamName = streamName,
            RetentionPeriodHours = retentionPeriodHours
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.IncreaseStreamRetentionPeriodAsync(request);
            return new IncreaseStreamRetentionPeriodResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to increase retention period for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Decrease the retention period of a Kinesis stream.
    /// </summary>
    public static async Task<DecreaseStreamRetentionPeriodResult> DecreaseStreamRetentionPeriodAsync(
        string streamName,
        int retentionPeriodHours,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DecreaseStreamRetentionPeriodRequest
        {
            StreamName = streamName,
            RetentionPeriodHours = retentionPeriodHours
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.DecreaseStreamRetentionPeriodAsync(request);
            return new DecreaseStreamRetentionPeriodResult(StreamName: streamName);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to decrease retention period for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Enable enhanced monitoring for a Kinesis stream.
    /// </summary>
    public static async Task<EnableEnhancedMonitoringResult> EnableEnhancedMonitoringAsync(
        string streamName,
        List<string> shardLevelMetrics,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableEnhancedMonitoringRequest
        {
            StreamName = streamName,
            ShardLevelMetrics = shardLevelMetrics
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.EnableEnhancedMonitoringAsync(request);
            return new EnableEnhancedMonitoringResult(
                StreamName: resp.StreamName,
                StreamARN: resp.StreamARN,
                CurrentShardLevelMetrics: resp.CurrentShardLevelMetrics,
                DesiredShardLevelMetrics: resp.DesiredShardLevelMetrics);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to enable enhanced monitoring for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Disable enhanced monitoring for a Kinesis stream.
    /// </summary>
    public static async Task<DisableEnhancedMonitoringResult> DisableEnhancedMonitoringAsync(
        string streamName,
        List<string> shardLevelMetrics,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DisableEnhancedMonitoringRequest
        {
            StreamName = streamName,
            ShardLevelMetrics = shardLevelMetrics
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            var resp = await client.DisableEnhancedMonitoringAsync(request);
            return new DisableEnhancedMonitoringResult(
                StreamName: resp.StreamName,
                StreamARN: resp.StreamARN,
                CurrentShardLevelMetrics: resp.CurrentShardLevelMetrics,
                DesiredShardLevelMetrics: resp.DesiredShardLevelMetrics);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to disable enhanced monitoring for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Add tags to a Kinesis stream.
    /// </summary>
    public static async Task AddTagsToStreamAsync(
        string streamName,
        Dictionary<string, string> tags,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AddTagsToStreamRequest
        {
            StreamName = streamName,
            Tags = tags
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.AddTagsToStreamAsync(request);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to add tags to stream '{streamName}'");
        }
    }

    /// <summary>
    /// Remove tags from a Kinesis stream.
    /// </summary>
    public static async Task RemoveTagsFromStreamAsync(
        string streamName,
        List<string> tagKeys,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemoveTagsFromStreamRequest
        {
            StreamName = streamName,
            TagKeys = tagKeys
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.RemoveTagsFromStreamAsync(request);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to remove tags from stream '{streamName}'");
        }
    }

    /// <summary>
    /// List tags for a Kinesis stream.
    /// </summary>
    public static async Task<ListTagsForStreamResult> ListTagsForStreamAsync(
        string streamName,
        string? streamARN = null,
        string? exclusiveStartTagKey = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForStreamRequest { StreamName = streamName };
        if (streamARN != null) request.StreamARN = streamARN;
        if (exclusiveStartTagKey != null)
            request.ExclusiveStartTagKey = exclusiveStartTagKey;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTagsForStreamAsync(request);
            return new ListTagsForStreamResult(
                Tags: resp.Tags,
                HasMoreTags: resp.HasMoreTags);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list tags for stream '{streamName}'");
        }
    }

    /// <summary>
    /// Register a stream consumer.
    /// </summary>
    public static async Task<RegisterStreamConsumerResult> RegisterStreamConsumerAsync(
        string streamARN,
        string consumerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RegisterStreamConsumerAsync(new RegisterStreamConsumerRequest
            {
                StreamARN = streamARN,
                ConsumerName = consumerName
            });
            var c = resp.Consumer;
            return new RegisterStreamConsumerResult(
                ConsumerName: c.ConsumerName,
                ConsumerARN: c.ConsumerARN,
                ConsumerStatus: c.ConsumerStatus?.Value,
                ConsumerCreationTimestamp: c.ConsumerCreationTimestamp);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to register stream consumer '{consumerName}'");
        }
    }

    /// <summary>
    /// Deregister a stream consumer.
    /// </summary>
    public static async Task DeregisterStreamConsumerAsync(
        string? streamARN = null,
        string? consumerName = null,
        string? consumerARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeregisterStreamConsumerRequest();
        if (streamARN != null) request.StreamARN = streamARN;
        if (consumerName != null) request.ConsumerName = consumerName;
        if (consumerARN != null) request.ConsumerARN = consumerARN;

        try
        {
            await client.DeregisterStreamConsumerAsync(request);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to deregister stream consumer");
        }
    }

    /// <summary>
    /// Describe a stream consumer.
    /// </summary>
    public static async Task<DescribeStreamConsumerResult> DescribeStreamConsumerAsync(
        string? streamARN = null,
        string? consumerName = null,
        string? consumerARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStreamConsumerRequest();
        if (streamARN != null) request.StreamARN = streamARN;
        if (consumerName != null) request.ConsumerName = consumerName;
        if (consumerARN != null) request.ConsumerARN = consumerARN;

        try
        {
            var resp = await client.DescribeStreamConsumerAsync(request);
            var d = resp.ConsumerDescription;
            return new DescribeStreamConsumerResult(
                ConsumerName: d.ConsumerName,
                ConsumerARN: d.ConsumerARN,
                ConsumerStatus: d.ConsumerStatus?.Value,
                ConsumerCreationTimestamp: d.ConsumerCreationTimestamp,
                StreamARN: d.StreamARN);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe stream consumer");
        }
    }

    /// <summary>
    /// List stream consumers.
    /// </summary>
    public static async Task<ListStreamConsumersResult> ListStreamConsumersAsync(
        string streamARN,
        string? nextToken = null,
        int? maxResults = null,
        DateTime? streamCreationTimestamp = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStreamConsumersRequest { StreamARN = streamARN };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (streamCreationTimestamp.HasValue)
            request.StreamCreationTimestamp = streamCreationTimestamp.Value;

        try
        {
            var resp = await client.ListStreamConsumersAsync(request);
            return new ListStreamConsumersResult(
                Consumers: resp.Consumers,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list stream consumers");
        }
    }

    /// <summary>
    /// Start stream encryption.
    /// </summary>
    public static async Task StartStreamEncryptionAsync(
        string streamName,
        string encryptionType,
        string keyId,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartStreamEncryptionRequest
        {
            StreamName = streamName,
            EncryptionType = new EncryptionType(encryptionType),
            KeyId = keyId
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.StartStreamEncryptionAsync(request);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start stream encryption for '{streamName}'");
        }
    }

    /// <summary>
    /// Stop stream encryption.
    /// </summary>
    public static async Task StopStreamEncryptionAsync(
        string streamName,
        string encryptionType,
        string keyId,
        string? streamARN = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopStreamEncryptionRequest
        {
            StreamName = streamName,
            EncryptionType = new EncryptionType(encryptionType),
            KeyId = keyId
        };
        if (streamARN != null) request.StreamARN = streamARN;

        try
        {
            await client.StopStreamEncryptionAsync(request);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop stream encryption for '{streamName}'");
        }
    }

    /// <summary>
    /// Update the stream mode of a Kinesis stream.
    /// </summary>
    public static async Task<UpdateStreamModeResult> UpdateStreamModeAsync(
        string streamARN,
        StreamModeDetails streamModeDetails,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateStreamModeAsync(new UpdateStreamModeRequest
            {
                StreamARN = streamARN,
                StreamModeDetails = streamModeDetails
            });
            return new UpdateStreamModeResult(StreamARN: streamARN);
        }
        catch (AmazonKinesisException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update stream mode");
        }
    }
}
