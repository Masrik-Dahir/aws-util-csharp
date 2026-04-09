using Amazon;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateDeliveryStreamResult(
    string? DeliveryStreamARN = null);

public sealed record DeleteDeliveryStreamResult(
    string? DeliveryStreamName = null);

public sealed record DescribeDeliveryStreamResult(
    string? DeliveryStreamName = null,
    string? DeliveryStreamARN = null,
    string? DeliveryStreamStatus = null,
    string? DeliveryStreamType = null,
    string? VersionId = null,
    DateTime? CreateTimestamp = null,
    DateTime? LastUpdateTimestamp = null,
    List<DestinationDescription>? Destinations = null,
    bool? HasMoreDestinations = null);

public sealed record ListDeliveryStreamsResult(
    List<string>? DeliveryStreamNames = null,
    bool? HasMoreDeliveryStreams = null);

public sealed record FirehosePutRecordResult(
    string? RecordId = null,
    bool? Encrypted = null);

public sealed record PutRecordBatchResult(
    int? FailedPutCount = null,
    bool? Encrypted = null,
    List<PutRecordBatchResponseEntry>? RequestResponses = null);

public sealed record UpdateDestinationResult(
    string? DeliveryStreamName = null);

public sealed record StartDeliveryStreamEncryptionResult(
    string? DeliveryStreamName = null);

public sealed record StopDeliveryStreamEncryptionResult(
    string? DeliveryStreamName = null);

public sealed record ListTagsForDeliveryStreamResult(
    List<Amazon.KinesisFirehose.Model.Tag>? Tags = null,
    bool? HasMoreTags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Kinesis Data Firehose.
/// </summary>
public static class FirehoseService
{
    private static AmazonKinesisFirehoseClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKinesisFirehoseClient>(region);

    /// <summary>
    /// Create a new Firehose delivery stream.
    /// </summary>
    public static async Task<CreateDeliveryStreamResult> CreateDeliveryStreamAsync(
        CreateDeliveryStreamRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDeliveryStreamAsync(request);
            return new CreateDeliveryStreamResult(
                DeliveryStreamARN: resp.DeliveryStreamARN);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create delivery stream");
        }
    }

    /// <summary>
    /// Delete a Firehose delivery stream.
    /// </summary>
    public static async Task<DeleteDeliveryStreamResult> DeleteDeliveryStreamAsync(
        string deliveryStreamName,
        bool? allowForceDelete = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDeliveryStreamRequest
        {
            DeliveryStreamName = deliveryStreamName
        };
        if (allowForceDelete.HasValue)
            request.AllowForceDelete = allowForceDelete.Value;

        try
        {
            await client.DeleteDeliveryStreamAsync(request);
            return new DeleteDeliveryStreamResult(
                DeliveryStreamName: deliveryStreamName);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Describe a Firehose delivery stream.
    /// </summary>
    public static async Task<DescribeDeliveryStreamResult> DescribeDeliveryStreamAsync(
        string deliveryStreamName,
        string? exclusiveStartDestinationId = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDeliveryStreamRequest
        {
            DeliveryStreamName = deliveryStreamName
        };
        if (exclusiveStartDestinationId != null)
            request.ExclusiveStartDestinationId = exclusiveStartDestinationId;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.DescribeDeliveryStreamAsync(request);
            var d = resp.DeliveryStreamDescription;
            return new DescribeDeliveryStreamResult(
                DeliveryStreamName: d.DeliveryStreamName,
                DeliveryStreamARN: d.DeliveryStreamARN,
                DeliveryStreamStatus: d.DeliveryStreamStatus?.Value,
                DeliveryStreamType: d.DeliveryStreamType?.Value,
                VersionId: d.VersionId,
                CreateTimestamp: d.CreateTimestamp,
                LastUpdateTimestamp: d.LastUpdateTimestamp,
                Destinations: d.Destinations,
                HasMoreDestinations: d.HasMoreDestinations);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// List Firehose delivery streams.
    /// </summary>
    public static async Task<ListDeliveryStreamsResult> ListDeliveryStreamsAsync(
        string? deliveryStreamType = null,
        string? exclusiveStartDeliveryStreamName = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeliveryStreamsRequest();
        if (deliveryStreamType != null)
            request.DeliveryStreamType = new DeliveryStreamType(deliveryStreamType);
        if (exclusiveStartDeliveryStreamName != null)
            request.ExclusiveStartDeliveryStreamName = exclusiveStartDeliveryStreamName;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListDeliveryStreamsAsync(request);
            return new ListDeliveryStreamsResult(
                DeliveryStreamNames: resp.DeliveryStreamNames,
                HasMoreDeliveryStreams: resp.HasMoreDeliveryStreams);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list delivery streams");
        }
    }

    /// <summary>
    /// Put a single record into a Firehose delivery stream.
    /// </summary>
    public static async Task<FirehosePutRecordResult> PutRecordAsync(
        string deliveryStreamName,
        Amazon.KinesisFirehose.Model.Record record,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutRecordAsync(new PutRecordRequest
            {
                DeliveryStreamName = deliveryStreamName,
                Record = record
            });
            return new FirehosePutRecordResult(
                RecordId: resp.RecordId,
                Encrypted: resp.Encrypted);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put record to delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Put a batch of records into a Firehose delivery stream.
    /// </summary>
    public static async Task<PutRecordBatchResult> PutRecordBatchAsync(
        string deliveryStreamName,
        List<Amazon.KinesisFirehose.Model.Record> records,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutRecordBatchAsync(new PutRecordBatchRequest
            {
                DeliveryStreamName = deliveryStreamName,
                Records = records
            });
            return new PutRecordBatchResult(
                FailedPutCount: resp.FailedPutCount,
                Encrypted: resp.Encrypted,
                RequestResponses: resp.RequestResponses);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put record batch to delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Update the destination of a Firehose delivery stream.
    /// </summary>
    public static async Task<UpdateDestinationResult> UpdateDestinationAsync(
        UpdateDestinationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateDestinationAsync(request);
            return new UpdateDestinationResult(
                DeliveryStreamName: request.DeliveryStreamName);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update destination");
        }
    }

    /// <summary>
    /// Start encryption for a Firehose delivery stream.
    /// </summary>
    public static async Task<StartDeliveryStreamEncryptionResult> StartDeliveryStreamEncryptionAsync(
        string deliveryStreamName,
        DeliveryStreamEncryptionConfigurationInput? encryptionConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartDeliveryStreamEncryptionRequest
        {
            DeliveryStreamName = deliveryStreamName
        };
        if (encryptionConfig != null)
            request.DeliveryStreamEncryptionConfigurationInput = encryptionConfig;

        try
        {
            await client.StartDeliveryStreamEncryptionAsync(request);
            return new StartDeliveryStreamEncryptionResult(
                DeliveryStreamName: deliveryStreamName);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start encryption for delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Stop encryption for a Firehose delivery stream.
    /// </summary>
    public static async Task<StopDeliveryStreamEncryptionResult> StopDeliveryStreamEncryptionAsync(
        string deliveryStreamName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopDeliveryStreamEncryptionAsync(new StopDeliveryStreamEncryptionRequest
            {
                DeliveryStreamName = deliveryStreamName
            });
            return new StopDeliveryStreamEncryptionResult(
                DeliveryStreamName: deliveryStreamName);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop encryption for delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Tag a Firehose delivery stream.
    /// </summary>
    public static async Task TagDeliveryStreamAsync(
        string deliveryStreamName,
        List<Amazon.KinesisFirehose.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagDeliveryStreamAsync(new TagDeliveryStreamRequest
            {
                DeliveryStreamName = deliveryStreamName,
                Tags = tags
            });
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// Untag a Firehose delivery stream.
    /// </summary>
    public static async Task UntagDeliveryStreamAsync(
        string deliveryStreamName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagDeliveryStreamAsync(new UntagDeliveryStreamRequest
            {
                DeliveryStreamName = deliveryStreamName,
                TagKeys = tagKeys
            });
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag delivery stream '{deliveryStreamName}'");
        }
    }

    /// <summary>
    /// List tags for a Firehose delivery stream.
    /// </summary>
    public static async Task<ListTagsForDeliveryStreamResult> ListTagsForDeliveryStreamAsync(
        string deliveryStreamName,
        string? exclusiveStartTagKey = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForDeliveryStreamRequest
        {
            DeliveryStreamName = deliveryStreamName
        };
        if (exclusiveStartTagKey != null)
            request.ExclusiveStartTagKey = exclusiveStartTagKey;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTagsForDeliveryStreamAsync(request);
            return new ListTagsForDeliveryStreamResult(
                Tags: resp.Tags,
                HasMoreTags: resp.HasMoreTags);
        }
        catch (AmazonKinesisFirehoseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list tags for delivery stream '{deliveryStreamName}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateDeliveryStreamAsync"/>.</summary>
    public static CreateDeliveryStreamResult CreateDeliveryStream(CreateDeliveryStreamRequest request, RegionEndpoint? region = null)
        => CreateDeliveryStreamAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDeliveryStreamAsync"/>.</summary>
    public static DeleteDeliveryStreamResult DeleteDeliveryStream(string deliveryStreamName, bool? allowForceDelete = null, RegionEndpoint? region = null)
        => DeleteDeliveryStreamAsync(deliveryStreamName, allowForceDelete, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDeliveryStreamAsync"/>.</summary>
    public static DescribeDeliveryStreamResult DescribeDeliveryStream(string deliveryStreamName, string? exclusiveStartDestinationId = null, int? limit = null, RegionEndpoint? region = null)
        => DescribeDeliveryStreamAsync(deliveryStreamName, exclusiveStartDestinationId, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeliveryStreamsAsync"/>.</summary>
    public static ListDeliveryStreamsResult ListDeliveryStreams(string? deliveryStreamType = null, string? exclusiveStartDeliveryStreamName = null, int? limit = null, RegionEndpoint? region = null)
        => ListDeliveryStreamsAsync(deliveryStreamType, exclusiveStartDeliveryStreamName, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutRecordAsync"/>.</summary>
    public static FirehosePutRecordResult PutRecord(string deliveryStreamName, Amazon.KinesisFirehose.Model.Record record, RegionEndpoint? region = null)
        => PutRecordAsync(deliveryStreamName, record, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutRecordBatchAsync"/>.</summary>
    public static PutRecordBatchResult PutRecordBatch(string deliveryStreamName, List<Amazon.KinesisFirehose.Model.Record> records, RegionEndpoint? region = null)
        => PutRecordBatchAsync(deliveryStreamName, records, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDestinationAsync"/>.</summary>
    public static UpdateDestinationResult UpdateDestination(UpdateDestinationRequest request, RegionEndpoint? region = null)
        => UpdateDestinationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartDeliveryStreamEncryptionAsync"/>.</summary>
    public static StartDeliveryStreamEncryptionResult StartDeliveryStreamEncryption(string deliveryStreamName, DeliveryStreamEncryptionConfigurationInput? encryptionConfig = null, RegionEndpoint? region = null)
        => StartDeliveryStreamEncryptionAsync(deliveryStreamName, encryptionConfig, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopDeliveryStreamEncryptionAsync"/>.</summary>
    public static StopDeliveryStreamEncryptionResult StopDeliveryStreamEncryption(string deliveryStreamName, RegionEndpoint? region = null)
        => StopDeliveryStreamEncryptionAsync(deliveryStreamName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagDeliveryStreamAsync"/>.</summary>
    public static void TagDeliveryStream(string deliveryStreamName, List<Amazon.KinesisFirehose.Model.Tag> tags, RegionEndpoint? region = null)
        => TagDeliveryStreamAsync(deliveryStreamName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagDeliveryStreamAsync"/>.</summary>
    public static void UntagDeliveryStream(string deliveryStreamName, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagDeliveryStreamAsync(deliveryStreamName, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForDeliveryStreamAsync"/>.</summary>
    public static ListTagsForDeliveryStreamResult ListTagsForDeliveryStream(string deliveryStreamName, string? exclusiveStartTagKey = null, int? limit = null, RegionEndpoint? region = null)
        => ListTagsForDeliveryStreamAsync(deliveryStreamName, exclusiveStartTagKey, limit, region).GetAwaiter().GetResult();

}
