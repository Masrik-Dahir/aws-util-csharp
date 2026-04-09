using Amazon;
using Amazon.IotData;
using Amazon.IotData.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record IoTPublishResult(string? Topic = null);

public sealed record GetThingShadowResult(MemoryStream? Payload = null);

public sealed record UpdateThingShadowResult(MemoryStream? Payload = null);

public sealed record ListNamedShadowsResult(
    List<string>? Results = null,
    string? NextToken = null);

public sealed record ListRetainedMessagesResult(
    List<RetainedMessageSummary>? RetainedTopics = null,
    string? NextToken = null);

public sealed record GetRetainedMessageResult(
    string? Topic = null,
    MemoryStream? Payload = null,
    int? Qos = null,
    DateTime? LastModifiedTime = null);

/// <summary>
/// Utility helpers for AWS IoT Data Plane (device shadow and MQTT).
/// </summary>
public static class IoTDataService
{
    private static AmazonIotDataClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIotDataClient>(region);

    /// <summary>
    /// Publish a message to an MQTT topic.
    /// </summary>
    public static async Task<IoTPublishResult> PublishAsync(
        string topic,
        MemoryStream? payload = null,
        int? qos = null,
        bool? retain = null,
        string? contentType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishRequest { Topic = topic };
        if (payload != null) request.Payload = payload;
        if (qos.HasValue) request.Qos = qos.Value;
        if (retain.HasValue) request.Retain = retain.Value;
        if (contentType != null) request.ContentType = contentType;

        try
        {
            await client.PublishAsync(request);
            return new IoTPublishResult(Topic: topic);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to publish to topic '{topic}'");
        }
    }

    /// <summary>
    /// Get the shadow for an IoT thing.
    /// </summary>
    public static async Task<GetThingShadowResult> GetThingShadowAsync(
        string thingName,
        string? shadowName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetThingShadowRequest { ThingName = thingName };
        if (shadowName != null) request.ShadowName = shadowName;

        try
        {
            var resp = await client.GetThingShadowAsync(request);
            return new GetThingShadowResult(Payload: resp.Payload);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get shadow for thing '{thingName}'");
        }
    }

    /// <summary>
    /// Update the shadow for an IoT thing.
    /// </summary>
    public static async Task<UpdateThingShadowResult> UpdateThingShadowAsync(
        string thingName,
        MemoryStream payload,
        string? shadowName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateThingShadowRequest
        {
            ThingName = thingName,
            Payload = payload
        };
        if (shadowName != null) request.ShadowName = shadowName;

        try
        {
            var resp = await client.UpdateThingShadowAsync(request);
            return new UpdateThingShadowResult(Payload: resp.Payload);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update shadow for thing '{thingName}'");
        }
    }

    /// <summary>
    /// Delete the shadow for an IoT thing.
    /// </summary>
    public static async Task DeleteThingShadowAsync(
        string thingName,
        string? shadowName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteThingShadowRequest { ThingName = thingName };
        if (shadowName != null) request.ShadowName = shadowName;

        try
        {
            await client.DeleteThingShadowAsync(request);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete shadow for thing '{thingName}'");
        }
    }

    /// <summary>
    /// List named shadows for an IoT thing.
    /// </summary>
    public static async Task<ListNamedShadowsResult> ListNamedShadowsForThingAsync(
        string thingName,
        int? pageSize = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListNamedShadowsForThingRequest { ThingName = thingName };
        if (pageSize.HasValue) request.PageSize = pageSize.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListNamedShadowsForThingAsync(request);
            return new ListNamedShadowsResult(
                Results: resp.Results,
                NextToken: resp.NextToken);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list named shadows for thing '{thingName}'");
        }
    }

    /// <summary>
    /// List retained messages with optional pagination.
    /// </summary>
    public static async Task<ListRetainedMessagesResult> ListRetainedMessagesAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRetainedMessagesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRetainedMessagesAsync(request);
            return new ListRetainedMessagesResult(
                RetainedTopics: resp.RetainedTopics,
                NextToken: resp.NextToken);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list retained messages");
        }
    }

    /// <summary>
    /// Get a specific retained message by topic.
    /// </summary>
    public static async Task<GetRetainedMessageResult> GetRetainedMessageAsync(
        string topic, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRetainedMessageAsync(
                new GetRetainedMessageRequest { Topic = topic });
            return new GetRetainedMessageResult(
                Topic: resp.Topic,
                Payload: resp.Payload,
                Qos: resp.Qos,
                LastModifiedTime: resp.LastModifiedTime.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(resp.LastModifiedTime.Value).UtcDateTime : (DateTime?)null);
        }
        catch (AmazonIotDataException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get retained message for topic '{topic}'");
        }
    }
}
