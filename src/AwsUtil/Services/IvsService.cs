using Amazon;
using Amazon.IVS;
using Amazon.IVS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateIvsChannelResult(
    Channel? Channel = null,
    StreamKey? StreamKey = null);

public sealed record GetIvsChannelResult(Channel? Channel = null);

public sealed record ListIvsChannelsResult(
    List<ChannelSummary>? Channels = null,
    string? NextToken = null);

public sealed record UpdateIvsChannelResult(Channel? Channel = null);

public sealed record CreateIvsStreamKeyResult(StreamKey? StreamKey = null);

public sealed record GetIvsStreamKeyResult(StreamKey? StreamKey = null);

public sealed record ListIvsStreamKeysResult(
    List<StreamKeySummary>? StreamKeys = null,
    string? NextToken = null);

public sealed record GetIvsStreamResult(Amazon.IVS.Model.Stream? Stream = null);

public sealed record ListIvsStreamsResult(
    List<StreamSummary>? Streams = null,
    string? NextToken = null);

public sealed record GetStreamSessionResult(StreamSession? StreamSession = null);

public sealed record ListStreamSessionsResult(
    List<StreamSessionSummary>? StreamSessions = null,
    string? NextToken = null);

public sealed record CreateIvsRecordingConfigurationResult(
    RecordingConfiguration? RecordingConfiguration = null);

public sealed record GetIvsRecordingConfigurationResult(
    RecordingConfiguration? RecordingConfiguration = null);

public sealed record ListIvsRecordingConfigurationsResult(
    List<RecordingConfigurationSummary>? RecordingConfigurations = null,
    string? NextToken = null);

public sealed record BatchGetChannelResult(
    List<Channel>? Channels = null,
    List<BatchError>? Errors = null);

public sealed record BatchGetStreamKeyResult(
    List<StreamKey>? StreamKeys = null,
    List<BatchError>? Errors = null);

public sealed record IvsListTagsResult(
    Dictionary<string, string>? Tags = null);

public sealed record CreatePlaybackRestrictionPolicyResult(
    PlaybackRestrictionPolicy? PlaybackRestrictionPolicy = null);

public sealed record GetPlaybackRestrictionPolicyResult(
    PlaybackRestrictionPolicy? PlaybackRestrictionPolicy = null);

public sealed record ListPlaybackRestrictionPoliciesResult(
    List<PlaybackRestrictionPolicySummary>? PlaybackRestrictionPolicies = null,
    string? NextToken = null);

public sealed record UpdatePlaybackRestrictionPolicyResult(
    PlaybackRestrictionPolicy? PlaybackRestrictionPolicy = null);

/// <summary>
/// Utility helpers for Amazon Interactive Video Service (IVS).
/// </summary>
public static class IvsService
{
    private static AmazonIVSClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIVSClient>(region);

    // ── Channel operations ──────────────────────────────────────────

    /// <summary>
    /// Create an IVS channel.
    /// </summary>
    public static async Task<CreateIvsChannelResult> CreateChannelAsync(
        string? name = null,
        ChannelLatencyMode? latencyMode = null,
        ChannelType? type = null,
        bool? authorized = null,
        string? recordingConfigurationArn = null,
        bool? insecureIngest = null,
        string? playbackRestrictionPolicyArn = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateChannelRequest();
        if (name != null) request.Name = name;
        if (latencyMode != null) request.LatencyMode = latencyMode;
        if (type != null) request.Type = type;
        if (authorized.HasValue) request.Authorized = authorized.Value;
        if (recordingConfigurationArn != null)
            request.RecordingConfigurationArn = recordingConfigurationArn;
        if (insecureIngest.HasValue) request.InsecureIngest = insecureIngest.Value;
        if (playbackRestrictionPolicyArn != null)
            request.PlaybackRestrictionPolicyArn = playbackRestrictionPolicyArn;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateChannelAsync(request);
            return new CreateIvsChannelResult(
                Channel: resp.Channel,
                StreamKey: resp.StreamKey);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create IVS channel '{name}'");
        }
    }

    /// <summary>
    /// Delete an IVS channel.
    /// </summary>
    public static async Task DeleteChannelAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteChannelAsync(
                new DeleteChannelRequest { Arn = arn });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete IVS channel '{arn}'");
        }
    }

    /// <summary>
    /// Get an IVS channel.
    /// </summary>
    public static async Task<GetIvsChannelResult> GetChannelAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetChannelAsync(
                new GetChannelRequest { Arn = arn });
            return new GetIvsChannelResult(Channel: resp.Channel);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get IVS channel '{arn}'");
        }
    }

    /// <summary>
    /// List IVS channels with optional filtering.
    /// </summary>
    public static async Task<ListIvsChannelsResult> ListChannelsAsync(
        string? filterByName = null,
        string? filterByPlaybackRestrictionPolicyArn = null,
        string? filterByRecordingConfigurationArn = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListChannelsRequest();
        if (filterByName != null) request.FilterByName = filterByName;
        if (filterByPlaybackRestrictionPolicyArn != null)
            request.FilterByPlaybackRestrictionPolicyArn =
                filterByPlaybackRestrictionPolicyArn;
        if (filterByRecordingConfigurationArn != null)
            request.FilterByRecordingConfigurationArn =
                filterByRecordingConfigurationArn;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListChannelsAsync(request);
            return new ListIvsChannelsResult(
                Channels: resp.Channels,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list IVS channels");
        }
    }

    /// <summary>
    /// Update an IVS channel.
    /// </summary>
    public static async Task<UpdateIvsChannelResult> UpdateChannelAsync(
        string arn,
        string? name = null,
        ChannelLatencyMode? latencyMode = null,
        ChannelType? type = null,
        bool? authorized = null,
        string? recordingConfigurationArn = null,
        bool? insecureIngest = null,
        string? playbackRestrictionPolicyArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateChannelRequest { Arn = arn };
        if (name != null) request.Name = name;
        if (latencyMode != null) request.LatencyMode = latencyMode;
        if (type != null) request.Type = type;
        if (authorized.HasValue) request.Authorized = authorized.Value;
        if (recordingConfigurationArn != null)
            request.RecordingConfigurationArn = recordingConfigurationArn;
        if (insecureIngest.HasValue) request.InsecureIngest = insecureIngest.Value;
        if (playbackRestrictionPolicyArn != null)
            request.PlaybackRestrictionPolicyArn = playbackRestrictionPolicyArn;

        try
        {
            var resp = await client.UpdateChannelAsync(request);
            return new UpdateIvsChannelResult(Channel: resp.Channel);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update IVS channel '{arn}'");
        }
    }

    // ── Stream Key operations ───────────────────────────────────────

    /// <summary>
    /// Create a stream key for an IVS channel.
    /// </summary>
    public static async Task<CreateIvsStreamKeyResult> CreateStreamKeyAsync(
        string channelArn,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateStreamKeyRequest { ChannelArn = channelArn };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateStreamKeyAsync(request);
            return new CreateIvsStreamKeyResult(StreamKey: resp.StreamKey);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create stream key for channel '{channelArn}'");
        }
    }

    /// <summary>
    /// Delete a stream key.
    /// </summary>
    public static async Task DeleteStreamKeyAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteStreamKeyAsync(
                new DeleteStreamKeyRequest { Arn = arn });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete stream key '{arn}'");
        }
    }

    /// <summary>
    /// Get a stream key.
    /// </summary>
    public static async Task<GetIvsStreamKeyResult> GetStreamKeyAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetStreamKeyAsync(
                new GetStreamKeyRequest { Arn = arn });
            return new GetIvsStreamKeyResult(StreamKey: resp.StreamKey);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get stream key '{arn}'");
        }
    }

    /// <summary>
    /// List stream keys for an IVS channel.
    /// </summary>
    public static async Task<ListIvsStreamKeysResult> ListStreamKeysAsync(
        string channelArn,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStreamKeysRequest { ChannelArn = channelArn };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStreamKeysAsync(request);
            return new ListIvsStreamKeysResult(
                StreamKeys: resp.StreamKeys,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list stream keys for channel '{channelArn}'");
        }
    }

    // ── Stream operations ───────────────────────────────────────────

    /// <summary>
    /// Get an active stream on a channel.
    /// </summary>
    public static async Task<GetIvsStreamResult> GetStreamAsync(
        string channelArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetStreamAsync(
                new GetStreamRequest { ChannelArn = channelArn });
            return new GetIvsStreamResult(Stream: resp.Stream);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get stream for channel '{channelArn}'");
        }
    }

    /// <summary>
    /// List active streams with optional filtering.
    /// </summary>
    public static async Task<ListIvsStreamsResult> ListStreamsAsync(
        StreamFilters? filterBy = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStreamsRequest();
        if (filterBy != null) request.FilterBy = filterBy;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStreamsAsync(request);
            return new ListIvsStreamsResult(
                Streams: resp.Streams,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list IVS streams");
        }
    }

    /// <summary>
    /// Stop an active stream on a channel.
    /// </summary>
    public static async Task StopStreamAsync(
        string channelArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopStreamAsync(
                new StopStreamRequest { ChannelArn = channelArn });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop stream for channel '{channelArn}'");
        }
    }

    /// <summary>
    /// Get a stream session by session ID.
    /// </summary>
    public static async Task<GetStreamSessionResult> GetStreamSessionAsync(
        string channelArn,
        string? streamId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetStreamSessionRequest { ChannelArn = channelArn };
        if (streamId != null) request.StreamId = streamId;

        try
        {
            var resp = await client.GetStreamSessionAsync(request);
            return new GetStreamSessionResult(StreamSession: resp.StreamSession);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get stream session for channel '{channelArn}'");
        }
    }

    /// <summary>
    /// List stream sessions for a channel.
    /// </summary>
    public static async Task<ListStreamSessionsResult> ListStreamSessionsAsync(
        string channelArn,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStreamSessionsRequest { ChannelArn = channelArn };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStreamSessionsAsync(request);
            return new ListStreamSessionsResult(
                StreamSessions: resp.StreamSessions,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list stream sessions for channel '{channelArn}'");
        }
    }

    // ── Recording Configuration operations ──────────────────────────

    /// <summary>
    /// Create a recording configuration.
    /// </summary>
    public static async Task<CreateIvsRecordingConfigurationResult>
        CreateRecordingConfigurationAsync(
        DestinationConfiguration destinationConfiguration,
        string? name = null,
        int? recordingReconnectWindowSeconds = null,
        ThumbnailConfiguration? thumbnailConfiguration = null,
        RenditionConfiguration? renditionConfiguration = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateRecordingConfigurationRequest
        {
            DestinationConfiguration = destinationConfiguration
        };
        if (name != null) request.Name = name;
        if (recordingReconnectWindowSeconds.HasValue)
            request.RecordingReconnectWindowSeconds =
                recordingReconnectWindowSeconds.Value;
        if (thumbnailConfiguration != null)
            request.ThumbnailConfiguration = thumbnailConfiguration;
        if (renditionConfiguration != null)
            request.RenditionConfiguration = renditionConfiguration;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateRecordingConfigurationAsync(request);
            return new CreateIvsRecordingConfigurationResult(
                RecordingConfiguration: resp.RecordingConfiguration);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create recording configuration");
        }
    }

    /// <summary>
    /// Delete a recording configuration.
    /// </summary>
    public static async Task DeleteRecordingConfigurationAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRecordingConfigurationAsync(
                new DeleteRecordingConfigurationRequest { Arn = arn });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete recording configuration '{arn}'");
        }
    }

    /// <summary>
    /// Get a recording configuration.
    /// </summary>
    public static async Task<GetIvsRecordingConfigurationResult>
        GetRecordingConfigurationAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRecordingConfigurationAsync(
                new GetRecordingConfigurationRequest { Arn = arn });
            return new GetIvsRecordingConfigurationResult(
                RecordingConfiguration: resp.RecordingConfiguration);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get recording configuration '{arn}'");
        }
    }

    /// <summary>
    /// List recording configurations with optional pagination.
    /// </summary>
    public static async Task<ListIvsRecordingConfigurationsResult>
        ListRecordingConfigurationsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRecordingConfigurationsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRecordingConfigurationsAsync(request);
            return new ListIvsRecordingConfigurationsResult(
                RecordingConfigurations: resp.RecordingConfigurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list recording configurations");
        }
    }

    // ── Batch operations ────────────────────────────────────────────

    /// <summary>
    /// Batch get channels by ARN.
    /// </summary>
    public static async Task<BatchGetChannelResult> BatchGetChannelAsync(
        List<string> arns, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetChannelAsync(
                new BatchGetChannelRequest { Arns = arns });
            return new BatchGetChannelResult(
                Channels: resp.Channels,
                Errors: resp.Errors);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch get channels");
        }
    }

    /// <summary>
    /// Batch get stream keys by ARN.
    /// </summary>
    public static async Task<BatchGetStreamKeyResult> BatchGetStreamKeyAsync(
        List<string> arns, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetStreamKeyAsync(
                new BatchGetStreamKeyRequest { Arns = arns });
            return new BatchGetStreamKeyResult(
                StreamKeys: resp.StreamKeys,
                Errors: resp.Errors);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch get stream keys");
        }
    }

    /// <summary>
    /// Put timed metadata into a channel's stream.
    /// </summary>
    public static async Task PutMetadataAsync(
        string channelArn,
        string metadata,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutMetadataAsync(new PutMetadataRequest
            {
                ChannelArn = channelArn,
                Metadata = metadata
            });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put metadata for channel '{channelArn}'");
        }
    }

    // ── Tagging operations ──────────────────────────────────────────

    /// <summary>
    /// Add tags to an IVS resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an IVS resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an IVS resource.
    /// </summary>
    public static async Task<IvsListTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new IvsListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }

    // ── Playback Restriction Policy operations ──────────────────────

    /// <summary>
    /// Create a playback restriction policy.
    /// </summary>
    public static async Task<CreatePlaybackRestrictionPolicyResult>
        CreatePlaybackRestrictionPolicyAsync(
        List<string>? allowedCountries = null,
        List<string>? allowedOrigins = null,
        bool? enableStrictOriginEnforcement = null,
        string? name = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePlaybackRestrictionPolicyRequest();
        if (allowedCountries != null) request.AllowedCountries = allowedCountries;
        if (allowedOrigins != null) request.AllowedOrigins = allowedOrigins;
        if (enableStrictOriginEnforcement.HasValue)
            request.EnableStrictOriginEnforcement =
                enableStrictOriginEnforcement.Value;
        if (name != null) request.Name = name;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePlaybackRestrictionPolicyAsync(request);
            return new CreatePlaybackRestrictionPolicyResult(
                PlaybackRestrictionPolicy: resp.PlaybackRestrictionPolicy);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create playback restriction policy");
        }
    }

    /// <summary>
    /// Delete a playback restriction policy.
    /// </summary>
    public static async Task DeletePlaybackRestrictionPolicyAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePlaybackRestrictionPolicyAsync(
                new DeletePlaybackRestrictionPolicyRequest { Arn = arn });
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete playback restriction policy '{arn}'");
        }
    }

    /// <summary>
    /// Get a playback restriction policy.
    /// </summary>
    public static async Task<GetPlaybackRestrictionPolicyResult>
        GetPlaybackRestrictionPolicyAsync(
        string arn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPlaybackRestrictionPolicyAsync(
                new GetPlaybackRestrictionPolicyRequest { Arn = arn });
            return new GetPlaybackRestrictionPolicyResult(
                PlaybackRestrictionPolicy: resp.PlaybackRestrictionPolicy);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get playback restriction policy '{arn}'");
        }
    }

    /// <summary>
    /// List playback restriction policies with optional pagination.
    /// </summary>
    public static async Task<ListPlaybackRestrictionPoliciesResult>
        ListPlaybackRestrictionPoliciesAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPlaybackRestrictionPoliciesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListPlaybackRestrictionPoliciesAsync(request);
            return new ListPlaybackRestrictionPoliciesResult(
                PlaybackRestrictionPolicies: resp.PlaybackRestrictionPolicies,
                NextToken: resp.NextToken);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list playback restriction policies");
        }
    }

    /// <summary>
    /// Update a playback restriction policy.
    /// </summary>
    public static async Task<UpdatePlaybackRestrictionPolicyResult>
        UpdatePlaybackRestrictionPolicyAsync(
        string arn,
        List<string>? allowedCountries = null,
        List<string>? allowedOrigins = null,
        bool? enableStrictOriginEnforcement = null,
        string? name = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdatePlaybackRestrictionPolicyRequest { Arn = arn };
        if (allowedCountries != null) request.AllowedCountries = allowedCountries;
        if (allowedOrigins != null) request.AllowedOrigins = allowedOrigins;
        if (enableStrictOriginEnforcement.HasValue)
            request.EnableStrictOriginEnforcement =
                enableStrictOriginEnforcement.Value;
        if (name != null) request.Name = name;

        try
        {
            var resp = await client.UpdatePlaybackRestrictionPolicyAsync(request);
            return new UpdatePlaybackRestrictionPolicyResult(
                PlaybackRestrictionPolicy: resp.PlaybackRestrictionPolicy);
        }
        catch (AmazonIVSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update playback restriction policy '{arn}'");
        }
    }
}
