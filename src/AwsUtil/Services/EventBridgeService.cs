using Amazon;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record PutEventsResult(
    int? FailedEntryCount = null,
    List<PutEventsResultEntry>? Entries = null);

public sealed record PutRuleResult(string? RuleArn = null);

public sealed record DeleteRuleResult(bool Success = true);

public sealed record DescribeRuleResult(
    string? Name = null,
    string? Arn = null,
    string? EventPattern = null,
    string? ScheduleExpression = null,
    string? State = null,
    string? Description = null,
    string? RoleArn = null,
    string? ManagedBy = null,
    string? EventBusName = null,
    string? CreatedBy = null);

public sealed record ListRulesResult(
    List<Rule>? Rules = null,
    string? NextToken = null);

public sealed record EnableRuleResult(bool Success = true);

public sealed record DisableRuleResult(bool Success = true);

public sealed record PutTargetsResult(
    int? FailedEntryCount = null,
    List<PutTargetsResultEntry>? FailedEntries = null);

public sealed record RemoveTargetsResult(
    int? FailedEntryCount = null,
    List<RemoveTargetsResultEntry>? FailedEntries = null);

public sealed record ListTargetsByRuleResult(
    List<Target>? Targets = null,
    string? NextToken = null);

public sealed record CreateEventBusResult(
    string? EventBusArn = null);

public sealed record DeleteEventBusResult(bool Success = true);

public sealed record DescribeEventBusResult(
    string? Name = null,
    string? Arn = null,
    string? Policy = null);

public sealed record ListEventBusesResult(
    List<EventBus>? EventBuses = null,
    string? NextToken = null);

public sealed record PutPermissionResult(bool Success = true);

public sealed record RemovePermissionResult(bool Success = true);

public sealed record DescribeEventSourceResult(
    string? Arn = null,
    string? CreatedBy = null,
    DateTime? CreationTime = null,
    DateTime? ExpirationTime = null,
    string? Name = null,
    string? State = null);

public sealed record ListEventSourcesResult(
    List<EventSource>? EventSources = null,
    string? NextToken = null);

public sealed record CreateArchiveResult(
    string? ArchiveArn = null,
    string? State = null,
    string? StateReason = null,
    DateTime? CreationTime = null);

public sealed record DeleteArchiveResult(bool Success = true);

public sealed record DescribeArchiveResult(
    string? ArchiveArn = null,
    string? ArchiveName = null,
    string? EventSourceArn = null,
    string? Description = null,
    string? EventPattern = null,
    string? State = null,
    string? StateReason = null,
    int? RetentionDays = null,
    long? SizeBytes = null,
    long? EventCount = null,
    DateTime? CreationTime = null);

public sealed record ListArchivesResult(
    List<Archive>? Archives = null,
    string? NextToken = null);

public sealed record StartReplayResult(
    string? ReplayArn = null,
    string? State = null,
    string? StateReason = null,
    DateTime? ReplayStartTime = null);

public sealed record DescribeReplayResult(
    string? ReplayName = null,
    string? ReplayArn = null,
    string? Description = null,
    string? State = null,
    string? StateReason = null,
    string? EventSourceArn = null,
    DateTime? EventStartTime = null,
    DateTime? EventEndTime = null,
    DateTime? EventLastReplayedTime = null,
    DateTime? ReplayStartTime = null,
    DateTime? ReplayEndTime = null);

public sealed record ListReplaysResult(
    List<Replay>? Replays = null,
    string? NextToken = null);

public sealed record CancelReplayResult(
    string? ReplayArn = null,
    string? State = null,
    string? StateReason = null);

public sealed record CreateConnectionResult(
    string? ConnectionArn = null,
    string? ConnectionState = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null);

public sealed record DeleteConnectionResult(
    string? ConnectionArn = null,
    string? ConnectionState = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null,
    DateTime? LastAuthorizedTime = null);

public sealed record DescribeConnectionResult(
    string? ConnectionArn = null,
    string? Name = null,
    string? Description = null,
    string? ConnectionState = null,
    string? StateReason = null,
    string? AuthorizationType = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null,
    DateTime? LastAuthorizedTime = null);

public sealed record ListConnectionsResult(
    List<Connection>? Connections = null,
    string? NextToken = null);

public sealed record CreateApiDestinationResult(
    string? ApiDestinationArn = null,
    string? ApiDestinationState = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null);

public sealed record DeleteApiDestinationResult(bool Success = true);

public sealed record DescribeApiDestinationResult(
    string? ApiDestinationArn = null,
    string? Name = null,
    string? Description = null,
    string? ApiDestinationState = null,
    string? ConnectionArn = null,
    string? InvocationEndpoint = null,
    string? HttpMethod = null,
    int? InvocationRateLimitPerSecond = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null);

public sealed record ListApiDestinationsResult(
    List<ApiDestination>? ApiDestinations = null,
    string? NextToken = null);

public sealed record EbTagResourceResult(bool Success = true);

public sealed record EbUntagResourceResult(bool Success = true);

public sealed record EbListTagsForResourceResult(
    List<Tag>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon EventBridge.
/// </summary>
public static class EventBridgeService
{
    private static AmazonEventBridgeClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEventBridgeClient>(region);

    /// <summary>
    /// Put events onto an EventBridge event bus.
    /// </summary>
    public static async Task<PutEventsResult> PutEventsAsync(
        List<PutEventsRequestEntry> entries,
        string? endpointId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutEventsRequest { Entries = entries };
        if (endpointId != null) request.EndpointId = endpointId;

        try
        {
            var resp = await client.PutEventsAsync(request);
            return new PutEventsResult(
                FailedEntryCount: resp.FailedEntryCount,
                Entries: resp.Entries);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put events");
        }
    }

    /// <summary>
    /// Create or update an EventBridge rule.
    /// </summary>
    public static async Task<PutRuleResult> PutRuleAsync(
        PutRuleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutRuleAsync(request);
            return new PutRuleResult(RuleArn: resp.RuleArn);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put rule");
        }
    }

    /// <summary>
    /// Delete an EventBridge rule.
    /// </summary>
    public static async Task<DeleteRuleResult> DeleteRuleAsync(
        string name,
        string? eventBusName = null,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRuleRequest { Name = name };
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (force.HasValue) request.Force = force.Value;

        try
        {
            await client.DeleteRuleAsync(request);
            return new DeleteRuleResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete rule '{name}'");
        }
    }

    /// <summary>
    /// Describe an EventBridge rule.
    /// </summary>
    public static async Task<DescribeRuleResult> DescribeRuleAsync(
        string name,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeRuleRequest { Name = name };
        if (eventBusName != null) request.EventBusName = eventBusName;

        try
        {
            var resp = await client.DescribeRuleAsync(request);
            return new DescribeRuleResult(
                Name: resp.Name,
                Arn: resp.Arn,
                EventPattern: resp.EventPattern,
                ScheduleExpression: resp.ScheduleExpression,
                State: resp.State?.Value,
                Description: resp.Description,
                RoleArn: resp.RoleArn,
                ManagedBy: resp.ManagedBy,
                EventBusName: resp.EventBusName,
                CreatedBy: resp.CreatedBy);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe rule '{name}'");
        }
    }

    /// <summary>
    /// List EventBridge rules.
    /// </summary>
    public static async Task<ListRulesResult> ListRulesAsync(
        string? namePrefix = null,
        string? eventBusName = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRulesRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListRulesAsync(request);
            return new ListRulesResult(Rules: resp.Rules, NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list rules");
        }
    }

    /// <summary>
    /// Enable an EventBridge rule.
    /// </summary>
    public static async Task<EnableRuleResult> EnableRuleAsync(
        string name,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableRuleRequest { Name = name };
        if (eventBusName != null) request.EventBusName = eventBusName;

        try
        {
            await client.EnableRuleAsync(request);
            return new EnableRuleResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to enable rule '{name}'");
        }
    }

    /// <summary>
    /// Disable an EventBridge rule.
    /// </summary>
    public static async Task<DisableRuleResult> DisableRuleAsync(
        string name,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DisableRuleRequest { Name = name };
        if (eventBusName != null) request.EventBusName = eventBusName;

        try
        {
            await client.DisableRuleAsync(request);
            return new DisableRuleResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to disable rule '{name}'");
        }
    }

    /// <summary>
    /// Add targets to an EventBridge rule.
    /// </summary>
    public static async Task<PutTargetsResult> PutTargetsAsync(
        string rule,
        List<Target> targets,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutTargetsRequest { Rule = rule, Targets = targets };
        if (eventBusName != null) request.EventBusName = eventBusName;

        try
        {
            var resp = await client.PutTargetsAsync(request);
            return new PutTargetsResult(
                FailedEntryCount: resp.FailedEntryCount,
                FailedEntries: resp.FailedEntries);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put targets for rule '{rule}'");
        }
    }

    /// <summary>
    /// Remove targets from an EventBridge rule.
    /// </summary>
    public static async Task<RemoveTargetsResult> RemoveTargetsAsync(
        string rule,
        List<string> ids,
        string? eventBusName = null,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemoveTargetsRequest { Rule = rule, Ids = ids };
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.RemoveTargetsAsync(request);
            return new RemoveTargetsResult(
                FailedEntryCount: resp.FailedEntryCount,
                FailedEntries: resp.FailedEntries);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove targets from rule '{rule}'");
        }
    }

    /// <summary>
    /// List targets for an EventBridge rule.
    /// </summary>
    public static async Task<ListTargetsByRuleResult> ListTargetsByRuleAsync(
        string rule,
        string? eventBusName = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTargetsByRuleRequest { Rule = rule };
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTargetsByRuleAsync(request);
            return new ListTargetsByRuleResult(
                Targets: resp.Targets,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list targets for rule '{rule}'");
        }
    }

    /// <summary>
    /// Create an EventBridge event bus.
    /// </summary>
    public static async Task<CreateEventBusResult> CreateEventBusAsync(
        string name,
        string? eventSourceName = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateEventBusRequest { Name = name };
        if (eventSourceName != null) request.EventSourceName = eventSourceName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateEventBusAsync(request);
            return new CreateEventBusResult(EventBusArn: resp.EventBusArn);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create event bus '{name}'");
        }
    }

    /// <summary>
    /// Delete an EventBridge event bus.
    /// </summary>
    public static async Task<DeleteEventBusResult> DeleteEventBusAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteEventBusAsync(new DeleteEventBusRequest { Name = name });
            return new DeleteEventBusResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete event bus '{name}'");
        }
    }

    /// <summary>
    /// Describe an EventBridge event bus.
    /// </summary>
    public static async Task<DescribeEventBusResult> DescribeEventBusAsync(
        string? name = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventBusRequest();
        if (name != null) request.Name = name;

        try
        {
            var resp = await client.DescribeEventBusAsync(request);
            return new DescribeEventBusResult(
                Name: resp.Name,
                Arn: resp.Arn,
                Policy: resp.Policy);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe event bus");
        }
    }

    /// <summary>
    /// List EventBridge event buses.
    /// </summary>
    public static async Task<ListEventBusesResult> ListEventBusesAsync(
        string? namePrefix = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEventBusesRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListEventBusesAsync(request);
            return new ListEventBusesResult(
                EventBuses: resp.EventBuses,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list event buses");
        }
    }

    /// <summary>
    /// Add a permission to an event bus policy.
    /// </summary>
    public static async Task<PutPermissionResult> PutPermissionAsync(
        string? eventBusName = null,
        string? action = null,
        string? principal = null,
        string? statementId = null,
        Condition? condition = null,
        string? policy = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutPermissionRequest();
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (action != null) request.Action = action;
        if (principal != null) request.Principal = principal;
        if (statementId != null) request.StatementId = statementId;
        if (condition != null) request.Condition = condition;
        if (policy != null) request.Policy = policy;

        try
        {
            await client.PutPermissionAsync(request);
            return new PutPermissionResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put permission");
        }
    }

    /// <summary>
    /// Remove a permission from an event bus policy.
    /// </summary>
    public static async Task<RemovePermissionResult> RemovePermissionAsync(
        string? statementId = null,
        string? eventBusName = null,
        bool? removeAllPermissions = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemovePermissionRequest();
        if (statementId != null) request.StatementId = statementId;
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (removeAllPermissions.HasValue)
            request.RemoveAllPermissions = removeAllPermissions.Value;

        try
        {
            await client.RemovePermissionAsync(request);
            return new RemovePermissionResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove permission");
        }
    }

    /// <summary>
    /// Describe a partner event source.
    /// </summary>
    public static async Task<DescribeEventSourceResult> DescribeEventSourceAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeEventSourceAsync(
                new DescribeEventSourceRequest { Name = name });
            return new DescribeEventSourceResult(
                Arn: resp.Arn,
                CreatedBy: resp.CreatedBy,
                CreationTime: resp.CreationTime,
                ExpirationTime: resp.ExpirationTime,
                Name: resp.Name,
                State: resp.State?.Value);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe event source '{name}'");
        }
    }

    /// <summary>
    /// List partner event sources.
    /// </summary>
    public static async Task<ListEventSourcesResult> ListEventSourcesAsync(
        string? namePrefix = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEventSourcesRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListEventSourcesAsync(request);
            return new ListEventSourcesResult(
                EventSources: resp.EventSources,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list event sources");
        }
    }

    /// <summary>
    /// Create an EventBridge archive.
    /// </summary>
    public static async Task<CreateArchiveResult> CreateArchiveAsync(
        string archiveName,
        string eventSourceArn,
        string? description = null,
        string? eventPattern = null,
        int? retentionDays = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateArchiveRequest
        {
            ArchiveName = archiveName,
            EventSourceArn = eventSourceArn
        };
        if (description != null) request.Description = description;
        if (eventPattern != null) request.EventPattern = eventPattern;
        if (retentionDays.HasValue) request.RetentionDays = retentionDays.Value;

        try
        {
            var resp = await client.CreateArchiveAsync(request);
            return new CreateArchiveResult(
                ArchiveArn: resp.ArchiveArn,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                CreationTime: resp.CreationTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create archive '{archiveName}'");
        }
    }

    /// <summary>
    /// Delete an EventBridge archive.
    /// </summary>
    public static async Task<DeleteArchiveResult> DeleteArchiveAsync(
        string archiveName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteArchiveAsync(new DeleteArchiveRequest
            {
                ArchiveName = archiveName
            });
            return new DeleteArchiveResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete archive '{archiveName}'");
        }
    }

    /// <summary>
    /// Describe an EventBridge archive.
    /// </summary>
    public static async Task<DescribeArchiveResult> DescribeArchiveAsync(
        string archiveName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeArchiveAsync(new DescribeArchiveRequest
            {
                ArchiveName = archiveName
            });
            return new DescribeArchiveResult(
                ArchiveArn: resp.ArchiveArn,
                ArchiveName: resp.ArchiveName,
                EventSourceArn: resp.EventSourceArn,
                Description: resp.Description,
                EventPattern: resp.EventPattern,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                RetentionDays: resp.RetentionDays,
                SizeBytes: resp.SizeBytes,
                EventCount: resp.EventCount,
                CreationTime: resp.CreationTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe archive '{archiveName}'");
        }
    }

    /// <summary>
    /// List EventBridge archives.
    /// </summary>
    public static async Task<ListArchivesResult> ListArchivesAsync(
        string? namePrefix = null,
        string? eventSourceArn = null,
        string? state = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListArchivesRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (eventSourceArn != null) request.EventSourceArn = eventSourceArn;
        if (state != null) request.State = new ArchiveState(state);
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListArchivesAsync(request);
            return new ListArchivesResult(
                Archives: resp.Archives,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list archives");
        }
    }

    /// <summary>
    /// Start a replay of archived events.
    /// </summary>
    public static async Task<StartReplayResult> StartReplayAsync(
        StartReplayRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartReplayAsync(request);
            return new StartReplayResult(
                ReplayArn: resp.ReplayArn,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                ReplayStartTime: resp.ReplayStartTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start replay");
        }
    }

    /// <summary>
    /// Describe a replay.
    /// </summary>
    public static async Task<DescribeReplayResult> DescribeReplayAsync(
        string replayName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeReplayAsync(new DescribeReplayRequest
            {
                ReplayName = replayName
            });
            return new DescribeReplayResult(
                ReplayName: resp.ReplayName,
                ReplayArn: resp.ReplayArn,
                Description: resp.Description,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                EventSourceArn: resp.EventSourceArn,
                EventStartTime: resp.EventStartTime,
                EventEndTime: resp.EventEndTime,
                EventLastReplayedTime: resp.EventLastReplayedTime,
                ReplayStartTime: resp.ReplayStartTime,
                ReplayEndTime: resp.ReplayEndTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe replay '{replayName}'");
        }
    }

    /// <summary>
    /// List replays.
    /// </summary>
    public static async Task<ListReplaysResult> ListReplaysAsync(
        string? namePrefix = null,
        string? state = null,
        string? eventSourceArn = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListReplaysRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (state != null) request.State = new ReplayState(state);
        if (eventSourceArn != null) request.EventSourceArn = eventSourceArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListReplaysAsync(request);
            return new ListReplaysResult(
                Replays: resp.Replays,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list replays");
        }
    }

    /// <summary>
    /// Cancel a running replay.
    /// </summary>
    public static async Task<CancelReplayResult> CancelReplayAsync(
        string replayName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelReplayAsync(new CancelReplayRequest
            {
                ReplayName = replayName
            });
            return new CancelReplayResult(
                ReplayArn: resp.ReplayArn,
                State: resp.State?.Value,
                StateReason: resp.StateReason);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel replay '{replayName}'");
        }
    }

    /// <summary>
    /// Create an EventBridge connection.
    /// </summary>
    public static async Task<CreateConnectionResult> CreateConnectionAsync(
        CreateConnectionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateConnectionAsync(request);
            return new CreateConnectionResult(
                ConnectionArn: resp.ConnectionArn,
                ConnectionState: resp.ConnectionState?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create connection");
        }
    }

    /// <summary>
    /// Delete an EventBridge connection.
    /// </summary>
    public static async Task<DeleteConnectionResult> DeleteConnectionAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteConnectionAsync(new DeleteConnectionRequest
            {
                Name = name
            });
            return new DeleteConnectionResult(
                ConnectionArn: resp.ConnectionArn,
                ConnectionState: resp.ConnectionState?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime,
                LastAuthorizedTime: resp.LastAuthorizedTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete connection '{name}'");
        }
    }

    /// <summary>
    /// Describe an EventBridge connection.
    /// </summary>
    public static async Task<DescribeConnectionResult> DescribeConnectionAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeConnectionAsync(
                new DescribeConnectionRequest { Name = name });
            return new DescribeConnectionResult(
                ConnectionArn: resp.ConnectionArn,
                Name: resp.Name,
                Description: resp.Description,
                ConnectionState: resp.ConnectionState?.Value,
                StateReason: resp.StateReason,
                AuthorizationType: resp.AuthorizationType?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime,
                LastAuthorizedTime: resp.LastAuthorizedTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe connection '{name}'");
        }
    }

    /// <summary>
    /// List EventBridge connections.
    /// </summary>
    public static async Task<ListConnectionsResult> ListConnectionsAsync(
        string? namePrefix = null,
        string? connectionState = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConnectionsRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (connectionState != null)
            request.ConnectionState = new ConnectionState(connectionState);
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListConnectionsAsync(request);
            return new ListConnectionsResult(
                Connections: resp.Connections,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list connections");
        }
    }

    /// <summary>
    /// Create an API destination.
    /// </summary>
    public static async Task<CreateApiDestinationResult> CreateApiDestinationAsync(
        CreateApiDestinationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateApiDestinationAsync(request);
            return new CreateApiDestinationResult(
                ApiDestinationArn: resp.ApiDestinationArn,
                ApiDestinationState: resp.ApiDestinationState?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create API destination");
        }
    }

    /// <summary>
    /// Delete an API destination.
    /// </summary>
    public static async Task<DeleteApiDestinationResult> DeleteApiDestinationAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApiDestinationAsync(
                new DeleteApiDestinationRequest { Name = name });
            return new DeleteApiDestinationResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete API destination '{name}'");
        }
    }

    /// <summary>
    /// Describe an API destination.
    /// </summary>
    public static async Task<DescribeApiDestinationResult> DescribeApiDestinationAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeApiDestinationAsync(
                new DescribeApiDestinationRequest { Name = name });
            return new DescribeApiDestinationResult(
                ApiDestinationArn: resp.ApiDestinationArn,
                Name: resp.Name,
                Description: resp.Description,
                ApiDestinationState: resp.ApiDestinationState?.Value,
                ConnectionArn: resp.ConnectionArn,
                InvocationEndpoint: resp.InvocationEndpoint,
                HttpMethod: resp.HttpMethod?.Value,
                InvocationRateLimitPerSecond: resp.InvocationRateLimitPerSecond,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe API destination '{name}'");
        }
    }

    /// <summary>
    /// List API destinations.
    /// </summary>
    public static async Task<ListApiDestinationsResult> ListApiDestinationsAsync(
        string? namePrefix = null,
        string? connectionArn = null,
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApiDestinationsRequest();
        if (namePrefix != null) request.NamePrefix = namePrefix;
        if (connectionArn != null) request.ConnectionArn = connectionArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListApiDestinationsAsync(request);
            return new ListApiDestinationsResult(
                ApiDestinations: resp.ApiDestinations,
                NextToken: resp.NextToken);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list API destinations");
        }
    }

    /// <summary>
    /// Add tags to an EventBridge resource.
    /// </summary>
    public static async Task<EbTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags
            });
            return new EbTagResourceResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resource");
        }
    }

    /// <summary>
    /// Remove tags from an EventBridge resource.
    /// </summary>
    public static async Task<EbUntagResourceResult> UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
            return new EbUntagResourceResult();
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag resource");
        }
    }

    /// <summary>
    /// List tags for an EventBridge resource.
    /// </summary>
    public static async Task<EbListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceARN = resourceArn });
            return new EbListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonEventBridgeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags for resource");
        }
    }
}
