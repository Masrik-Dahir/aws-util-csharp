using Amazon;
using Amazon.CloudTrail;
using Amazon.CloudTrail.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of CreateTrail.</summary>
public sealed record CreateTrailResult(
    string? Name = null,
    string? TrailArn = null,
    string? S3BucketName = null,
    string? S3KeyPrefix = null,
    string? SnsTopicArn = null,
    bool? IncludeGlobalServiceEvents = null,
    bool? IsMultiRegionTrail = null,
    string? LogFileValidationEnabled = null,
    string? CloudWatchLogsLogGroupArn = null,
    string? CloudWatchLogsRoleArn = null,
    string? KmsKeyId = null,
    bool? IsOrganizationTrail = null);

/// <summary>Result of DeleteTrail (void operation).</summary>
public sealed record DeleteTrailResult(bool Success = true);

/// <summary>Result of DescribeTrails.</summary>
public sealed record DescribeTrailsResult(List<Trail>? TrailList = null);

/// <summary>Result of GetTrail.</summary>
public sealed record GetTrailResult(Trail? Trail = null);

/// <summary>Result of GetTrailStatus.</summary>
public sealed record GetTrailStatusResult(
    bool? IsLogging = null,
    DateTime? LatestDeliveryTime = null,
    DateTime? LatestNotificationTime = null,
    DateTime? StartLoggingTime = null,
    DateTime? StopLoggingTime = null,
    DateTime? LatestCloudWatchLogsDeliveryTime = null,
    DateTime? LatestDigestDeliveryTime = null,
    string? LatestDeliveryError = null,
    string? LatestNotificationError = null,
    string? LatestDigestDeliveryError = null,
    string? LatestCloudWatchLogsDeliveryError = null);

/// <summary>Result of UpdateTrail.</summary>
public sealed record UpdateTrailResult(
    string? Name = null,
    string? TrailArn = null,
    string? S3BucketName = null,
    string? S3KeyPrefix = null,
    string? SnsTopicArn = null,
    bool? IncludeGlobalServiceEvents = null,
    bool? IsMultiRegionTrail = null,
    string? CloudWatchLogsLogGroupArn = null,
    string? CloudWatchLogsRoleArn = null,
    string? KmsKeyId = null,
    bool? IsOrganizationTrail = null);

/// <summary>Result of StartLogging (void operation).</summary>
public sealed record StartLoggingResult(bool Success = true);

/// <summary>Result of StopLogging (void operation).</summary>
public sealed record StopLoggingResult(bool Success = true);

/// <summary>Result of LookupEvents.</summary>
public sealed record LookupEventsResult(
    List<Event>? Events = null,
    string? NextToken = null);

/// <summary>Result of PutEventSelectors.</summary>
public sealed record PutEventSelectorsResult(
    string? TrailArn = null,
    List<EventSelector>? EventSelectors = null,
    List<AdvancedEventSelector>? AdvancedEventSelectors = null);

/// <summary>Result of GetEventSelectors.</summary>
public sealed record GetEventSelectorsResult(
    string? TrailArn = null,
    List<EventSelector>? EventSelectors = null,
    List<AdvancedEventSelector>? AdvancedEventSelectors = null);

/// <summary>Result of PutInsightSelectors.</summary>
public sealed record PutInsightSelectorsResult(
    string? TrailArn = null,
    List<InsightSelector>? InsightSelectors = null);

/// <summary>Result of GetInsightSelectors.</summary>
public sealed record GetInsightSelectorsResult(
    string? TrailArn = null,
    List<InsightSelector>? InsightSelectors = null);

/// <summary>Result of AddTags (void operation).</summary>
public sealed record CloudTrailAddTagsResult(bool Success = true);

/// <summary>Result of RemoveTags (void operation).</summary>
public sealed record CloudTrailRemoveTagsResult(bool Success = true);

/// <summary>Result of ListTags.</summary>
public sealed record CloudTrailListTagsResult(
    List<ResourceTag>? ResourceTagList = null,
    string? NextToken = null);

/// <summary>Result of GetQueryResults.</summary>
public sealed record GetQueryResultsResult(
    string? QueryStatus = null,
    QueryStatistics? QueryStatistics = null,
    List<List<Dictionary<string, string>>>? QueryResultRows = null,
    string? NextToken = null,
    string? ErrorMessage = null);

/// <summary>Result of StartQuery.</summary>
public sealed record StartQueryResult(string? QueryId = null);

/// <summary>Result of CancelQuery.</summary>
public sealed record CancelQueryResult(
    string? QueryId = null,
    string? QueryStatus = null);

/// <summary>Result of ListQueries.</summary>
public sealed record ListQueriesResult(
    List<Query>? Queries = null,
    string? NextToken = null);

/// <summary>Result of CreateEventDataStore.</summary>
public sealed record CreateEventDataStoreResult(
    string? EventDataStoreArn = null,
    string? Name = null,
    string? Status = null,
    bool? MultiRegionEnabled = null,
    bool? OrganizationEnabled = null,
    int? RetentionPeriod = null,
    bool? TerminationProtectionEnabled = null,
    List<AdvancedEventSelector>? AdvancedEventSelectors = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null,
    string? KmsKeyId = null);

/// <summary>Result of DeleteEventDataStore (void operation).</summary>
public sealed record DeleteEventDataStoreResult(bool Success = true);

/// <summary>Result of GetEventDataStore.</summary>
public sealed record GetEventDataStoreResult(
    string? EventDataStoreArn = null,
    string? Name = null,
    string? Status = null,
    bool? MultiRegionEnabled = null,
    bool? OrganizationEnabled = null,
    int? RetentionPeriod = null,
    bool? TerminationProtectionEnabled = null,
    List<AdvancedEventSelector>? AdvancedEventSelectors = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null,
    string? KmsKeyId = null);

/// <summary>Result of ListEventDataStores.</summary>
public sealed record ListEventDataStoresResult(
    List<EventDataStore>? EventDataStores = null,
    string? NextToken = null);

/// <summary>Result of UpdateEventDataStore.</summary>
public sealed record UpdateEventDataStoreResult(
    string? EventDataStoreArn = null,
    string? Name = null,
    string? Status = null,
    bool? MultiRegionEnabled = null,
    bool? OrganizationEnabled = null,
    int? RetentionPeriod = null,
    bool? TerminationProtectionEnabled = null,
    List<AdvancedEventSelector>? AdvancedEventSelectors = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null,
    string? KmsKeyId = null);

/// <summary>Result of StartImport.</summary>
public sealed record StartImportResult(
    string? ImportId = null,
    string? ImportStatus = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null);

/// <summary>Result of StopImport.</summary>
public sealed record StopImportResult(
    string? ImportId = null,
    string? ImportStatus = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null);

/// <summary>Result of GetImport.</summary>
public sealed record GetImportResult(
    string? ImportId = null,
    string? ImportStatus = null,
    DateTime? CreatedTimestamp = null,
    DateTime? UpdatedTimestamp = null,
    DateTime? StartEventTime = null,
    DateTime? EndEventTime = null);

/// <summary>Result of ListImports.</summary>
public sealed record CloudTrailListImportsResult(
    List<ImportsListItem>? Imports = null,
    string? NextToken = null);

/// <summary>Result of CreateChannel.</summary>
public sealed record CreateChannelResult(
    string? ChannelArn = null,
    string? Name = null,
    string? Source = null);

/// <summary>Result of DeleteChannel (void operation).</summary>
public sealed record DeleteChannelResult(bool Success = true);

/// <summary>Result of GetChannel.</summary>
public sealed record GetChannelResult(
    string? ChannelArn = null,
    string? Name = null,
    string? Source = null,
    List<Destination>? Destinations = null);

/// <summary>Result of ListChannels.</summary>
public sealed record ListChannelsResult(
    List<Channel>? Channels = null,
    string? NextToken = null);

/// <summary>Result of UpdateChannel.</summary>
public sealed record UpdateChannelResult(
    string? ChannelArn = null,
    string? Name = null,
    string? Source = null,
    List<Destination>? Destinations = null);

/// <summary>Result of RegisterOrganizationDelegatedAdmin (void operation).</summary>
public sealed record RegisterOrganizationDelegatedAdminResult(bool Success = true);

/// <summary>Result of DeregisterOrganizationDelegatedAdmin (void operation).</summary>
public sealed record DeregisterOrganizationDelegatedAdminResult(bool Success = true);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for AWS CloudTrail.
/// </summary>
public static class CloudTrailService
{
    private static AmazonCloudTrailClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudTrailClient>(region);

    // -----------------------------------------------------------------------
    // Trail management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new CloudTrail trail.
    /// </summary>
    public static async Task<CreateTrailResult> CreateTrailAsync(
        CreateTrailRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTrailAsync(request);
            return new CreateTrailResult(
                Name: resp.Name,
                TrailArn: resp.TrailARN,
                S3BucketName: resp.S3BucketName,
                S3KeyPrefix: resp.S3KeyPrefix,
                SnsTopicArn: resp.SnsTopicARN,
                IncludeGlobalServiceEvents: resp.IncludeGlobalServiceEvents,
                IsMultiRegionTrail: resp.IsMultiRegionTrail,
                CloudWatchLogsLogGroupArn: resp.CloudWatchLogsLogGroupArn,
                CloudWatchLogsRoleArn: resp.CloudWatchLogsRoleArn,
                KmsKeyId: resp.KmsKeyId,
                IsOrganizationTrail: resp.IsOrganizationTrail);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create trail");
        }
    }

    /// <summary>
    /// Delete a CloudTrail trail.
    /// </summary>
    public static async Task<DeleteTrailResult> DeleteTrailAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTrailAsync(new DeleteTrailRequest { Name = name });
            return new DeleteTrailResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete trail '{name}'");
        }
    }

    /// <summary>
    /// Describe one or more trails.
    /// </summary>
    public static async Task<DescribeTrailsResult> DescribeTrailsAsync(
        List<string>? trailNameList = null,
        bool? includeShadowTrails = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTrailsRequest();
        if (trailNameList != null) request.TrailNameList = trailNameList;
        if (includeShadowTrails.HasValue) request.IncludeShadowTrails = includeShadowTrails.Value;

        try
        {
            var resp = await client.DescribeTrailsAsync(request);
            return new DescribeTrailsResult(TrailList: resp.TrailList);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe trails");
        }
    }

    /// <summary>
    /// Get details for a single trail.
    /// </summary>
    public static async Task<GetTrailResult> GetTrailAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTrailAsync(new GetTrailRequest { Name = name });
            return new GetTrailResult(Trail: resp.Trail);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get trail '{name}'");
        }
    }

    /// <summary>
    /// Get the status of a trail.
    /// </summary>
    public static async Task<GetTrailStatusResult> GetTrailStatusAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTrailStatusAsync(new GetTrailStatusRequest { Name = name });
            return new GetTrailStatusResult(
                IsLogging: resp.IsLogging,
                LatestDeliveryTime: resp.LatestDeliveryTime,
                LatestNotificationTime: resp.LatestNotificationTime,
                StartLoggingTime: resp.StartLoggingTime,
                StopLoggingTime: resp.StopLoggingTime,
                LatestCloudWatchLogsDeliveryTime: resp.LatestCloudWatchLogsDeliveryTime,
                LatestDigestDeliveryTime: resp.LatestDigestDeliveryTime,
                LatestDeliveryError: resp.LatestDeliveryError,
                LatestNotificationError: resp.LatestNotificationError,
                LatestDigestDeliveryError: resp.LatestDigestDeliveryError,
                LatestCloudWatchLogsDeliveryError: resp.LatestCloudWatchLogsDeliveryError);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get trail status for '{name}'");
        }
    }

    /// <summary>
    /// Update a CloudTrail trail.
    /// </summary>
    public static async Task<UpdateTrailResult> UpdateTrailAsync(
        UpdateTrailRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateTrailAsync(request);
            return new UpdateTrailResult(
                Name: resp.Name,
                TrailArn: resp.TrailARN,
                S3BucketName: resp.S3BucketName,
                S3KeyPrefix: resp.S3KeyPrefix,
                SnsTopicArn: resp.SnsTopicARN,
                IncludeGlobalServiceEvents: resp.IncludeGlobalServiceEvents,
                IsMultiRegionTrail: resp.IsMultiRegionTrail,
                CloudWatchLogsLogGroupArn: resp.CloudWatchLogsLogGroupArn,
                CloudWatchLogsRoleArn: resp.CloudWatchLogsRoleArn,
                KmsKeyId: resp.KmsKeyId,
                IsOrganizationTrail: resp.IsOrganizationTrail);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update trail");
        }
    }

    // -----------------------------------------------------------------------
    // Logging control
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start logging for a trail.
    /// </summary>
    public static async Task<StartLoggingResult> StartLoggingAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartLoggingAsync(new StartLoggingRequest { Name = name });
            return new StartLoggingResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start logging for trail '{name}'");
        }
    }

    /// <summary>
    /// Stop logging for a trail.
    /// </summary>
    public static async Task<StopLoggingResult> StopLoggingAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopLoggingAsync(new StopLoggingRequest { Name = name });
            return new StopLoggingResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop logging for trail '{name}'");
        }
    }

    // -----------------------------------------------------------------------
    // Event lookup
    // -----------------------------------------------------------------------

    /// <summary>
    /// Look up management events captured by CloudTrail.
    /// </summary>
    public static async Task<LookupEventsResult> LookupEventsAsync(
        LookupEventsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.LookupEventsAsync(request);
            return new LookupEventsResult(
                Events: resp.Events,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to lookup events");
        }
    }

    // -----------------------------------------------------------------------
    // Event selectors
    // -----------------------------------------------------------------------

    /// <summary>
    /// Configure event selectors for a trail.
    /// </summary>
    public static async Task<PutEventSelectorsResult> PutEventSelectorsAsync(
        PutEventSelectorsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutEventSelectorsAsync(request);
            return new PutEventSelectorsResult(
                TrailArn: resp.TrailARN,
                EventSelectors: resp.EventSelectors,
                AdvancedEventSelectors: resp.AdvancedEventSelectors);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put event selectors");
        }
    }

    /// <summary>
    /// Get event selectors for a trail.
    /// </summary>
    public static async Task<GetEventSelectorsResult> GetEventSelectorsAsync(
        string trailName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEventSelectorsAsync(new GetEventSelectorsRequest
            {
                TrailName = trailName
            });
            return new GetEventSelectorsResult(
                TrailArn: resp.TrailARN,
                EventSelectors: resp.EventSelectors,
                AdvancedEventSelectors: resp.AdvancedEventSelectors);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get event selectors for trail '{trailName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Insight selectors
    // -----------------------------------------------------------------------

    /// <summary>
    /// Configure insight selectors for a trail.
    /// </summary>
    public static async Task<PutInsightSelectorsResult> PutInsightSelectorsAsync(
        PutInsightSelectorsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutInsightSelectorsAsync(request);
            return new PutInsightSelectorsResult(
                TrailArn: resp.TrailARN,
                InsightSelectors: resp.InsightSelectors);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put insight selectors");
        }
    }

    /// <summary>
    /// Get insight selectors for a trail.
    /// </summary>
    public static async Task<GetInsightSelectorsResult> GetInsightSelectorsAsync(
        string trailName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInsightSelectorsAsync(new GetInsightSelectorsRequest
            {
                TrailName = trailName
            });
            return new GetInsightSelectorsResult(
                TrailArn: resp.TrailARN,
                InsightSelectors: resp.InsightSelectors);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get insight selectors for trail '{trailName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Add tags to a CloudTrail resource.
    /// </summary>
    public static async Task<CloudTrailAddTagsResult> AddTagsAsync(
        AddTagsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsAsync(request);
            return new CloudTrailAddTagsResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add tags");
        }
    }

    /// <summary>
    /// Remove tags from a CloudTrail resource.
    /// </summary>
    public static async Task<CloudTrailRemoveTagsResult> RemoveTagsAsync(
        RemoveTagsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsAsync(request);
            return new CloudTrailRemoveTagsResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove tags");
        }
    }

    /// <summary>
    /// List tags for CloudTrail resources.
    /// </summary>
    public static async Task<CloudTrailListTagsResult> ListTagsAsync(
        ListTagsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsAsync(request);
            return new CloudTrailListTagsResult(
                ResourceTagList: resp.ResourceTagList,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags");
        }
    }

    // -----------------------------------------------------------------------
    // Lake queries
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get the results of a CloudTrail Lake query.
    /// </summary>
    public static async Task<GetQueryResultsResult> GetQueryResultsAsync(
        string queryId,
        string? nextToken = null,
        int? maxQueryResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetQueryResultsRequest { QueryId = queryId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxQueryResults.HasValue) request.MaxQueryResults = maxQueryResults.Value;

        try
        {
            var resp = await client.GetQueryResultsAsync(request);
            return new GetQueryResultsResult(
                QueryStatus: resp.QueryStatus?.Value,
                QueryStatistics: resp.QueryStatistics,
                QueryResultRows: resp.QueryResultRows,
                NextToken: resp.NextToken,
                ErrorMessage: resp.ErrorMessage);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get query results for query '{queryId}'");
        }
    }

    /// <summary>
    /// Start a CloudTrail Lake query.
    /// </summary>
    public static async Task<StartQueryResult> StartQueryAsync(
        StartQueryRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartQueryAsync(request);
            return new StartQueryResult(QueryId: resp.QueryId);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start query");
        }
    }

    /// <summary>
    /// Cancel a running CloudTrail Lake query.
    /// </summary>
    public static async Task<CancelQueryResult> CancelQueryAsync(
        string queryId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelQueryAsync(new CancelQueryRequest
            {
                QueryId = queryId
            });
            return new CancelQueryResult(
                QueryId: resp.QueryId,
                QueryStatus: resp.QueryStatus?.Value);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel query '{queryId}'");
        }
    }

    /// <summary>
    /// List CloudTrail Lake queries.
    /// </summary>
    public static async Task<ListQueriesResult> ListQueriesAsync(
        ListQueriesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListQueriesAsync(request);
            return new ListQueriesResult(
                Queries: resp.Queries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list queries");
        }
    }

    // -----------------------------------------------------------------------
    // Event data stores
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a CloudTrail Lake event data store.
    /// </summary>
    public static async Task<CreateEventDataStoreResult> CreateEventDataStoreAsync(
        CreateEventDataStoreRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEventDataStoreAsync(request);
            return new CreateEventDataStoreResult(
                EventDataStoreArn: resp.EventDataStoreArn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                MultiRegionEnabled: resp.MultiRegionEnabled,
                OrganizationEnabled: resp.OrganizationEnabled,
                RetentionPeriod: resp.RetentionPeriod,
                TerminationProtectionEnabled: resp.TerminationProtectionEnabled,
                AdvancedEventSelectors: resp.AdvancedEventSelectors,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp,
                KmsKeyId: resp.KmsKeyId);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create event data store");
        }
    }

    /// <summary>
    /// Delete a CloudTrail Lake event data store.
    /// </summary>
    public static async Task<DeleteEventDataStoreResult> DeleteEventDataStoreAsync(
        string eventDataStore,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteEventDataStoreAsync(new DeleteEventDataStoreRequest
            {
                EventDataStore = eventDataStore
            });
            return new DeleteEventDataStoreResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete event data store '{eventDataStore}'");
        }
    }

    /// <summary>
    /// Get details for an event data store.
    /// </summary>
    public static async Task<GetEventDataStoreResult> GetEventDataStoreAsync(
        string eventDataStore,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEventDataStoreAsync(new GetEventDataStoreRequest
            {
                EventDataStore = eventDataStore
            });
            return new GetEventDataStoreResult(
                EventDataStoreArn: resp.EventDataStoreArn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                MultiRegionEnabled: resp.MultiRegionEnabled,
                OrganizationEnabled: resp.OrganizationEnabled,
                RetentionPeriod: resp.RetentionPeriod,
                TerminationProtectionEnabled: resp.TerminationProtectionEnabled,
                AdvancedEventSelectors: resp.AdvancedEventSelectors,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp,
                KmsKeyId: resp.KmsKeyId);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get event data store '{eventDataStore}'");
        }
    }

    /// <summary>
    /// List CloudTrail Lake event data stores.
    /// </summary>
    public static async Task<ListEventDataStoresResult> ListEventDataStoresAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEventDataStoresRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListEventDataStoresAsync(request);
            return new ListEventDataStoresResult(
                EventDataStores: resp.EventDataStores,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list event data stores");
        }
    }

    /// <summary>
    /// Update a CloudTrail Lake event data store.
    /// </summary>
    public static async Task<UpdateEventDataStoreResult> UpdateEventDataStoreAsync(
        UpdateEventDataStoreRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateEventDataStoreAsync(request);
            return new UpdateEventDataStoreResult(
                EventDataStoreArn: resp.EventDataStoreArn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                MultiRegionEnabled: resp.MultiRegionEnabled,
                OrganizationEnabled: resp.OrganizationEnabled,
                RetentionPeriod: resp.RetentionPeriod,
                TerminationProtectionEnabled: resp.TerminationProtectionEnabled,
                AdvancedEventSelectors: resp.AdvancedEventSelectors,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp,
                KmsKeyId: resp.KmsKeyId);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update event data store");
        }
    }

    // -----------------------------------------------------------------------
    // Imports
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start a CloudTrail Lake import.
    /// </summary>
    public static async Task<StartImportResult> StartImportAsync(
        StartImportRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartImportAsync(request);
            return new StartImportResult(
                ImportId: resp.ImportId,
                ImportStatus: resp.ImportStatus?.Value,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start import");
        }
    }

    /// <summary>
    /// Stop a CloudTrail Lake import.
    /// </summary>
    public static async Task<StopImportResult> StopImportAsync(
        string importId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopImportAsync(new StopImportRequest
            {
                ImportId = importId
            });
            return new StopImportResult(
                ImportId: resp.ImportId,
                ImportStatus: resp.ImportStatus?.Value,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop import '{importId}'");
        }
    }

    /// <summary>
    /// Get details for a CloudTrail Lake import.
    /// </summary>
    public static async Task<GetImportResult> GetImportAsync(
        string importId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetImportAsync(new GetImportRequest
            {
                ImportId = importId
            });
            return new GetImportResult(
                ImportId: resp.ImportId,
                ImportStatus: resp.ImportStatus?.Value,
                CreatedTimestamp: resp.CreatedTimestamp,
                UpdatedTimestamp: resp.UpdatedTimestamp,
                StartEventTime: resp.StartEventTime,
                EndEventTime: resp.EndEventTime);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get import '{importId}'");
        }
    }

    /// <summary>
    /// List CloudTrail Lake imports.
    /// </summary>
    public static async Task<CloudTrailListImportsResult> ListImportsAsync(
        ListImportsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListImportsAsync(request);
            return new CloudTrailListImportsResult(
                Imports: resp.Imports,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list imports");
        }
    }

    // -----------------------------------------------------------------------
    // Channels
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a CloudTrail channel.
    /// </summary>
    public static async Task<CreateChannelResult> CreateChannelAsync(
        CreateChannelRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateChannelAsync(request);
            return new CreateChannelResult(
                ChannelArn: resp.ChannelArn,
                Name: resp.Name,
                Source: resp.Source);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create channel");
        }
    }

    /// <summary>
    /// Delete a CloudTrail channel.
    /// </summary>
    public static async Task<DeleteChannelResult> DeleteChannelAsync(
        string channel,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteChannelAsync(new DeleteChannelRequest
            {
                Channel = channel
            });
            return new DeleteChannelResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete channel '{channel}'");
        }
    }

    /// <summary>
    /// Get details for a CloudTrail channel.
    /// </summary>
    public static async Task<GetChannelResult> GetChannelAsync(
        string channel,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetChannelAsync(new GetChannelRequest
            {
                Channel = channel
            });
            return new GetChannelResult(
                ChannelArn: resp.ChannelArn,
                Name: resp.Name,
                Source: resp.Source,
                Destinations: resp.Destinations);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get channel '{channel}'");
        }
    }

    /// <summary>
    /// List CloudTrail channels.
    /// </summary>
    public static async Task<ListChannelsResult> ListChannelsAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListChannelsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListChannelsAsync(request);
            return new ListChannelsResult(
                Channels: resp.Channels,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list channels");
        }
    }

    /// <summary>
    /// Update a CloudTrail channel.
    /// </summary>
    public static async Task<UpdateChannelResult> UpdateChannelAsync(
        UpdateChannelRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateChannelAsync(request);
            return new UpdateChannelResult(
                ChannelArn: resp.ChannelArn,
                Name: resp.Name,
                Source: resp.Source,
                Destinations: resp.Destinations);
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update channel");
        }
    }

    // -----------------------------------------------------------------------
    // Organization delegated admin
    // -----------------------------------------------------------------------

    /// <summary>
    /// Register an organization member account as a delegated administrator for CloudTrail.
    /// </summary>
    public static async Task<RegisterOrganizationDelegatedAdminResult>
        RegisterOrganizationDelegatedAdminAsync(
            string memberAccountId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RegisterOrganizationDelegatedAdminAsync(
                new RegisterOrganizationDelegatedAdminRequest
                {
                    MemberAccountId = memberAccountId
                });
            return new RegisterOrganizationDelegatedAdminResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to register delegated admin '{memberAccountId}'");
        }
    }

    /// <summary>
    /// Deregister an organization member account as a delegated administrator for CloudTrail.
    /// </summary>
    public static async Task<DeregisterOrganizationDelegatedAdminResult>
        DeregisterOrganizationDelegatedAdminAsync(
            string delegatedAdminAccountId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeregisterOrganizationDelegatedAdminAsync(
                new DeregisterOrganizationDelegatedAdminRequest
                {
                    DelegatedAdminAccountId = delegatedAdminAccountId
                });
            return new DeregisterOrganizationDelegatedAdminResult();
        }
        catch (AmazonCloudTrailException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister delegated admin '{delegatedAdminAccountId}'");
        }
    }
}
