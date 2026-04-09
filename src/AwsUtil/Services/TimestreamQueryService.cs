using Amazon;
using Amazon.TimestreamQuery;
using Amazon.TimestreamQuery.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record TsqQueryResult(
    List<Row>? Rows = null,
    List<ColumnInfo>? ColumnInfo = null,
    string? QueryId = null,
    string? NextToken = null);

public sealed record TsqCancelQueryResult(string? CancellationMessage = null);

public sealed record TsqDescribeEndpointsResult(
    List<Amazon.TimestreamQuery.Model.Endpoint>? Endpoints = null);

public sealed record TsqCreateScheduledQueryResult(string? Arn = null);

public sealed record TsqDeleteScheduledQueryResult(bool Success = true);

public sealed record TsqDescribeScheduledQueryResult(
    ScheduledQueryDescription? ScheduledQuery = null);

public sealed record TsqListScheduledQueriesResult(
    List<ScheduledQuery>? ScheduledQueries = null,
    string? NextToken = null);

public sealed record TsqUpdateScheduledQueryResult(bool Success = true);

public sealed record TsqExecuteScheduledQueryResult(bool Success = true);

public sealed record TsqPrepareQueryResult(
    string? QueryString = null,
    List<SelectColumn>? Columns = null,
    List<ParameterMapping>? Parameters = null);

public sealed record TsqTagResourceResult(bool Success = true);
public sealed record TsqUntagResourceResult(bool Success = true);

public sealed record TsqListTagsForResourceResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

public sealed record TsqDescribeAccountSettingsResult(
    int? MaxQueryTCU = null,
    QueryPricingModel? QueryPricingModel = null);

public sealed record TsqUpdateAccountSettingsResult(
    int? MaxQueryTCU = null,
    QueryPricingModel? QueryPricingModel = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Timestream Query.
/// </summary>
public static class TimestreamQueryService
{
    private static AmazonTimestreamQueryClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTimestreamQueryClient>(region);

    /// <summary>
    /// Execute a Timestream query.
    /// </summary>
    public static async Task<TsqQueryResult> QueryAsync(
        string queryString,
        string? clientToken = null,
        string? nextToken = null,
        int? maxRows = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new QueryRequest { QueryString = queryString };
        if (clientToken != null) request.ClientToken = clientToken;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxRows.HasValue) request.MaxRows = maxRows.Value;

        try
        {
            var resp = await client.QueryAsync(request);
            return new TsqQueryResult(
                Rows: resp.Rows,
                ColumnInfo: resp.ColumnInfo,
                QueryId: resp.QueryId,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to execute Timestream query");
        }
    }

    /// <summary>
    /// Cancel a running Timestream query.
    /// </summary>
    public static async Task<TsqCancelQueryResult> CancelQueryAsync(
        string queryId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelQueryAsync(
                new CancelQueryRequest { QueryId = queryId });
            return new TsqCancelQueryResult(
                CancellationMessage: resp.CancellationMessage);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel Timestream query '{queryId}'");
        }
    }

    /// <summary>
    /// Describe Timestream query endpoints.
    /// </summary>
    public static async Task<TsqDescribeEndpointsResult> DescribeEndpointsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeEndpointsAsync(
                new DescribeEndpointsRequest());
            return new TsqDescribeEndpointsResult(Endpoints: resp.Endpoints);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Timestream query endpoints");
        }
    }

    /// <summary>
    /// Create a scheduled query.
    /// </summary>
    public static async Task<TsqCreateScheduledQueryResult> CreateScheduledQueryAsync(
        CreateScheduledQueryRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateScheduledQueryAsync(request);
            return new TsqCreateScheduledQueryResult(Arn: resp.Arn);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create scheduled query");
        }
    }

    /// <summary>
    /// Delete a scheduled query.
    /// </summary>
    public static async Task<TsqDeleteScheduledQueryResult> DeleteScheduledQueryAsync(
        string scheduledQueryArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteScheduledQueryAsync(
                new DeleteScheduledQueryRequest
                {
                    ScheduledQueryArn = scheduledQueryArn
                });
            return new TsqDeleteScheduledQueryResult();
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete scheduled query '{scheduledQueryArn}'");
        }
    }

    /// <summary>
    /// Describe a scheduled query.
    /// </summary>
    public static async Task<TsqDescribeScheduledQueryResult> DescribeScheduledQueryAsync(
        string scheduledQueryArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeScheduledQueryAsync(
                new DescribeScheduledQueryRequest
                {
                    ScheduledQueryArn = scheduledQueryArn
                });
            return new TsqDescribeScheduledQueryResult(
                ScheduledQuery: resp.ScheduledQuery);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe scheduled query '{scheduledQueryArn}'");
        }
    }

    /// <summary>
    /// List scheduled queries.
    /// </summary>
    public static async Task<TsqListScheduledQueriesResult> ListScheduledQueriesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListScheduledQueriesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListScheduledQueriesAsync(request);
            return new TsqListScheduledQueriesResult(
                ScheduledQueries: resp.ScheduledQueries,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list scheduled queries");
        }
    }

    /// <summary>
    /// Update a scheduled query.
    /// </summary>
    public static async Task<TsqUpdateScheduledQueryResult> UpdateScheduledQueryAsync(
        string scheduledQueryArn,
        ScheduledQueryState state,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateScheduledQueryAsync(
                new UpdateScheduledQueryRequest
                {
                    ScheduledQueryArn = scheduledQueryArn,
                    State = state
                });
            return new TsqUpdateScheduledQueryResult();
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update scheduled query '{scheduledQueryArn}'");
        }
    }

    /// <summary>
    /// Manually execute a scheduled query.
    /// </summary>
    public static async Task<TsqExecuteScheduledQueryResult> ExecuteScheduledQueryAsync(
        string scheduledQueryArn,
        DateTime invocationTime,
        string? clientToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExecuteScheduledQueryRequest
        {
            ScheduledQueryArn = scheduledQueryArn,
            InvocationTime = invocationTime
        };
        if (clientToken != null) request.ClientToken = clientToken;

        try
        {
            await client.ExecuteScheduledQueryAsync(request);
            return new TsqExecuteScheduledQueryResult();
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to execute scheduled query '{scheduledQueryArn}'");
        }
    }

    /// <summary>
    /// Prepare a Timestream query (dry-run for schema discovery).
    /// </summary>
    public static async Task<TsqPrepareQueryResult> PrepareQueryAsync(
        string queryString,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PrepareQueryAsync(
                new PrepareQueryRequest { QueryString = queryString });
            return new TsqPrepareQueryResult(
                QueryString: resp.QueryString,
                Columns: resp.Columns,
                Parameters: resp.Parameters);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to prepare Timestream query");
        }
    }

    /// <summary>
    /// Tag a Timestream query resource.
    /// </summary>
    public static async Task<TsqTagResourceResult> TagResourceAsync(
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
            return new TsqTagResourceResult();
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Timestream query resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Timestream query resource.
    /// </summary>
    public static async Task<TsqUntagResourceResult> UntagResourceAsync(
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
            return new TsqUntagResourceResult();
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Timestream query resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Timestream query resource.
    /// </summary>
    public static async Task<TsqListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceARN = resourceArn
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new TsqListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Timestream query resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Describe Timestream account settings.
    /// </summary>
    public static async Task<TsqDescribeAccountSettingsResult>
        DescribeAccountSettingsAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccountSettingsAsync(
                new DescribeAccountSettingsRequest());
            return new TsqDescribeAccountSettingsResult(
                MaxQueryTCU: resp.MaxQueryTCU,
                QueryPricingModel: resp.QueryPricingModel);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Timestream account settings");
        }
    }

    /// <summary>
    /// Update Timestream account settings.
    /// </summary>
    public static async Task<TsqUpdateAccountSettingsResult>
        UpdateAccountSettingsAsync(
            int? maxQueryTcu = null,
            QueryPricingModel? queryPricingModel = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAccountSettingsRequest();
        if (maxQueryTcu.HasValue) request.MaxQueryTCU = maxQueryTcu.Value;
        if (queryPricingModel != null) request.QueryPricingModel = queryPricingModel;

        try
        {
            var resp = await client.UpdateAccountSettingsAsync(request);
            return new TsqUpdateAccountSettingsResult(
                MaxQueryTCU: resp.MaxQueryTCU,
                QueryPricingModel: resp.QueryPricingModel);
        }
        catch (AmazonTimestreamQueryException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Timestream account settings");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="QueryAsync"/>.</summary>
    public static TsqQueryResult Query(string queryString, string? clientToken = null, string? nextToken = null, int? maxRows = null, RegionEndpoint? region = null)
        => QueryAsync(queryString, clientToken, nextToken, maxRows, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelQueryAsync"/>.</summary>
    public static TsqCancelQueryResult CancelQuery(string queryId, RegionEndpoint? region = null)
        => CancelQueryAsync(queryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEndpointsAsync"/>.</summary>
    public static TsqDescribeEndpointsResult DescribeEndpoints(RegionEndpoint? region = null)
        => DescribeEndpointsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateScheduledQueryAsync"/>.</summary>
    public static TsqCreateScheduledQueryResult CreateScheduledQuery(CreateScheduledQueryRequest request, RegionEndpoint? region = null)
        => CreateScheduledQueryAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteScheduledQueryAsync"/>.</summary>
    public static TsqDeleteScheduledQueryResult DeleteScheduledQuery(string scheduledQueryArn, RegionEndpoint? region = null)
        => DeleteScheduledQueryAsync(scheduledQueryArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeScheduledQueryAsync"/>.</summary>
    public static TsqDescribeScheduledQueryResult DescribeScheduledQuery(string scheduledQueryArn, RegionEndpoint? region = null)
        => DescribeScheduledQueryAsync(scheduledQueryArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListScheduledQueriesAsync"/>.</summary>
    public static TsqListScheduledQueriesResult ListScheduledQueries(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListScheduledQueriesAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateScheduledQueryAsync"/>.</summary>
    public static TsqUpdateScheduledQueryResult UpdateScheduledQuery(string scheduledQueryArn, ScheduledQueryState state, RegionEndpoint? region = null)
        => UpdateScheduledQueryAsync(scheduledQueryArn, state, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExecuteScheduledQueryAsync"/>.</summary>
    public static TsqExecuteScheduledQueryResult ExecuteScheduledQuery(string scheduledQueryArn, DateTime invocationTime, string? clientToken = null, RegionEndpoint? region = null)
        => ExecuteScheduledQueryAsync(scheduledQueryArn, invocationTime, clientToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PrepareQueryAsync"/>.</summary>
    public static TsqPrepareQueryResult PrepareQuery(string queryString, RegionEndpoint? region = null)
        => PrepareQueryAsync(queryString, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static TsqTagResourceResult TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static TsqUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static TsqListTagsForResourceResult ListTagsForResource(string resourceArn, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAccountSettingsAsync"/>.</summary>
    public static TsqDescribeAccountSettingsResult DescribeAccountSettings(RegionEndpoint? region = null)
        => DescribeAccountSettingsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAccountSettingsAsync"/>.</summary>
    public static TsqUpdateAccountSettingsResult UpdateAccountSettings(int? maxQueryTcu = null, QueryPricingModel? queryPricingModel = null, RegionEndpoint? region = null)
        => UpdateAccountSettingsAsync(maxQueryTcu, queryPricingModel, region).GetAwaiter().GetResult();

}
