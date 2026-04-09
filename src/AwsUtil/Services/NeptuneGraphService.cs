using Amazon;
using Amazon.NeptuneGraph;
using Amazon.NeptuneGraph.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record NeptuneGraphCreateGraphResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphDeleteGraphResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphGetGraphResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null,
    string? Endpoint = null);

public sealed record NeptuneGraphListGraphsResult(
    List<GraphSummary>? Graphs = null,
    string? NextToken = null);

public sealed record NeptuneGraphUpdateGraphResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphResetGraphResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphCreateSnapshotResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphDeleteSnapshotResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphGetSnapshotResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null,
    string? SourceGraphId = null);

public sealed record NeptuneGraphListSnapshotsResult(
    List<GraphSnapshotSummary>? Snapshots = null,
    string? NextToken = null);

public sealed record NeptuneGraphRestoreFromSnapshotResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? Status = null);

public sealed record NeptuneGraphCreatePrivateEndpointResult(
    string? VpcId = null,
    string? SubnetIds = null,
    string? Status = null);

public sealed record NeptuneGraphDeletePrivateEndpointResult(
    string? VpcId = null,
    string? Status = null);

public sealed record NeptuneGraphGetPrivateEndpointResult(
    string? VpcId = null,
    string? VpcEndpointId = null,
    string? Status = null);

public sealed record NeptuneGraphListPrivateEndpointsResult(
    List<PrivateGraphEndpointSummary>? Endpoints = null,
    string? NextToken = null);

public sealed record NeptuneGraphExecuteQueryResult(
    string? Payload = null);

public sealed record NeptuneGraphGetQueryResult(
    string? Id = null,
    string? QueryString = null,
    string? State = null);

public sealed record NeptuneGraphListQueriesResult(
    List<QuerySummary>? Queries = null);

public sealed record NeptuneGraphCreateGraphUsingImportTaskResult(
    string? TaskId = null,
    string? GraphId = null,
    string? Status = null,
    string? Source = null);

public sealed record NeptuneGraphGetImportTaskResult(
    string? TaskId = null,
    string? GraphId = null,
    string? Status = null,
    string? Source = null);

public sealed record NeptuneGraphListImportTasksResult(
    List<ImportTaskSummary>? Tasks = null,
    string? NextToken = null);

public sealed record NeptuneGraphCancelImportTaskResult(
    string? TaskId = null,
    string? GraphId = null,
    string? Status = null);

public sealed record NeptuneGraphListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Neptune Analytics (Neptune Graph).
/// </summary>
public static class NeptuneGraphService
{
    private static AmazonNeptuneGraphClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonNeptuneGraphClient>(region);

    // ── Graph operations ────────────────────────────────────────────

    /// <summary>
    /// Create a new Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphCreateGraphResult> CreateGraphAsync(
        CreateGraphRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGraphAsync(request);
            return new NeptuneGraphCreateGraphResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune graph");
        }
    }

    /// <summary>
    /// Delete a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphDeleteGraphResult> DeleteGraphAsync(
        string graphIdentifier,
        bool skipSnapshot = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteGraphRequest
        {
            GraphIdentifier = graphIdentifier,
            SkipSnapshot = skipSnapshot
        };

        try
        {
            var resp = await client.DeleteGraphAsync(request);
            return new NeptuneGraphDeleteGraphResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune graph '{graphIdentifier}'");
        }
    }

    /// <summary>
    /// Get details of a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphGetGraphResult> GetGraphAsync(
        string graphIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGraphAsync(new GetGraphRequest
            {
                GraphIdentifier = graphIdentifier
            });
            return new NeptuneGraphGetGraphResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                Endpoint: resp.Endpoint);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Neptune graph '{graphIdentifier}'");
        }
    }

    /// <summary>
    /// List Neptune Analytics graphs.
    /// </summary>
    public static async Task<NeptuneGraphListGraphsResult> ListGraphsAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGraphsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListGraphsAsync(request);
            return new NeptuneGraphListGraphsResult(
                Graphs: resp.Graphs,
                NextToken: resp.NextToken);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Neptune graphs");
        }
    }

    /// <summary>
    /// Update a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphUpdateGraphResult> UpdateGraphAsync(
        UpdateGraphRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateGraphAsync(request);
            return new NeptuneGraphUpdateGraphResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Neptune graph");
        }
    }

    /// <summary>
    /// Reset a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphResetGraphResult> ResetGraphAsync(
        string graphIdentifier,
        bool skipSnapshot = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ResetGraphRequest
        {
            GraphIdentifier = graphIdentifier,
            SkipSnapshot = skipSnapshot
        };

        try
        {
            var resp = await client.ResetGraphAsync(request);
            return new NeptuneGraphResetGraphResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reset Neptune graph '{graphIdentifier}'");
        }
    }

    // ── Snapshot operations ─────────────────────────────────────────

    /// <summary>
    /// Create a Neptune Analytics graph snapshot.
    /// </summary>
    public static async Task<NeptuneGraphCreateSnapshotResult>
        CreateGraphSnapshotAsync(
            string graphIdentifier,
            string snapshotName,
            Dictionary<string, string>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGraphSnapshotRequest
        {
            GraphIdentifier = graphIdentifier,
            SnapshotName = snapshotName
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateGraphSnapshotAsync(request);
            return new NeptuneGraphCreateSnapshotResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune graph snapshot");
        }
    }

    /// <summary>
    /// Delete a Neptune Analytics graph snapshot.
    /// </summary>
    public static async Task<NeptuneGraphDeleteSnapshotResult>
        DeleteGraphSnapshotAsync(
            string snapshotIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGraphSnapshotAsync(
                new DeleteGraphSnapshotRequest
                {
                    SnapshotIdentifier = snapshotIdentifier
                });
            return new NeptuneGraphDeleteSnapshotResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune graph snapshot '{snapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Get details of a Neptune Analytics graph snapshot.
    /// </summary>
    public static async Task<NeptuneGraphGetSnapshotResult>
        GetGraphSnapshotAsync(
            string snapshotIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGraphSnapshotAsync(
                new GetGraphSnapshotRequest
                {
                    SnapshotIdentifier = snapshotIdentifier
                });
            return new NeptuneGraphGetSnapshotResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                SourceGraphId: resp.SourceGraphId);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Neptune graph snapshot '{snapshotIdentifier}'");
        }
    }

    /// <summary>
    /// List Neptune Analytics graph snapshots.
    /// </summary>
    public static async Task<NeptuneGraphListSnapshotsResult>
        ListGraphSnapshotsAsync(
            string? graphIdentifier = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGraphSnapshotsRequest();
        if (graphIdentifier != null) request.GraphIdentifier = graphIdentifier;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListGraphSnapshotsAsync(request);
            return new NeptuneGraphListSnapshotsResult(
                Snapshots: resp.GraphSnapshots,
                NextToken: resp.NextToken);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Neptune graph snapshots");
        }
    }

    /// <summary>
    /// Restore a Neptune Analytics graph from a snapshot.
    /// </summary>
    public static async Task<NeptuneGraphRestoreFromSnapshotResult>
        RestoreGraphFromSnapshotAsync(
            RestoreGraphFromSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreGraphFromSnapshotAsync(request);
            return new NeptuneGraphRestoreFromSnapshotResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore Neptune graph from snapshot");
        }
    }

    // ── Private endpoint operations ─────────────────────────────────

    /// <summary>
    /// Create a private graph endpoint.
    /// </summary>
    public static async Task<NeptuneGraphCreatePrivateEndpointResult>
        CreatePrivateGraphEndpointAsync(
            CreatePrivateGraphEndpointRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePrivateGraphEndpointAsync(request);
            return new NeptuneGraphCreatePrivateEndpointResult(
                VpcId: resp.VpcId,
                SubnetIds: resp.SubnetIds != null
                    ? string.Join(",", resp.SubnetIds) : null,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune private graph endpoint");
        }
    }

    /// <summary>
    /// Delete a private graph endpoint.
    /// </summary>
    public static async Task<NeptuneGraphDeletePrivateEndpointResult>
        DeletePrivateGraphEndpointAsync(
            string graphIdentifier,
            string vpcId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeletePrivateGraphEndpointAsync(
                new DeletePrivateGraphEndpointRequest
                {
                    GraphIdentifier = graphIdentifier,
                    VpcId = vpcId
                });
            return new NeptuneGraphDeletePrivateEndpointResult(
                VpcId: resp.VpcId,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune private graph endpoint for VPC '{vpcId}'");
        }
    }

    /// <summary>
    /// Get details of a private graph endpoint.
    /// </summary>
    public static async Task<NeptuneGraphGetPrivateEndpointResult>
        GetPrivateGraphEndpointAsync(
            string graphIdentifier,
            string vpcId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPrivateGraphEndpointAsync(
                new GetPrivateGraphEndpointRequest
                {
                    GraphIdentifier = graphIdentifier,
                    VpcId = vpcId
                });
            return new NeptuneGraphGetPrivateEndpointResult(
                VpcId: resp.VpcId,
                VpcEndpointId: resp.VpcEndpointId,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Neptune private graph endpoint for VPC '{vpcId}'");
        }
    }

    /// <summary>
    /// List private graph endpoints.
    /// </summary>
    public static async Task<NeptuneGraphListPrivateEndpointsResult>
        ListPrivateGraphEndpointsAsync(
            string graphIdentifier,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPrivateGraphEndpointsRequest
        {
            GraphIdentifier = graphIdentifier
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListPrivateGraphEndpointsAsync(request);
            return new NeptuneGraphListPrivateEndpointsResult(
                Endpoints: resp.PrivateGraphEndpoints,
                NextToken: resp.NextToken);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Neptune private graph endpoints");
        }
    }

    // ── Query operations ────────────────────────────────────────────

    /// <summary>
    /// Execute a query against a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphExecuteQueryResult> ExecuteQueryAsync(
        ExecuteQueryRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ExecuteQueryAsync(request);
            using var reader = new System.IO.StreamReader(resp.Payload);
            var payloadStr = await reader.ReadToEndAsync();
            return new NeptuneGraphExecuteQueryResult(
                Payload: payloadStr);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to execute Neptune graph query");
        }
    }

    /// <summary>
    /// Get details of a running query.
    /// </summary>
    public static async Task<NeptuneGraphGetQueryResult> GetQueryAsync(
        string graphIdentifier,
        string queryId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueryAsync(new GetQueryRequest
            {
                GraphIdentifier = graphIdentifier,
                QueryId = queryId
            });
            return new NeptuneGraphGetQueryResult(
                Id: resp.Id,
                QueryString: resp.QueryString,
                State: resp.State?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Neptune graph query '{queryId}'");
        }
    }

    /// <summary>
    /// List queries running on a Neptune Analytics graph.
    /// </summary>
    public static async Task<NeptuneGraphListQueriesResult> ListQueriesAsync(
        string graphIdentifier,
        int maxResults,
        string? state = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListQueriesRequest
        {
            GraphIdentifier = graphIdentifier,
            MaxResults = maxResults
        };
        if (state != null) request.State = state;

        try
        {
            var resp = await client.ListQueriesAsync(request);
            return new NeptuneGraphListQueriesResult(
                Queries: resp.Queries);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Neptune graph queries");
        }
    }

    /// <summary>
    /// Cancel a running query on a Neptune Analytics graph.
    /// </summary>
    public static async Task CancelQueryAsync(
        string graphIdentifier,
        string queryId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CancelQueryAsync(new CancelQueryRequest
            {
                GraphIdentifier = graphIdentifier,
                QueryId = queryId
            });
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel Neptune graph query '{queryId}'");
        }
    }

    // ── Import task operations ──────────────────────────────────────

    /// <summary>
    /// Create a Neptune Analytics graph using an import task.
    /// </summary>
    public static async Task<NeptuneGraphCreateGraphUsingImportTaskResult>
        CreateGraphUsingImportTaskAsync(
            CreateGraphUsingImportTaskRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGraphUsingImportTaskAsync(request);
            return new NeptuneGraphCreateGraphUsingImportTaskResult(
                TaskId: resp.TaskId,
                GraphId: resp.GraphId,
                Status: resp.Status?.Value,
                Source: resp.Source);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune graph using import task");
        }
    }

    /// <summary>
    /// Get details of an import task.
    /// </summary>
    public static async Task<NeptuneGraphGetImportTaskResult>
        GetImportTaskAsync(
            string taskIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetImportTaskAsync(
                new GetImportTaskRequest
                {
                    TaskIdentifier = taskIdentifier
                });
            return new NeptuneGraphGetImportTaskResult(
                TaskId: resp.TaskId,
                GraphId: resp.GraphId,
                Status: resp.Status?.Value,
                Source: resp.Source);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Neptune graph import task '{taskIdentifier}'");
        }
    }

    /// <summary>
    /// List import tasks for Neptune Analytics.
    /// </summary>
    public static async Task<NeptuneGraphListImportTasksResult>
        ListImportTasksAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListImportTasksRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListImportTasksAsync(request);
            return new NeptuneGraphListImportTasksResult(
                Tasks: resp.Tasks,
                NextToken: resp.NextToken);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Neptune graph import tasks");
        }
    }

    /// <summary>
    /// Cancel an import task.
    /// </summary>
    public static async Task<NeptuneGraphCancelImportTaskResult>
        CancelImportTaskAsync(
            string taskIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelImportTaskAsync(
                new CancelImportTaskRequest
                {
                    TaskIdentifier = taskIdentifier
                });
            return new NeptuneGraphCancelImportTaskResult(
                TaskId: resp.TaskId,
                GraphId: resp.GraphId,
                Status: resp.Status?.Value);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel Neptune graph import task '{taskIdentifier}'");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Tag a Neptune Analytics resource.
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
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag Neptune graph resource");
        }
    }

    /// <summary>
    /// Untag a Neptune Analytics resource.
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
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag Neptune graph resource");
        }
    }

    /// <summary>
    /// List tags for a Neptune Analytics resource.
    /// </summary>
    public static async Task<NeptuneGraphListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest
                {
                    ResourceArn = resourceArn
                });
            return new NeptuneGraphListTagsForResourceResult(
                Tags: resp.Tags);
        }
        catch (AmazonNeptuneGraphException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for Neptune graph resource");
        }
    }
}
