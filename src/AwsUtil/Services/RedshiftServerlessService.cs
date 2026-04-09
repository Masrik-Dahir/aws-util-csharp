using Amazon;
using Amazon.RedshiftServerless;
using Amazon.RedshiftServerless.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateRssNamespaceResult(
    Namespace? Namespace = null);

public sealed record DeleteRssNamespaceResult(
    Namespace? Namespace = null);

public sealed record GetRssNamespaceResult(
    Namespace? Namespace = null);

public sealed record ListRssNamespacesResult(
    List<Namespace>? Namespaces = null,
    string? NextToken = null);

public sealed record UpdateRssNamespaceResult(
    Namespace? Namespace = null);

public sealed record CreateRssWorkgroupResult(
    Workgroup? Workgroup = null);

public sealed record DeleteRssWorkgroupResult(
    Workgroup? Workgroup = null);

public sealed record GetRssWorkgroupResult(
    Workgroup? Workgroup = null);

public sealed record ListRssWorkgroupsResult(
    List<Workgroup>? Workgroups = null,
    string? NextToken = null);

public sealed record UpdateRssWorkgroupResult(
    Workgroup? Workgroup = null);

public sealed record CreateRssSnapshotResult(
    Amazon.RedshiftServerless.Model.Snapshot? Snapshot = null);

public sealed record DeleteRssSnapshotResult(
    Amazon.RedshiftServerless.Model.Snapshot? Snapshot = null);

public sealed record GetRssSnapshotResult(
    Amazon.RedshiftServerless.Model.Snapshot? Snapshot = null);

public sealed record ListRssSnapshotsResult(
    List<Amazon.RedshiftServerless.Model.Snapshot>? Snapshots = null,
    string? NextToken = null);

public sealed record RestoreFromRssSnapshotResult(
    string? SnapshotName = null,
    string? NamespaceName = null,
    string? WorkgroupName = null);

public sealed record ConvertRecoveryPointToRssSnapshotResult(
    Amazon.RedshiftServerless.Model.Snapshot? Snapshot = null);

public sealed record CreateRssEndpointAccessResult(
    EndpointAccess? Endpoint = null);

public sealed record DeleteRssEndpointAccessResult(
    EndpointAccess? Endpoint = null);

public sealed record GetRssEndpointAccessResult(
    EndpointAccess? Endpoint = null);

public sealed record ListRssEndpointAccessResult(
    List<EndpointAccess>? Endpoints = null,
    string? NextToken = null);

public sealed record UpdateRssEndpointAccessResult(
    EndpointAccess? Endpoint = null);

public sealed record GetRssCredentialsResult(
    string? DbUser = null,
    string? DbPassword = null,
    DateTime? Expiration = null,
    string? NextRefreshTime = null);

public sealed record GetRssRecoveryPointResult(
    RecoveryPoint? RecoveryPoint = null);

public sealed record ListRssRecoveryPointsResult(
    List<RecoveryPoint>? RecoveryPoints = null,
    string? NextToken = null);

public sealed record CreateRssUsageLimitResult(
    UsageLimit? UsageLimit = null);

public sealed record DeleteRssUsageLimitResult(
    UsageLimit? UsageLimit = null);

public sealed record GetRssUsageLimitResult(
    UsageLimit? UsageLimit = null);

public sealed record ListRssUsageLimitsResult(
    List<UsageLimit>? UsageLimits = null,
    string? NextToken = null);

public sealed record UpdateRssUsageLimitResult(
    UsageLimit? UsageLimit = null);

public sealed record ListRssTagsResult(
    List<Amazon.RedshiftServerless.Model.Tag>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Redshift Serverless.
/// </summary>
public static class RedshiftServerlessService
{
    private static AmazonRedshiftServerlessClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRedshiftServerlessClient>(region);

    // ──────────────────────── Namespaces ───────────────────────────────

    /// <summary>
    /// Create a Redshift Serverless namespace.
    /// </summary>
    public static async Task<CreateRssNamespaceResult>
        CreateNamespaceAsync(
            CreateNamespaceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateNamespaceAsync(request);
            return new CreateRssNamespaceResult(Namespace: resp.Namespace);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Redshift Serverless namespace");
        }
    }

    /// <summary>
    /// Delete a Redshift Serverless namespace.
    /// </summary>
    public static async Task<DeleteRssNamespaceResult>
        DeleteNamespaceAsync(
            string namespaceName,
            string? finalSnapshotName = null,
            int? finalSnapshotRetentionPeriod = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteNamespaceRequest
        {
            NamespaceName = namespaceName
        };
        if (finalSnapshotName != null)
            request.FinalSnapshotName = finalSnapshotName;
        if (finalSnapshotRetentionPeriod.HasValue)
            request.FinalSnapshotRetentionPeriod =
                finalSnapshotRetentionPeriod.Value;

        try
        {
            var resp = await client.DeleteNamespaceAsync(request);
            return new DeleteRssNamespaceResult(Namespace: resp.Namespace);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift Serverless namespace '{namespaceName}'");
        }
    }

    /// <summary>
    /// Get a Redshift Serverless namespace.
    /// </summary>
    public static async Task<GetRssNamespaceResult> GetNamespaceAsync(
        string namespaceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetNamespaceAsync(
                new GetNamespaceRequest { NamespaceName = namespaceName });
            return new GetRssNamespaceResult(Namespace: resp.Namespace);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Redshift Serverless namespace '{namespaceName}'");
        }
    }

    /// <summary>
    /// List Redshift Serverless namespaces.
    /// </summary>
    public static async Task<ListRssNamespacesResult> ListNamespacesAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListNamespacesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListNamespacesAsync(request);
            return new ListRssNamespacesResult(
                Namespaces: resp.Namespaces,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless namespaces");
        }
    }

    /// <summary>
    /// Update a Redshift Serverless namespace.
    /// </summary>
    public static async Task<UpdateRssNamespaceResult>
        UpdateNamespaceAsync(
            UpdateNamespaceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateNamespaceAsync(request);
            return new UpdateRssNamespaceResult(Namespace: resp.Namespace);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Redshift Serverless namespace");
        }
    }

    // ──────────────────────── Workgroups ───────────────────────────────

    /// <summary>
    /// Create a Redshift Serverless workgroup.
    /// </summary>
    public static async Task<CreateRssWorkgroupResult>
        CreateWorkgroupAsync(
            CreateWorkgroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateWorkgroupAsync(request);
            return new CreateRssWorkgroupResult(Workgroup: resp.Workgroup);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Redshift Serverless workgroup");
        }
    }

    /// <summary>
    /// Delete a Redshift Serverless workgroup.
    /// </summary>
    public static async Task<DeleteRssWorkgroupResult>
        DeleteWorkgroupAsync(
            string workgroupName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteWorkgroupAsync(
                new DeleteWorkgroupRequest
                {
                    WorkgroupName = workgroupName
                });
            return new DeleteRssWorkgroupResult(Workgroup: resp.Workgroup);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift Serverless workgroup '{workgroupName}'");
        }
    }

    /// <summary>
    /// Get a Redshift Serverless workgroup.
    /// </summary>
    public static async Task<GetRssWorkgroupResult> GetWorkgroupAsync(
        string workgroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetWorkgroupAsync(
                new GetWorkgroupRequest
                {
                    WorkgroupName = workgroupName
                });
            return new GetRssWorkgroupResult(Workgroup: resp.Workgroup);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Redshift Serverless workgroup '{workgroupName}'");
        }
    }

    /// <summary>
    /// List Redshift Serverless workgroups.
    /// </summary>
    public static async Task<ListRssWorkgroupsResult> ListWorkgroupsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWorkgroupsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListWorkgroupsAsync(request);
            return new ListRssWorkgroupsResult(
                Workgroups: resp.Workgroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless workgroups");
        }
    }

    /// <summary>
    /// Update a Redshift Serverless workgroup.
    /// </summary>
    public static async Task<UpdateRssWorkgroupResult>
        UpdateWorkgroupAsync(
            UpdateWorkgroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateWorkgroupAsync(request);
            return new UpdateRssWorkgroupResult(Workgroup: resp.Workgroup);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Redshift Serverless workgroup");
        }
    }

    // ──────────────────────── Snapshots ────────────────────────────────

    /// <summary>
    /// Create a Redshift Serverless snapshot.
    /// </summary>
    public static async Task<CreateRssSnapshotResult> CreateSnapshotAsync(
        string namespaceName,
        string snapshotName,
        int? retentionPeriod = null,
        List<Amazon.RedshiftServerless.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSnapshotRequest
        {
            NamespaceName = namespaceName,
            SnapshotName = snapshotName
        };
        if (retentionPeriod.HasValue)
            request.RetentionPeriod = retentionPeriod.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            return new CreateRssSnapshotResult(Snapshot: resp.Snapshot);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Redshift Serverless snapshot '{snapshotName}'");
        }
    }

    /// <summary>
    /// Delete a Redshift Serverless snapshot.
    /// </summary>
    public static async Task<DeleteRssSnapshotResult> DeleteSnapshotAsync(
        string snapshotName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSnapshotAsync(
                new DeleteSnapshotRequest { SnapshotName = snapshotName });
            return new DeleteRssSnapshotResult(Snapshot: resp.Snapshot);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift Serverless snapshot '{snapshotName}'");
        }
    }

    /// <summary>
    /// Get a Redshift Serverless snapshot.
    /// </summary>
    public static async Task<GetRssSnapshotResult> GetSnapshotAsync(
        string? snapshotName = null,
        string? snapshotArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSnapshotRequest();
        if (snapshotName != null) request.SnapshotName = snapshotName;
        if (snapshotArn != null) request.SnapshotArn = snapshotArn;

        try
        {
            var resp = await client.GetSnapshotAsync(request);
            return new GetRssSnapshotResult(Snapshot: resp.Snapshot);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get Redshift Serverless snapshot");
        }
    }

    /// <summary>
    /// List Redshift Serverless snapshots.
    /// </summary>
    public static async Task<ListRssSnapshotsResult> ListSnapshotsAsync(
        string? namespaceName = null,
        string? namespaceArn = null,
        DateTime? startTime = null,
        DateTime? endTime = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSnapshotsRequest();
        if (namespaceName != null) request.NamespaceName = namespaceName;
        if (namespaceArn != null) request.NamespaceArn = namespaceArn;
        if (startTime.HasValue) request.StartTime = startTime.Value;
        if (endTime.HasValue) request.EndTime = endTime.Value;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListSnapshotsAsync(request);
            return new ListRssSnapshotsResult(
                Snapshots: resp.Snapshots,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless snapshots");
        }
    }

    /// <summary>
    /// Restore a Redshift Serverless namespace from a snapshot.
    /// </summary>
    public static async Task<RestoreFromRssSnapshotResult>
        RestoreFromSnapshotAsync(
            RestoreFromSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreFromSnapshotAsync(request);
            return new RestoreFromRssSnapshotResult(
                SnapshotName: resp.SnapshotName,
                NamespaceName: resp.Namespace?.NamespaceName,
                WorkgroupName: resp.Namespace?.NamespaceName);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore Redshift Serverless from snapshot");
        }
    }

    /// <summary>
    /// Convert a recovery point to a Redshift Serverless snapshot.
    /// </summary>
    public static async Task<ConvertRecoveryPointToRssSnapshotResult>
        ConvertRecoveryPointToSnapshotAsync(
            string recoveryPointId,
            string snapshotName,
            int? retentionPeriod = null,
            List<Amazon.RedshiftServerless.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ConvertRecoveryPointToSnapshotRequest
        {
            RecoveryPointId = recoveryPointId,
            SnapshotName = snapshotName
        };
        if (retentionPeriod.HasValue)
            request.RetentionPeriod = retentionPeriod.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp =
                await client.ConvertRecoveryPointToSnapshotAsync(request);
            return new ConvertRecoveryPointToRssSnapshotResult(
                Snapshot: resp.Snapshot);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to convert recovery point '{recoveryPointId}' to snapshot");
        }
    }

    // ──────────────────── Endpoint Access ──────────────────────────────

    /// <summary>
    /// Create a Redshift Serverless endpoint access.
    /// </summary>
    public static async Task<CreateRssEndpointAccessResult>
        CreateEndpointAccessAsync(
            CreateEndpointAccessRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEndpointAccessAsync(request);
            return new CreateRssEndpointAccessResult(
                Endpoint: resp.Endpoint);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Redshift Serverless endpoint access");
        }
    }

    /// <summary>
    /// Delete a Redshift Serverless endpoint access.
    /// </summary>
    public static async Task<DeleteRssEndpointAccessResult>
        DeleteEndpointAccessAsync(
            string endpointName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteEndpointAccessAsync(
                new DeleteEndpointAccessRequest
                {
                    EndpointName = endpointName
                });
            return new DeleteRssEndpointAccessResult(
                Endpoint: resp.Endpoint);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift Serverless endpoint '{endpointName}'");
        }
    }

    /// <summary>
    /// Get a Redshift Serverless endpoint access.
    /// </summary>
    public static async Task<GetRssEndpointAccessResult>
        GetEndpointAccessAsync(
            string endpointName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEndpointAccessAsync(
                new GetEndpointAccessRequest
                {
                    EndpointName = endpointName
                });
            return new GetRssEndpointAccessResult(Endpoint: resp.Endpoint);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Redshift Serverless endpoint '{endpointName}'");
        }
    }

    /// <summary>
    /// List Redshift Serverless endpoint accesses.
    /// </summary>
    public static async Task<ListRssEndpointAccessResult>
        ListEndpointAccessAsync(
            string? workgroupName = null,
            string? vpcId = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEndpointAccessRequest();
        if (workgroupName != null) request.WorkgroupName = workgroupName;
        if (vpcId != null) request.VpcId = vpcId;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListEndpointAccessAsync(request);
            return new ListRssEndpointAccessResult(
                Endpoints: resp.Endpoints,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless endpoint accesses");
        }
    }

    /// <summary>
    /// Update a Redshift Serverless endpoint access.
    /// </summary>
    public static async Task<UpdateRssEndpointAccessResult>
        UpdateEndpointAccessAsync(
            UpdateEndpointAccessRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateEndpointAccessAsync(request);
            return new UpdateRssEndpointAccessResult(
                Endpoint: resp.Endpoint);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Redshift Serverless endpoint access");
        }
    }

    // ──────────────────────── Credentials ──────────────────────────────

    /// <summary>
    /// Get temporary credentials for a Redshift Serverless workgroup.
    /// </summary>
    public static async Task<GetRssCredentialsResult> GetCredentialsAsync(
        string workgroupName,
        string? dbName = null,
        int? durationSeconds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetCredentialsRequest
        {
            WorkgroupName = workgroupName
        };
        if (dbName != null) request.DbName = dbName;
        if (durationSeconds.HasValue)
            request.DurationSeconds = durationSeconds.Value;

        try
        {
            var resp = await client.GetCredentialsAsync(request);
            return new GetRssCredentialsResult(
                DbUser: resp.DbUser,
                DbPassword: resp.DbPassword,
                Expiration: resp.Expiration,
                NextRefreshTime: resp.NextRefreshTime?.ToString());
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get credentials for Redshift Serverless workgroup '{workgroupName}'");
        }
    }

    // ──────────────────── Recovery Points ──────────────────────────────

    /// <summary>
    /// Get a Redshift Serverless recovery point.
    /// </summary>
    public static async Task<GetRssRecoveryPointResult>
        GetRecoveryPointAsync(
            string recoveryPointId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRecoveryPointAsync(
                new GetRecoveryPointRequest
                {
                    RecoveryPointId = recoveryPointId
                });
            return new GetRssRecoveryPointResult(
                RecoveryPoint: resp.RecoveryPoint);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Redshift Serverless recovery point '{recoveryPointId}'");
        }
    }

    /// <summary>
    /// List Redshift Serverless recovery points.
    /// </summary>
    public static async Task<ListRssRecoveryPointsResult>
        ListRecoveryPointsAsync(
            string? namespaceName = null,
            string? namespaceArn = null,
            DateTime? startTime = null,
            DateTime? endTime = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRecoveryPointsRequest();
        if (namespaceName != null) request.NamespaceName = namespaceName;
        if (namespaceArn != null) request.NamespaceArn = namespaceArn;
        if (startTime.HasValue) request.StartTime = startTime.Value;
        if (endTime.HasValue) request.EndTime = endTime.Value;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRecoveryPointsAsync(request);
            return new ListRssRecoveryPointsResult(
                RecoveryPoints: resp.RecoveryPoints,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless recovery points");
        }
    }

    // ──────────────────── Usage Limits ─────────────────────────────────

    /// <summary>
    /// Create a Redshift Serverless usage limit.
    /// </summary>
    public static async Task<CreateRssUsageLimitResult>
        CreateUsageLimitAsync(
            CreateUsageLimitRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUsageLimitAsync(request);
            return new CreateRssUsageLimitResult(
                UsageLimit: resp.UsageLimit);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Redshift Serverless usage limit");
        }
    }

    /// <summary>
    /// Delete a Redshift Serverless usage limit.
    /// </summary>
    public static async Task<DeleteRssUsageLimitResult>
        DeleteUsageLimitAsync(
            string usageLimitId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteUsageLimitAsync(
                new DeleteUsageLimitRequest
                {
                    UsageLimitId = usageLimitId
                });
            return new DeleteRssUsageLimitResult(
                UsageLimit: resp.UsageLimit);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift Serverless usage limit '{usageLimitId}'");
        }
    }

    /// <summary>
    /// Get a Redshift Serverless usage limit.
    /// </summary>
    public static async Task<GetRssUsageLimitResult> GetUsageLimitAsync(
        string usageLimitId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetUsageLimitAsync(
                new GetUsageLimitRequest
                {
                    UsageLimitId = usageLimitId
                });
            return new GetRssUsageLimitResult(UsageLimit: resp.UsageLimit);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Redshift Serverless usage limit '{usageLimitId}'");
        }
    }

    /// <summary>
    /// List Redshift Serverless usage limits.
    /// </summary>
    public static async Task<ListRssUsageLimitsResult>
        ListUsageLimitsAsync(
            string? resourceArn = null,
            string? usageType = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsageLimitsRequest();
        if (resourceArn != null) request.ResourceArn = resourceArn;
        if (usageType != null)
            request.UsageType = new UsageLimitUsageType(usageType);
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListUsageLimitsAsync(request);
            return new ListRssUsageLimitsResult(
                UsageLimits: resp.UsageLimits,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Serverless usage limits");
        }
    }

    /// <summary>
    /// Update a Redshift Serverless usage limit.
    /// </summary>
    public static async Task<UpdateRssUsageLimitResult>
        UpdateUsageLimitAsync(
            UpdateUsageLimitRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateUsageLimitAsync(request);
            return new UpdateRssUsageLimitResult(
                UsageLimit: resp.UsageLimit);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Redshift Serverless usage limit");
        }
    }

    // ──────────────────────── Tags ─────────────────────────────────────

    /// <summary>
    /// Tag a Redshift Serverless resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Amazon.RedshiftServerless.Model.Tag> tags,
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
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Redshift Serverless resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Untag a Redshift Serverless resource.
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
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Redshift Serverless resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Redshift Serverless resource.
    /// </summary>
    public static async Task<ListRssTagsResult>
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
            return new ListRssTagsResult(Tags: resp.Tags);
        }
        catch (AmazonRedshiftServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Redshift Serverless resource '{resourceArn}'");
        }
    }
}
