using Amazon;
using Amazon.MemoryDB;
using Amazon.MemoryDB.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record MemoryDbCreateClusterResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDeleteClusterResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeClustersResult(
    List<Cluster>? Clusters = null,
    string? NextToken = null);

public sealed record MemoryDbUpdateClusterResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbCreateSnapshotResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDeleteSnapshotResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeSnapshotsResult(
    List<Snapshot>? Snapshots = null,
    string? NextToken = null);

public sealed record MemoryDbCopySnapshotResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbCreateSubnetGroupResult(
    string? Name = null,
    string? ARN = null,
    string? Description = null);

public sealed record MemoryDbDeleteSubnetGroupResult(
    string? Name = null,
    string? ARN = null);

public sealed record MemoryDbDescribeSubnetGroupsResult(
    List<SubnetGroup>? SubnetGroups = null,
    string? NextToken = null);

public sealed record MemoryDbUpdateSubnetGroupResult(
    string? Name = null,
    string? ARN = null,
    string? Description = null);

public sealed record MemoryDbCreateParameterGroupResult(
    string? Name = null,
    string? ARN = null,
    string? Family = null);

public sealed record MemoryDbDeleteParameterGroupResult(
    string? Name = null,
    string? ARN = null);

public sealed record MemoryDbDescribeParameterGroupsResult(
    List<ParameterGroup>? ParameterGroups = null,
    string? NextToken = null);

public sealed record MemoryDbUpdateParameterGroupResult(
    string? Name = null,
    string? ARN = null);

public sealed record MemoryDbDescribeParametersResult(
    List<Parameter>? Parameters = null,
    string? NextToken = null);

public sealed record MemoryDbResetParameterGroupResult(
    string? Name = null,
    string? ARN = null);

public sealed record MemoryDbCreateACLResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDeleteACLResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeACLsResult(
    List<ACL>? ACLs = null,
    string? NextToken = null);

public sealed record MemoryDbUpdateACLResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbCreateUserResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDeleteUserResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeUsersResult(
    List<User>? Users = null,
    string? NextToken = null);

public sealed record MemoryDbUpdateUserResult(
    string? Name = null,
    string? ARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeEventsResult(
    List<Event>? Events = null,
    string? NextToken = null);

public sealed record MemoryDbFailoverShardResult(
    string? ClusterName = null,
    string? ClusterARN = null,
    string? Status = null);

public sealed record MemoryDbDescribeServiceUpdatesResult(
    List<ServiceUpdate>? ServiceUpdates = null,
    string? NextToken = null);

public sealed record MemoryDbBatchUpdateClusterResult(
    List<Cluster>? ProcessedClusters = null,
    List<UnprocessedCluster>? UnprocessedClusters = null);

public sealed record MemoryDbListTagsResult(
    List<Tag>? TagList = null);

public sealed record MemoryDbListAllowedNodeTypeUpdatesResult(
    List<string>? ScaleUpNodeTypes = null,
    List<string>? ScaleDownNodeTypes = null);

public sealed record MemoryDbDescribeReservedNodesResult(
    List<ReservedNode>? ReservedNodes = null,
    string? NextToken = null);

public sealed record MemoryDbDescribeReservedNodesOfferingsResult(
    List<ReservedNodesOffering>? ReservedNodesOfferings = null,
    string? NextToken = null);

public sealed record MemoryDbPurchaseReservedNodesOfferingResult(
    string? ReservedNodeId = null,
    string? ReservedNodesOfferingId = null,
    string? NodeType = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon MemoryDB for Redis.
/// </summary>
public static class MemoryDbService
{
    private static AmazonMemoryDBClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonMemoryDBClient>(region);

    // ── Cluster operations ──────────────────────────────────────────

    /// <summary>
    /// Create a new MemoryDB cluster.
    /// </summary>
    public static async Task<MemoryDbCreateClusterResult> CreateClusterAsync(
        CreateClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterAsync(request);
            var c = resp.Cluster;
            return new MemoryDbCreateClusterResult(
                Name: c.Name,
                ARN: c.ARN,
                Status: c.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB cluster");
        }
    }

    /// <summary>
    /// Delete a MemoryDB cluster.
    /// </summary>
    public static async Task<MemoryDbDeleteClusterResult> DeleteClusterAsync(
        string clusterName,
        string? finalSnapshotName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteClusterRequest
        {
            ClusterName = clusterName
        };
        if (finalSnapshotName != null) request.FinalSnapshotName = finalSnapshotName;

        try
        {
            var resp = await client.DeleteClusterAsync(request);
            var c = resp.Cluster;
            return new MemoryDbDeleteClusterResult(
                Name: c.Name,
                ARN: c.ARN,
                Status: c.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB cluster '{clusterName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB clusters.
    /// </summary>
    public static async Task<MemoryDbDescribeClustersResult>
        DescribeClustersAsync(
            string? clusterName = null,
            bool? showShardDetails = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClustersRequest();
        if (clusterName != null) request.ClusterName = clusterName;
        if (showShardDetails.HasValue) request.ShowShardDetails = showShardDetails.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeClustersAsync(request);
            return new MemoryDbDescribeClustersResult(
                Clusters: resp.Clusters,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB clusters");
        }
    }

    /// <summary>
    /// Update a MemoryDB cluster.
    /// </summary>
    public static async Task<MemoryDbUpdateClusterResult> UpdateClusterAsync(
        UpdateClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateClusterAsync(request);
            var c = resp.Cluster;
            return new MemoryDbUpdateClusterResult(
                Name: c.Name,
                ARN: c.ARN,
                Status: c.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update MemoryDB cluster");
        }
    }

    // ── Snapshot operations ─────────────────────────────────────────

    /// <summary>
    /// Create a MemoryDB snapshot.
    /// </summary>
    public static async Task<MemoryDbCreateSnapshotResult>
        CreateSnapshotAsync(
            string clusterName,
            string snapshotName,
            string? kmsKeyId = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSnapshotRequest
        {
            ClusterName = clusterName,
            SnapshotName = snapshotName
        };
        if (kmsKeyId != null) request.KmsKeyId = kmsKeyId;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            var s = resp.Snapshot;
            return new MemoryDbCreateSnapshotResult(
                Name: s.Name,
                ARN: s.ARN,
                Status: s.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB snapshot");
        }
    }

    /// <summary>
    /// Delete a MemoryDB snapshot.
    /// </summary>
    public static async Task<MemoryDbDeleteSnapshotResult>
        DeleteSnapshotAsync(
            string snapshotName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSnapshotAsync(
                new DeleteSnapshotRequest
                {
                    SnapshotName = snapshotName
                });
            var s = resp.Snapshot;
            return new MemoryDbDeleteSnapshotResult(
                Name: s.Name,
                ARN: s.ARN,
                Status: s.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB snapshot '{snapshotName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB snapshots.
    /// </summary>
    public static async Task<MemoryDbDescribeSnapshotsResult>
        DescribeSnapshotsAsync(
            string? clusterName = null,
            string? snapshotName = null,
            string? source = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSnapshotsRequest();
        if (clusterName != null) request.ClusterName = clusterName;
        if (snapshotName != null) request.SnapshotName = snapshotName;
        if (source != null) request.Source = source;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeSnapshotsAsync(request);
            return new MemoryDbDescribeSnapshotsResult(
                Snapshots: resp.Snapshots,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB snapshots");
        }
    }

    /// <summary>
    /// Copy a MemoryDB snapshot.
    /// </summary>
    public static async Task<MemoryDbCopySnapshotResult> CopySnapshotAsync(
        string sourceSnapshotName,
        string targetSnapshotName,
        string? targetBucket = null,
        string? kmsKeyId = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CopySnapshotRequest
        {
            SourceSnapshotName = sourceSnapshotName,
            TargetSnapshotName = targetSnapshotName
        };
        if (targetBucket != null) request.TargetBucket = targetBucket;
        if (kmsKeyId != null) request.KmsKeyId = kmsKeyId;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CopySnapshotAsync(request);
            var s = resp.Snapshot;
            return new MemoryDbCopySnapshotResult(
                Name: s.Name,
                ARN: s.ARN,
                Status: s.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to copy MemoryDB snapshot");
        }
    }

    // ── Subnet group operations ─────────────────────────────────────

    /// <summary>
    /// Create a MemoryDB subnet group.
    /// </summary>
    public static async Task<MemoryDbCreateSubnetGroupResult>
        CreateSubnetGroupAsync(
            string subnetGroupName,
            List<string> subnetIds,
            string? description = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSubnetGroupRequest
        {
            SubnetGroupName = subnetGroupName,
            SubnetIds = subnetIds
        };
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSubnetGroupAsync(request);
            var g = resp.SubnetGroup;
            return new MemoryDbCreateSubnetGroupResult(
                Name: g.Name,
                ARN: g.ARN,
                Description: g.Description);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB subnet group");
        }
    }

    /// <summary>
    /// Delete a MemoryDB subnet group.
    /// </summary>
    public static async Task<MemoryDbDeleteSubnetGroupResult>
        DeleteSubnetGroupAsync(
            string subnetGroupName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSubnetGroupAsync(
                new DeleteSubnetGroupRequest
                {
                    SubnetGroupName = subnetGroupName
                });
            var g = resp.SubnetGroup;
            return new MemoryDbDeleteSubnetGroupResult(
                Name: g.Name,
                ARN: g.ARN);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB subnet group '{subnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB subnet groups.
    /// </summary>
    public static async Task<MemoryDbDescribeSubnetGroupsResult>
        DescribeSubnetGroupsAsync(
            string? subnetGroupName = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSubnetGroupsRequest();
        if (subnetGroupName != null) request.SubnetGroupName = subnetGroupName;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeSubnetGroupsAsync(request);
            return new MemoryDbDescribeSubnetGroupsResult(
                SubnetGroups: resp.SubnetGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB subnet groups");
        }
    }

    /// <summary>
    /// Update a MemoryDB subnet group.
    /// </summary>
    public static async Task<MemoryDbUpdateSubnetGroupResult>
        UpdateSubnetGroupAsync(
            string subnetGroupName,
            List<string>? subnetIds = null,
            string? description = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateSubnetGroupRequest
        {
            SubnetGroupName = subnetGroupName
        };
        if (subnetIds != null) request.SubnetIds = subnetIds;
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateSubnetGroupAsync(request);
            var g = resp.SubnetGroup;
            return new MemoryDbUpdateSubnetGroupResult(
                Name: g.Name,
                ARN: g.ARN,
                Description: g.Description);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MemoryDB subnet group '{subnetGroupName}'");
        }
    }

    // ── Parameter group operations ──────────────────────────────────

    /// <summary>
    /// Create a MemoryDB parameter group.
    /// </summary>
    public static async Task<MemoryDbCreateParameterGroupResult>
        CreateParameterGroupAsync(
            string parameterGroupName,
            string family,
            string? description = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateParameterGroupRequest
        {
            ParameterGroupName = parameterGroupName,
            Family = family
        };
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateParameterGroupAsync(request);
            var g = resp.ParameterGroup;
            return new MemoryDbCreateParameterGroupResult(
                Name: g.Name,
                ARN: g.ARN,
                Family: g.Family);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB parameter group");
        }
    }

    /// <summary>
    /// Delete a MemoryDB parameter group.
    /// </summary>
    public static async Task<MemoryDbDeleteParameterGroupResult>
        DeleteParameterGroupAsync(
            string parameterGroupName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteParameterGroupAsync(
                new DeleteParameterGroupRequest
                {
                    ParameterGroupName = parameterGroupName
                });
            var g = resp.ParameterGroup;
            return new MemoryDbDeleteParameterGroupResult(
                Name: g.Name,
                ARN: g.ARN);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB parameter group '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB parameter groups.
    /// </summary>
    public static async Task<MemoryDbDescribeParameterGroupsResult>
        DescribeParameterGroupsAsync(
            string? parameterGroupName = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeParameterGroupsRequest();
        if (parameterGroupName != null) request.ParameterGroupName = parameterGroupName;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeParameterGroupsAsync(request);
            return new MemoryDbDescribeParameterGroupsResult(
                ParameterGroups: resp.ParameterGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB parameter groups");
        }
    }

    /// <summary>
    /// Update a MemoryDB parameter group.
    /// </summary>
    public static async Task<MemoryDbUpdateParameterGroupResult>
        UpdateParameterGroupAsync(
            string parameterGroupName,
            List<ParameterNameValue> parameterNameValues,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateParameterGroupAsync(
                new UpdateParameterGroupRequest
                {
                    ParameterGroupName = parameterGroupName,
                    ParameterNameValues = parameterNameValues
                });
            var g = resp.ParameterGroup;
            return new MemoryDbUpdateParameterGroupResult(
                Name: g.Name,
                ARN: g.ARN);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MemoryDB parameter group '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe parameters in a MemoryDB parameter group.
    /// </summary>
    public static async Task<MemoryDbDescribeParametersResult>
        DescribeParametersAsync(
            string parameterGroupName,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeParametersRequest
        {
            ParameterGroupName = parameterGroupName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeParametersAsync(request);
            return new MemoryDbDescribeParametersResult(
                Parameters: resp.Parameters,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe MemoryDB parameters for '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Reset a MemoryDB parameter group to default values.
    /// </summary>
    public static async Task<MemoryDbResetParameterGroupResult>
        ResetParameterGroupAsync(
            string parameterGroupName,
            bool allParameters = false,
            List<ParameterNameValue>? parameterNames = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ResetParameterGroupRequest
        {
            ParameterGroupName = parameterGroupName,
            AllParameters = allParameters
        };
        if (parameterNames != null) request.ParameterNames = parameterNames.Select(p => p.ParameterName).ToList();

        try
        {
            var resp = await client.ResetParameterGroupAsync(request);
            var g = resp.ParameterGroup;
            return new MemoryDbResetParameterGroupResult(
                Name: g.Name,
                ARN: g.ARN);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reset MemoryDB parameter group '{parameterGroupName}'");
        }
    }

    // ── ACL operations ──────────────────────────────────────────────

    /// <summary>
    /// Create a MemoryDB ACL.
    /// </summary>
    public static async Task<MemoryDbCreateACLResult> CreateACLAsync(
        string aclName,
        List<string>? userNames = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateACLRequest
        {
            ACLName = aclName
        };
        if (userNames != null) request.UserNames = userNames;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateACLAsync(request);
            var a = resp.ACL;
            return new MemoryDbCreateACLResult(
                Name: a.Name,
                ARN: a.ARN,
                Status: a.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB ACL");
        }
    }

    /// <summary>
    /// Delete a MemoryDB ACL.
    /// </summary>
    public static async Task<MemoryDbDeleteACLResult> DeleteACLAsync(
        string aclName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteACLAsync(new DeleteACLRequest
            {
                ACLName = aclName
            });
            var a = resp.ACL;
            return new MemoryDbDeleteACLResult(
                Name: a.Name,
                ARN: a.ARN,
                Status: a.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB ACL '{aclName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB ACLs.
    /// </summary>
    public static async Task<MemoryDbDescribeACLsResult> DescribeACLsAsync(
        string? aclName = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeACLsRequest();
        if (aclName != null) request.ACLName = aclName;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeACLsAsync(request);
            return new MemoryDbDescribeACLsResult(
                ACLs: resp.ACLs,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB ACLs");
        }
    }

    /// <summary>
    /// Update a MemoryDB ACL.
    /// </summary>
    public static async Task<MemoryDbUpdateACLResult> UpdateACLAsync(
        string aclName,
        List<string>? userNamesToAdd = null,
        List<string>? userNamesToRemove = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateACLRequest
        {
            ACLName = aclName
        };
        if (userNamesToAdd != null) request.UserNamesToAdd = userNamesToAdd;
        if (userNamesToRemove != null) request.UserNamesToRemove = userNamesToRemove;

        try
        {
            var resp = await client.UpdateACLAsync(request);
            var a = resp.ACL;
            return new MemoryDbUpdateACLResult(
                Name: a.Name,
                ARN: a.ARN,
                Status: a.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MemoryDB ACL '{aclName}'");
        }
    }

    // ── User operations ─────────────────────────────────────────────

    /// <summary>
    /// Create a MemoryDB user.
    /// </summary>
    public static async Task<MemoryDbCreateUserResult> CreateUserAsync(
        CreateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserAsync(request);
            var u = resp.User;
            return new MemoryDbCreateUserResult(
                Name: u.Name,
                ARN: u.ARN,
                Status: u.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create MemoryDB user");
        }
    }

    /// <summary>
    /// Delete a MemoryDB user.
    /// </summary>
    public static async Task<MemoryDbDeleteUserResult> DeleteUserAsync(
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteUserAsync(new DeleteUserRequest
            {
                UserName = userName
            });
            var u = resp.User;
            return new MemoryDbDeleteUserResult(
                Name: u.Name,
                ARN: u.ARN,
                Status: u.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MemoryDB user '{userName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB users.
    /// </summary>
    public static async Task<MemoryDbDescribeUsersResult> DescribeUsersAsync(
        string? userName = null,
        List<Filter>? filters = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeUsersRequest();
        if (userName != null) request.UserName = userName;
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeUsersAsync(request);
            return new MemoryDbDescribeUsersResult(
                Users: resp.Users,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB users");
        }
    }

    /// <summary>
    /// Update a MemoryDB user.
    /// </summary>
    public static async Task<MemoryDbUpdateUserResult> UpdateUserAsync(
        UpdateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateUserAsync(request);
            var u = resp.User;
            return new MemoryDbUpdateUserResult(
                Name: u.Name,
                ARN: u.ARN,
                Status: u.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update MemoryDB user");
        }
    }

    // ── Event & management operations ───────────────────────────────

    /// <summary>
    /// Describe MemoryDB events.
    /// </summary>
    public static async Task<MemoryDbDescribeEventsResult> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new MemoryDbDescribeEventsResult(
                Events: resp.Events,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB events");
        }
    }

    /// <summary>
    /// Failover a shard in a MemoryDB cluster.
    /// </summary>
    public static async Task<MemoryDbFailoverShardResult> FailoverShardAsync(
        string clusterName,
        string shardName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.FailoverShardAsync(
                new FailoverShardRequest
                {
                    ClusterName = clusterName,
                    ShardName = shardName
                });
            var c = resp.Cluster;
            return new MemoryDbFailoverShardResult(
                ClusterName: c.Name,
                ClusterARN: c.ARN,
                Status: c.Status);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to failover shard '{shardName}' in MemoryDB cluster '{clusterName}'");
        }
    }

    /// <summary>
    /// Describe MemoryDB service updates.
    /// </summary>
    public static async Task<MemoryDbDescribeServiceUpdatesResult>
        DescribeServiceUpdatesAsync(
            string? serviceUpdateName = null,
            List<string>? clusterNames = null,
            List<string>? status = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeServiceUpdatesRequest();
        if (serviceUpdateName != null) request.ServiceUpdateName = serviceUpdateName;
        if (clusterNames != null) request.ClusterNames = clusterNames;
        if (status != null) request.Status = status;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeServiceUpdatesAsync(request);
            return new MemoryDbDescribeServiceUpdatesResult(
                ServiceUpdates: resp.ServiceUpdates,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB service updates");
        }
    }

    /// <summary>
    /// Batch update MemoryDB clusters.
    /// </summary>
    public static async Task<MemoryDbBatchUpdateClusterResult>
        BatchUpdateClusterAsync(
            BatchUpdateClusterRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchUpdateClusterAsync(request);
            return new MemoryDbBatchUpdateClusterResult(
                ProcessedClusters: resp.ProcessedClusters,
                UnprocessedClusters: resp.UnprocessedClusters);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch update MemoryDB clusters");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Tag a MemoryDB resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
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
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag MemoryDB resource");
        }
    }

    /// <summary>
    /// Untag a MemoryDB resource.
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
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag MemoryDB resource");
        }
    }

    /// <summary>
    /// List tags for a MemoryDB resource.
    /// </summary>
    public static async Task<MemoryDbListTagsResult> ListTagsAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsAsync(new ListTagsRequest
            {
                ResourceArn = resourceArn
            });
            return new MemoryDbListTagsResult(TagList: resp.TagList);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for MemoryDB resource");
        }
    }

    /// <summary>
    /// List allowed node type updates for a MemoryDB cluster.
    /// </summary>
    public static async Task<MemoryDbListAllowedNodeTypeUpdatesResult>
        ListAllowedNodeTypeUpdatesAsync(
            string clusterName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListAllowedNodeTypeUpdatesAsync(
                new ListAllowedNodeTypeUpdatesRequest
                {
                    ClusterName = clusterName
                });
            return new MemoryDbListAllowedNodeTypeUpdatesResult(
                ScaleUpNodeTypes: resp.ScaleUpNodeTypes,
                ScaleDownNodeTypes: resp.ScaleDownNodeTypes);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list allowed node type updates for MemoryDB cluster '{clusterName}'");
        }
    }

    // ── Reserved nodes operations ───────────────────────────────────

    /// <summary>
    /// Describe MemoryDB reserved nodes.
    /// </summary>
    public static async Task<MemoryDbDescribeReservedNodesResult>
        DescribeReservedNodesAsync(
            string? reservedNodeId = null,
            string? reservedNodesOfferingId = null,
            string? nodeType = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReservedNodesRequest();
        if (reservedNodeId != null) request.ReservationId = reservedNodeId;
        if (reservedNodesOfferingId != null)
            request.ReservedNodesOfferingId = reservedNodesOfferingId;
        if (nodeType != null) request.NodeType = nodeType;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeReservedNodesAsync(request);
            return new MemoryDbDescribeReservedNodesResult(
                ReservedNodes: resp.ReservedNodes,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB reserved nodes");
        }
    }

    /// <summary>
    /// Describe MemoryDB reserved nodes offerings.
    /// </summary>
    public static async Task<MemoryDbDescribeReservedNodesOfferingsResult>
        DescribeReservedNodesOfferingsAsync(
            string? reservedNodesOfferingId = null,
            string? nodeType = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReservedNodesOfferingsRequest();
        if (reservedNodesOfferingId != null)
            request.ReservedNodesOfferingId = reservedNodesOfferingId;
        if (nodeType != null) request.NodeType = nodeType;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeReservedNodesOfferingsAsync(request);
            return new MemoryDbDescribeReservedNodesOfferingsResult(
                ReservedNodesOfferings: resp.ReservedNodesOfferings,
                NextToken: resp.NextToken);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MemoryDB reserved nodes offerings");
        }
    }

    /// <summary>
    /// Purchase a MemoryDB reserved nodes offering.
    /// </summary>
    public static async Task<MemoryDbPurchaseReservedNodesOfferingResult>
        PurchaseReservedNodesOfferingAsync(
            string reservedNodesOfferingId,
            string? reservationId = null,
            int? nodeCount = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PurchaseReservedNodesOfferingRequest
        {
            ReservedNodesOfferingId = reservedNodesOfferingId
        };
        if (reservationId != null) request.ReservationId = reservationId;
        if (nodeCount.HasValue) request.NodeCount = nodeCount.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.PurchaseReservedNodesOfferingAsync(request);
            var n = resp.ReservedNode;
            return new MemoryDbPurchaseReservedNodesOfferingResult(
                ReservedNodeId: n.ReservationId,
                ReservedNodesOfferingId: n.ReservedNodesOfferingId,
                NodeType: n.NodeType);
        }
        catch (AmazonMemoryDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to purchase MemoryDB reserved nodes offering");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateClusterAsync"/>.</summary>
    public static MemoryDbCreateClusterResult CreateCluster(CreateClusterRequest request, RegionEndpoint? region = null)
        => CreateClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteClusterAsync"/>.</summary>
    public static MemoryDbDeleteClusterResult DeleteCluster(string clusterName, string? finalSnapshotName = null, RegionEndpoint? region = null)
        => DeleteClusterAsync(clusterName, finalSnapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeClustersAsync"/>.</summary>
    public static MemoryDbDescribeClustersResult DescribeClusters(string? clusterName = null, bool? showShardDetails = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeClustersAsync(clusterName, showShardDetails, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateClusterAsync"/>.</summary>
    public static MemoryDbUpdateClusterResult UpdateCluster(UpdateClusterRequest request, RegionEndpoint? region = null)
        => UpdateClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSnapshotAsync"/>.</summary>
    public static MemoryDbCreateSnapshotResult CreateSnapshot(string clusterName, string snapshotName, string? kmsKeyId = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateSnapshotAsync(clusterName, snapshotName, kmsKeyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSnapshotAsync"/>.</summary>
    public static MemoryDbDeleteSnapshotResult DeleteSnapshot(string snapshotName, RegionEndpoint? region = null)
        => DeleteSnapshotAsync(snapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSnapshotsAsync"/>.</summary>
    public static MemoryDbDescribeSnapshotsResult DescribeSnapshots(string? clusterName = null, string? snapshotName = null, string? source = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeSnapshotsAsync(clusterName, snapshotName, source, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CopySnapshotAsync"/>.</summary>
    public static MemoryDbCopySnapshotResult CopySnapshot(string sourceSnapshotName, string targetSnapshotName, string? targetBucket = null, string? kmsKeyId = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CopySnapshotAsync(sourceSnapshotName, targetSnapshotName, targetBucket, kmsKeyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSubnetGroupAsync"/>.</summary>
    public static MemoryDbCreateSubnetGroupResult CreateSubnetGroup(string subnetGroupName, List<string> subnetIds, string? description = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateSubnetGroupAsync(subnetGroupName, subnetIds, description, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSubnetGroupAsync"/>.</summary>
    public static MemoryDbDeleteSubnetGroupResult DeleteSubnetGroup(string subnetGroupName, RegionEndpoint? region = null)
        => DeleteSubnetGroupAsync(subnetGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSubnetGroupsAsync"/>.</summary>
    public static MemoryDbDescribeSubnetGroupsResult DescribeSubnetGroups(string? subnetGroupName = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeSubnetGroupsAsync(subnetGroupName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateSubnetGroupAsync"/>.</summary>
    public static MemoryDbUpdateSubnetGroupResult UpdateSubnetGroup(string subnetGroupName, List<string>? subnetIds = null, string? description = null, RegionEndpoint? region = null)
        => UpdateSubnetGroupAsync(subnetGroupName, subnetIds, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateParameterGroupAsync"/>.</summary>
    public static MemoryDbCreateParameterGroupResult CreateParameterGroup(string parameterGroupName, string family, string? description = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateParameterGroupAsync(parameterGroupName, family, description, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteParameterGroupAsync"/>.</summary>
    public static MemoryDbDeleteParameterGroupResult DeleteParameterGroup(string parameterGroupName, RegionEndpoint? region = null)
        => DeleteParameterGroupAsync(parameterGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeParameterGroupsAsync"/>.</summary>
    public static MemoryDbDescribeParameterGroupsResult DescribeParameterGroups(string? parameterGroupName = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeParameterGroupsAsync(parameterGroupName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateParameterGroupAsync"/>.</summary>
    public static MemoryDbUpdateParameterGroupResult UpdateParameterGroup(string parameterGroupName, List<ParameterNameValue> parameterNameValues, RegionEndpoint? region = null)
        => UpdateParameterGroupAsync(parameterGroupName, parameterNameValues, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeParametersAsync"/>.</summary>
    public static MemoryDbDescribeParametersResult DescribeParameters(string parameterGroupName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeParametersAsync(parameterGroupName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ResetParameterGroupAsync"/>.</summary>
    public static MemoryDbResetParameterGroupResult ResetParameterGroup(string parameterGroupName, bool allParameters = false, List<ParameterNameValue>? parameterNames = null, RegionEndpoint? region = null)
        => ResetParameterGroupAsync(parameterGroupName, allParameters, parameterNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateACLAsync"/>.</summary>
    public static MemoryDbCreateACLResult CreateACL(string aclName, List<string>? userNames = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateACLAsync(aclName, userNames, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteACLAsync"/>.</summary>
    public static MemoryDbDeleteACLResult DeleteACL(string aclName, RegionEndpoint? region = null)
        => DeleteACLAsync(aclName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeACLsAsync"/>.</summary>
    public static MemoryDbDescribeACLsResult DescribeACLs(string? aclName = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeACLsAsync(aclName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateACLAsync"/>.</summary>
    public static MemoryDbUpdateACLResult UpdateACL(string aclName, List<string>? userNamesToAdd = null, List<string>? userNamesToRemove = null, RegionEndpoint? region = null)
        => UpdateACLAsync(aclName, userNamesToAdd, userNamesToRemove, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserAsync"/>.</summary>
    public static MemoryDbCreateUserResult CreateUser(CreateUserRequest request, RegionEndpoint? region = null)
        => CreateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserAsync"/>.</summary>
    public static MemoryDbDeleteUserResult DeleteUser(string userName, RegionEndpoint? region = null)
        => DeleteUserAsync(userName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUsersAsync"/>.</summary>
    public static MemoryDbDescribeUsersResult DescribeUsers(string? userName = null, List<Filter>? filters = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeUsersAsync(userName, filters, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserAsync"/>.</summary>
    public static MemoryDbUpdateUserResult UpdateUser(UpdateUserRequest request, RegionEndpoint? region = null)
        => UpdateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEventsAsync"/>.</summary>
    public static MemoryDbDescribeEventsResult DescribeEvents(DescribeEventsRequest? request = null, RegionEndpoint? region = null)
        => DescribeEventsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="FailoverShardAsync"/>.</summary>
    public static MemoryDbFailoverShardResult FailoverShard(string clusterName, string shardName, RegionEndpoint? region = null)
        => FailoverShardAsync(clusterName, shardName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeServiceUpdatesAsync"/>.</summary>
    public static MemoryDbDescribeServiceUpdatesResult DescribeServiceUpdates(string? serviceUpdateName = null, List<string>? clusterNames = null, List<string>? status = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeServiceUpdatesAsync(serviceUpdateName, clusterNames, status, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchUpdateClusterAsync"/>.</summary>
    public static MemoryDbBatchUpdateClusterResult BatchUpdateCluster(BatchUpdateClusterRequest request, RegionEndpoint? region = null)
        => BatchUpdateClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsAsync"/>.</summary>
    public static MemoryDbListTagsResult ListTags(string resourceArn, RegionEndpoint? region = null)
        => ListTagsAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAllowedNodeTypeUpdatesAsync"/>.</summary>
    public static MemoryDbListAllowedNodeTypeUpdatesResult ListAllowedNodeTypeUpdates(string clusterName, RegionEndpoint? region = null)
        => ListAllowedNodeTypeUpdatesAsync(clusterName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeReservedNodesAsync"/>.</summary>
    public static MemoryDbDescribeReservedNodesResult DescribeReservedNodes(string? reservedNodeId = null, string? reservedNodesOfferingId = null, string? nodeType = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeReservedNodesAsync(reservedNodeId, reservedNodesOfferingId, nodeType, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeReservedNodesOfferingsAsync"/>.</summary>
    public static MemoryDbDescribeReservedNodesOfferingsResult DescribeReservedNodesOfferings(string? reservedNodesOfferingId = null, string? nodeType = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeReservedNodesOfferingsAsync(reservedNodesOfferingId, nodeType, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PurchaseReservedNodesOfferingAsync"/>.</summary>
    public static MemoryDbPurchaseReservedNodesOfferingResult PurchaseReservedNodesOffering(string reservedNodesOfferingId, string? reservationId = null, int? nodeCount = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => PurchaseReservedNodesOfferingAsync(reservedNodesOfferingId, reservationId, nodeCount, tags, region).GetAwaiter().GetResult();

}
