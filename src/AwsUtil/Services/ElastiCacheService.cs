using Amazon;
using Amazon.ElastiCache;
using Amazon.ElastiCache.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record ElastiCacheClusterInfo(
    string? CacheClusterId = null,
    string? CacheClusterStatus = null,
    string? CacheNodeType = null,
    string? Engine = null,
    string? EngineVersion = null,
    int? NumCacheNodes = null,
    string? PreferredAvailabilityZone = null,
    DateTime? CacheClusterCreateTime = null);

public sealed record ElastiCacheReplicationGroupInfo(
    string? ReplicationGroupId = null,
    string? Description = null,
    string? Status = null,
    string? NodeGroupId = null,
    bool? ClusterEnabled = null,
    bool? AutomaticFailover = null,
    bool? MultiAZ = null);

public sealed record ElastiCacheSubnetGroupInfo(
    string? CacheSubnetGroupName = null,
    string? CacheSubnetGroupDescription = null,
    string? VpcId = null,
    List<string>? SubnetIds = null);

public sealed record ElastiCacheParameterGroupInfo(
    string? CacheParameterGroupName = null,
    string? CacheParameterGroupFamily = null,
    string? Description = null,
    bool? IsGlobal = null);

public sealed record ElastiCacheParameterInfo(
    string? ParameterName = null,
    string? ParameterValue = null,
    string? Description = null,
    string? DataType = null,
    bool? IsModifiable = null,
    string? Source = null);

public sealed record ElastiCacheSnapshotInfo(
    string? SnapshotName = null,
    string? SnapshotStatus = null,
    string? SnapshotSource = null,
    string? CacheClusterId = null,
    string? ReplicationGroupId = null,
    string? Engine = null,
    string? EngineVersion = null,
    string? CacheNodeType = null,
    int? NumCacheNodes = null);

public sealed record ElastiCacheGlobalReplicationGroupInfo(
    string? GlobalReplicationGroupId = null,
    string? GlobalReplicationGroupDescription = null,
    string? Status = null,
    string? Engine = null,
    string? EngineVersion = null,
    string? CacheNodeType = null,
    bool? ClusterEnabled = null);

public sealed record ElastiCacheUserInfo(
    string? UserId = null,
    string? UserName = null,
    string? Status = null,
    string? Engine = null,
    string? AccessString = null,
    string? ARN = null);

public sealed record ElastiCacheUserGroupInfo(
    string? UserGroupId = null,
    string? Status = null,
    string? Engine = null,
    List<string>? UserIds = null,
    string? ARN = null);

public sealed record ElastiCacheEngineVersionInfo(
    string? Engine = null,
    string? EngineVersion = null,
    string? CacheParameterGroupFamily = null,
    string? CacheEngineDescription = null,
    string? CacheEngineVersionDescription = null);

public sealed record ElastiCacheReservedNodeInfo(
    string? ReservedCacheNodeId = null,
    string? ReservedCacheNodesOfferingId = null,
    string? CacheNodeType = null,
    int? CacheNodeCount = null,
    string? State = null,
    double? FixedPrice = null,
    double? UsagePrice = null,
    string? OfferingType = null);

public sealed record ElastiCacheEventInfo(
    string? SourceIdentifier = null,
    string? SourceType = null,
    string? Message = null,
    DateTime? Date = null);

public sealed record ElastiCacheTagResult(
    List<Amazon.ElastiCache.Model.Tag>? Tags = null);

public sealed record ElastiCacheTestFailoverResult(
    string? ReplicationGroupId = null,
    string? Status = null);

public sealed record ElastiCacheServerlessCacheInfo(
    string? ServerlessCacheName = null,
    string? Status = null,
    string? Engine = null,
    string? MajorEngineVersion = null,
    string? ARN = null,
    DateTime? CreateTime = null);

public sealed record ElastiCacheModifyResult(
    string? CacheParameterGroupName = null);

/// <summary>
/// Utility helpers for Amazon ElastiCache.
/// </summary>
public static class ElastiCacheService
{
    private static AmazonElastiCacheClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonElastiCacheClient>(region);

    // ── Cache Cluster operations ─────────────────────────────────────

    /// <summary>
    /// Create a cache cluster.
    /// </summary>
    public static async Task<ElastiCacheClusterInfo> CreateCacheClusterAsync(
        CreateCacheClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateCacheClusterAsync(request);
            var c = resp.CacheCluster;
            return new ElastiCacheClusterInfo(
                CacheClusterId: c.CacheClusterId,
                CacheClusterStatus: c.CacheClusterStatus,
                CacheNodeType: c.CacheNodeType,
                Engine: c.Engine,
                EngineVersion: c.EngineVersion,
                NumCacheNodes: c.NumCacheNodes,
                PreferredAvailabilityZone: c.PreferredAvailabilityZone,
                CacheClusterCreateTime: c.CacheClusterCreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create cache cluster");
        }
    }

    /// <summary>
    /// Delete a cache cluster.
    /// </summary>
    public static async Task<ElastiCacheClusterInfo> DeleteCacheClusterAsync(
        string cacheClusterId,
        string? finalSnapshotIdentifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteCacheClusterRequest
        {
            CacheClusterId = cacheClusterId
        };
        if (finalSnapshotIdentifier != null)
            request.FinalSnapshotIdentifier = finalSnapshotIdentifier;

        try
        {
            var resp = await client.DeleteCacheClusterAsync(request);
            var c = resp.CacheCluster;
            return new ElastiCacheClusterInfo(
                CacheClusterId: c.CacheClusterId,
                CacheClusterStatus: c.CacheClusterStatus,
                CacheNodeType: c.CacheNodeType,
                Engine: c.Engine,
                EngineVersion: c.EngineVersion,
                NumCacheNodes: c.NumCacheNodes,
                PreferredAvailabilityZone: c.PreferredAvailabilityZone,
                CacheClusterCreateTime: c.CacheClusterCreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete cache cluster '{cacheClusterId}'");
        }
    }

    /// <summary>
    /// Describe cache clusters.
    /// </summary>
    public static async Task<List<ElastiCacheClusterInfo>>
        DescribeCacheClustersAsync(
            string? cacheClusterId = null,
            bool? showCacheNodeInfo = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCacheClustersRequest();
        if (cacheClusterId != null) request.CacheClusterId = cacheClusterId;
        if (showCacheNodeInfo.HasValue)
            request.ShowCacheNodeInfo = showCacheNodeInfo.Value;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeCacheClustersAsync(request);
            return resp.CacheClusters.Select(c => new ElastiCacheClusterInfo(
                CacheClusterId: c.CacheClusterId,
                CacheClusterStatus: c.CacheClusterStatus,
                CacheNodeType: c.CacheNodeType,
                Engine: c.Engine,
                EngineVersion: c.EngineVersion,
                NumCacheNodes: c.NumCacheNodes,
                PreferredAvailabilityZone: c.PreferredAvailabilityZone,
                CacheClusterCreateTime: c.CacheClusterCreateTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe cache clusters");
        }
    }

    /// <summary>
    /// Modify a cache cluster.
    /// </summary>
    public static async Task<ElastiCacheClusterInfo> ModifyCacheClusterAsync(
        ModifyCacheClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyCacheClusterAsync(request);
            var c = resp.CacheCluster;
            return new ElastiCacheClusterInfo(
                CacheClusterId: c.CacheClusterId,
                CacheClusterStatus: c.CacheClusterStatus,
                CacheNodeType: c.CacheNodeType,
                Engine: c.Engine,
                EngineVersion: c.EngineVersion,
                NumCacheNodes: c.NumCacheNodes,
                PreferredAvailabilityZone: c.PreferredAvailabilityZone,
                CacheClusterCreateTime: c.CacheClusterCreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify cache cluster");
        }
    }

    /// <summary>
    /// Reboot a cache cluster.
    /// </summary>
    public static async Task<ElastiCacheClusterInfo> RebootCacheClusterAsync(
        string cacheClusterId,
        List<string> cacheNodeIdsToReboot,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RebootCacheClusterAsync(
                new RebootCacheClusterRequest
                {
                    CacheClusterId = cacheClusterId,
                    CacheNodeIdsToReboot = cacheNodeIdsToReboot
                });
            var c = resp.CacheCluster;
            return new ElastiCacheClusterInfo(
                CacheClusterId: c.CacheClusterId,
                CacheClusterStatus: c.CacheClusterStatus,
                CacheNodeType: c.CacheNodeType,
                Engine: c.Engine,
                EngineVersion: c.EngineVersion,
                NumCacheNodes: c.NumCacheNodes,
                PreferredAvailabilityZone: c.PreferredAvailabilityZone,
                CacheClusterCreateTime: c.CacheClusterCreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot cache cluster '{cacheClusterId}'");
        }
    }

    // ── Replication Group operations ─────────────────────────────────

    /// <summary>
    /// Create a replication group.
    /// </summary>
    public static async Task<ElastiCacheReplicationGroupInfo>
        CreateReplicationGroupAsync(
            CreateReplicationGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateReplicationGroupAsync(request);
            var r = resp.ReplicationGroup;
            return new ElastiCacheReplicationGroupInfo(
                ReplicationGroupId: r.ReplicationGroupId,
                Description: r.Description,
                Status: r.Status,
                ClusterEnabled: r.ClusterEnabled,
                AutomaticFailover: r.AutomaticFailover?.Value == "enabled");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create replication group");
        }
    }

    /// <summary>
    /// Delete a replication group.
    /// </summary>
    public static async Task<ElastiCacheReplicationGroupInfo>
        DeleteReplicationGroupAsync(
            string replicationGroupId,
            bool? retainPrimaryCluster = null,
            string? finalSnapshotIdentifier = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteReplicationGroupRequest
        {
            ReplicationGroupId = replicationGroupId
        };
        if (retainPrimaryCluster.HasValue)
            request.RetainPrimaryCluster = retainPrimaryCluster.Value;
        if (finalSnapshotIdentifier != null)
            request.FinalSnapshotIdentifier = finalSnapshotIdentifier;

        try
        {
            var resp = await client.DeleteReplicationGroupAsync(request);
            var r = resp.ReplicationGroup;
            return new ElastiCacheReplicationGroupInfo(
                ReplicationGroupId: r.ReplicationGroupId,
                Description: r.Description,
                Status: r.Status,
                ClusterEnabled: r.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete replication group '{replicationGroupId}'");
        }
    }

    /// <summary>
    /// Describe replication groups.
    /// </summary>
    public static async Task<List<ElastiCacheReplicationGroupInfo>>
        DescribeReplicationGroupsAsync(
            string? replicationGroupId = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationGroupsRequest();
        if (replicationGroupId != null)
            request.ReplicationGroupId = replicationGroupId;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeReplicationGroupsAsync(request);
            return resp.ReplicationGroups.Select(r =>
                new ElastiCacheReplicationGroupInfo(
                    ReplicationGroupId: r.ReplicationGroupId,
                    Description: r.Description,
                    Status: r.Status,
                    ClusterEnabled: r.ClusterEnabled,
                    AutomaticFailover: r.AutomaticFailover?.Value == "enabled"))
                .ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe replication groups");
        }
    }

    /// <summary>
    /// Modify a replication group.
    /// </summary>
    public static async Task<ElastiCacheReplicationGroupInfo>
        ModifyReplicationGroupAsync(
            ModifyReplicationGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyReplicationGroupAsync(request);
            var r = resp.ReplicationGroup;
            return new ElastiCacheReplicationGroupInfo(
                ReplicationGroupId: r.ReplicationGroupId,
                Description: r.Description,
                Status: r.Status,
                ClusterEnabled: r.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify replication group");
        }
    }

    /// <summary>
    /// Increase the replica count of a replication group.
    /// </summary>
    public static async Task<ElastiCacheReplicationGroupInfo>
        IncreaseReplicaCountAsync(
            IncreaseReplicaCountRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.IncreaseReplicaCountAsync(request);
            var r = resp.ReplicationGroup;
            return new ElastiCacheReplicationGroupInfo(
                ReplicationGroupId: r.ReplicationGroupId,
                Description: r.Description,
                Status: r.Status,
                ClusterEnabled: r.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to increase replica count");
        }
    }

    /// <summary>
    /// Decrease the replica count of a replication group.
    /// </summary>
    public static async Task<ElastiCacheReplicationGroupInfo>
        DecreaseReplicaCountAsync(
            DecreaseReplicaCountRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DecreaseReplicaCountAsync(request);
            var r = resp.ReplicationGroup;
            return new ElastiCacheReplicationGroupInfo(
                ReplicationGroupId: r.ReplicationGroupId,
                Description: r.Description,
                Status: r.Status,
                ClusterEnabled: r.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to decrease replica count");
        }
    }

    // ── Subnet Group operations ──────────────────────────────────────

    /// <summary>
    /// Create a cache subnet group.
    /// </summary>
    public static async Task<ElastiCacheSubnetGroupInfo>
        CreateCacheSubnetGroupAsync(
            string cacheSubnetGroupName,
            string cacheSubnetGroupDescription,
            List<string> subnetIds,
            List<Amazon.ElastiCache.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = cacheSubnetGroupName,
            CacheSubnetGroupDescription = cacheSubnetGroupDescription,
            SubnetIds = subnetIds
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCacheSubnetGroupAsync(request);
            var g = resp.CacheSubnetGroup;
            return new ElastiCacheSubnetGroupInfo(
                CacheSubnetGroupName: g.CacheSubnetGroupName,
                CacheSubnetGroupDescription: g.CacheSubnetGroupDescription,
                VpcId: g.VpcId,
                SubnetIds: g.Subnets?.Select(s => s.SubnetIdentifier).ToList());
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create cache subnet group '{cacheSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Delete a cache subnet group.
    /// </summary>
    public static async Task DeleteCacheSubnetGroupAsync(
        string cacheSubnetGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCacheSubnetGroupAsync(
                new DeleteCacheSubnetGroupRequest
                {
                    CacheSubnetGroupName = cacheSubnetGroupName
                });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete cache subnet group '{cacheSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe cache subnet groups.
    /// </summary>
    public static async Task<List<ElastiCacheSubnetGroupInfo>>
        DescribeCacheSubnetGroupsAsync(
            string? cacheSubnetGroupName = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCacheSubnetGroupsRequest();
        if (cacheSubnetGroupName != null)
            request.CacheSubnetGroupName = cacheSubnetGroupName;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeCacheSubnetGroupsAsync(request);
            return resp.CacheSubnetGroups.Select(g =>
                new ElastiCacheSubnetGroupInfo(
                    CacheSubnetGroupName: g.CacheSubnetGroupName,
                    CacheSubnetGroupDescription: g.CacheSubnetGroupDescription,
                    VpcId: g.VpcId,
                    SubnetIds: g.Subnets?.Select(s => s.SubnetIdentifier).ToList()))
                .ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe cache subnet groups");
        }
    }

    /// <summary>
    /// Modify a cache subnet group.
    /// </summary>
    public static async Task<ElastiCacheSubnetGroupInfo>
        ModifyCacheSubnetGroupAsync(
            string cacheSubnetGroupName,
            string? cacheSubnetGroupDescription = null,
            List<string>? subnetIds = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ModifyCacheSubnetGroupRequest
        {
            CacheSubnetGroupName = cacheSubnetGroupName
        };
        if (cacheSubnetGroupDescription != null)
            request.CacheSubnetGroupDescription = cacheSubnetGroupDescription;
        if (subnetIds != null) request.SubnetIds = subnetIds;

        try
        {
            var resp = await client.ModifyCacheSubnetGroupAsync(request);
            var g = resp.CacheSubnetGroup;
            return new ElastiCacheSubnetGroupInfo(
                CacheSubnetGroupName: g.CacheSubnetGroupName,
                CacheSubnetGroupDescription: g.CacheSubnetGroupDescription,
                VpcId: g.VpcId,
                SubnetIds: g.Subnets?.Select(s => s.SubnetIdentifier).ToList());
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify cache subnet group '{cacheSubnetGroupName}'");
        }
    }

    // ── Parameter Group operations ───────────────────────────────────

    /// <summary>
    /// Create a cache parameter group.
    /// </summary>
    public static async Task<ElastiCacheParameterGroupInfo>
        CreateCacheParameterGroupAsync(
            string cacheParameterGroupName,
            string cacheParameterGroupFamily,
            string description,
            List<Amazon.ElastiCache.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCacheParameterGroupRequest
        {
            CacheParameterGroupName = cacheParameterGroupName,
            CacheParameterGroupFamily = cacheParameterGroupFamily,
            Description = description
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCacheParameterGroupAsync(request);
            var g = resp.CacheParameterGroup;
            return new ElastiCacheParameterGroupInfo(
                CacheParameterGroupName: g.CacheParameterGroupName,
                CacheParameterGroupFamily: g.CacheParameterGroupFamily,
                Description: g.Description,
                IsGlobal: g.IsGlobal);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create cache parameter group '{cacheParameterGroupName}'");
        }
    }

    /// <summary>
    /// Delete a cache parameter group.
    /// </summary>
    public static async Task DeleteCacheParameterGroupAsync(
        string cacheParameterGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCacheParameterGroupAsync(
                new DeleteCacheParameterGroupRequest
                {
                    CacheParameterGroupName = cacheParameterGroupName
                });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete cache parameter group '{cacheParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe cache parameter groups.
    /// </summary>
    public static async Task<List<ElastiCacheParameterGroupInfo>>
        DescribeCacheParameterGroupsAsync(
            string? cacheParameterGroupName = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCacheParameterGroupsRequest();
        if (cacheParameterGroupName != null)
            request.CacheParameterGroupName = cacheParameterGroupName;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeCacheParameterGroupsAsync(request);
            return resp.CacheParameterGroups.Select(g =>
                new ElastiCacheParameterGroupInfo(
                    CacheParameterGroupName: g.CacheParameterGroupName,
                    CacheParameterGroupFamily: g.CacheParameterGroupFamily,
                    Description: g.Description,
                    IsGlobal: g.IsGlobal)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe cache parameter groups");
        }
    }

    /// <summary>
    /// Modify a cache parameter group.
    /// </summary>
    public static async Task<ElastiCacheModifyResult>
        ModifyCacheParameterGroupAsync(
            string cacheParameterGroupName,
            List<ParameterNameValue> parameterNameValues,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyCacheParameterGroupAsync(
                new ModifyCacheParameterGroupRequest
                {
                    CacheParameterGroupName = cacheParameterGroupName,
                    ParameterNameValues = parameterNameValues
                });
            return new ElastiCacheModifyResult(
                CacheParameterGroupName: resp.CacheParameterGroupName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify cache parameter group '{cacheParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe cache parameters.
    /// </summary>
    public static async Task<List<ElastiCacheParameterInfo>>
        DescribeCacheParametersAsync(
            string cacheParameterGroupName,
            string? source = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCacheParametersRequest
        {
            CacheParameterGroupName = cacheParameterGroupName
        };
        if (source != null) request.Source = source;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeCacheParametersAsync(request);
            return resp.Parameters.Select(p => new ElastiCacheParameterInfo(
                ParameterName: p.ParameterName,
                ParameterValue: p.ParameterValue,
                Description: p.Description,
                DataType: p.DataType,
                IsModifiable: p.IsModifiable,
                Source: p.Source)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe cache parameters for '{cacheParameterGroupName}'");
        }
    }

    // ── Snapshot operations ──────────────────────────────────────────

    /// <summary>
    /// Create a snapshot.
    /// </summary>
    public static async Task<ElastiCacheSnapshotInfo> CreateSnapshotAsync(
        CreateSnapshotRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            var s = resp.Snapshot;
            return new ElastiCacheSnapshotInfo(
                SnapshotName: s.SnapshotName,
                SnapshotStatus: s.SnapshotStatus,
                SnapshotSource: s.SnapshotSource,
                CacheClusterId: s.CacheClusterId,
                ReplicationGroupId: s.ReplicationGroupId,
                Engine: s.Engine,
                EngineVersion: s.EngineVersion,
                CacheNodeType: s.CacheNodeType,
                NumCacheNodes: s.NumCacheNodes);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create snapshot");
        }
    }

    /// <summary>
    /// Delete a snapshot.
    /// </summary>
    public static async Task<ElastiCacheSnapshotInfo> DeleteSnapshotAsync(
        string snapshotName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSnapshotAsync(new DeleteSnapshotRequest
            {
                SnapshotName = snapshotName
            });
            var s = resp.Snapshot;
            return new ElastiCacheSnapshotInfo(
                SnapshotName: s.SnapshotName,
                SnapshotStatus: s.SnapshotStatus,
                SnapshotSource: s.SnapshotSource,
                CacheClusterId: s.CacheClusterId,
                ReplicationGroupId: s.ReplicationGroupId,
                Engine: s.Engine,
                EngineVersion: s.EngineVersion,
                CacheNodeType: s.CacheNodeType,
                NumCacheNodes: s.NumCacheNodes);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot '{snapshotName}'");
        }
    }

    /// <summary>
    /// Describe snapshots.
    /// </summary>
    public static async Task<List<ElastiCacheSnapshotInfo>>
        DescribeSnapshotsAsync(
            string? cacheClusterId = null,
            string? snapshotName = null,
            string? replicationGroupId = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSnapshotsRequest();
        if (cacheClusterId != null) request.CacheClusterId = cacheClusterId;
        if (snapshotName != null) request.SnapshotName = snapshotName;
        if (replicationGroupId != null)
            request.ReplicationGroupId = replicationGroupId;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeSnapshotsAsync(request);
            return resp.Snapshots.Select(s => new ElastiCacheSnapshotInfo(
                SnapshotName: s.SnapshotName,
                SnapshotStatus: s.SnapshotStatus,
                SnapshotSource: s.SnapshotSource,
                CacheClusterId: s.CacheClusterId,
                ReplicationGroupId: s.ReplicationGroupId,
                Engine: s.Engine,
                EngineVersion: s.EngineVersion,
                CacheNodeType: s.CacheNodeType,
                NumCacheNodes: s.NumCacheNodes)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe snapshots");
        }
    }

    /// <summary>
    /// Copy a snapshot.
    /// </summary>
    public static async Task<ElastiCacheSnapshotInfo> CopySnapshotAsync(
        string sourceSnapshotName,
        string targetSnapshotName,
        string? targetBucket = null,
        string? kmsKeyId = null,
        List<Amazon.ElastiCache.Model.Tag>? tags = null,
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
            return new ElastiCacheSnapshotInfo(
                SnapshotName: s.SnapshotName,
                SnapshotStatus: s.SnapshotStatus,
                SnapshotSource: s.SnapshotSource,
                CacheClusterId: s.CacheClusterId,
                ReplicationGroupId: s.ReplicationGroupId,
                Engine: s.Engine,
                EngineVersion: s.EngineVersion,
                CacheNodeType: s.CacheNodeType,
                NumCacheNodes: s.NumCacheNodes);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to copy snapshot '{sourceSnapshotName}'");
        }
    }

    // ── Global Replication Group operations ──────────────────────────

    /// <summary>
    /// Create a global replication group.
    /// </summary>
    public static async Task<ElastiCacheGlobalReplicationGroupInfo>
        CreateGlobalReplicationGroupAsync(
            CreateGlobalReplicationGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGlobalReplicationGroupAsync(request);
            var g = resp.GlobalReplicationGroup;
            return new ElastiCacheGlobalReplicationGroupInfo(
                GlobalReplicationGroupId: g.GlobalReplicationGroupId,
                GlobalReplicationGroupDescription:
                    g.GlobalReplicationGroupDescription,
                Status: g.Status,
                Engine: g.Engine,
                EngineVersion: g.EngineVersion,
                CacheNodeType: g.CacheNodeType,
                ClusterEnabled: g.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create global replication group");
        }
    }

    /// <summary>
    /// Delete a global replication group.
    /// </summary>
    public static async Task<ElastiCacheGlobalReplicationGroupInfo>
        DeleteGlobalReplicationGroupAsync(
            string globalReplicationGroupId,
            bool retainPrimaryReplicationGroup,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGlobalReplicationGroupAsync(
                new DeleteGlobalReplicationGroupRequest
                {
                    GlobalReplicationGroupId = globalReplicationGroupId,
                    RetainPrimaryReplicationGroup = retainPrimaryReplicationGroup
                });
            var g = resp.GlobalReplicationGroup;
            return new ElastiCacheGlobalReplicationGroupInfo(
                GlobalReplicationGroupId: g.GlobalReplicationGroupId,
                GlobalReplicationGroupDescription:
                    g.GlobalReplicationGroupDescription,
                Status: g.Status,
                Engine: g.Engine,
                EngineVersion: g.EngineVersion,
                CacheNodeType: g.CacheNodeType,
                ClusterEnabled: g.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete global replication group '{globalReplicationGroupId}'");
        }
    }

    /// <summary>
    /// Describe global replication groups.
    /// </summary>
    public static async Task<List<ElastiCacheGlobalReplicationGroupInfo>>
        DescribeGlobalReplicationGroupsAsync(
            string? globalReplicationGroupId = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeGlobalReplicationGroupsRequest();
        if (globalReplicationGroupId != null)
            request.GlobalReplicationGroupId = globalReplicationGroupId;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeGlobalReplicationGroupsAsync(request);
            return resp.GlobalReplicationGroups.Select(g =>
                new ElastiCacheGlobalReplicationGroupInfo(
                    GlobalReplicationGroupId: g.GlobalReplicationGroupId,
                    GlobalReplicationGroupDescription:
                        g.GlobalReplicationGroupDescription,
                    Status: g.Status,
                    Engine: g.Engine,
                    EngineVersion: g.EngineVersion,
                    CacheNodeType: g.CacheNodeType,
                    ClusterEnabled: g.ClusterEnabled)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe global replication groups");
        }
    }

    /// <summary>
    /// Modify a global replication group.
    /// </summary>
    public static async Task<ElastiCacheGlobalReplicationGroupInfo>
        ModifyGlobalReplicationGroupAsync(
            ModifyGlobalReplicationGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyGlobalReplicationGroupAsync(request);
            var g = resp.GlobalReplicationGroup;
            return new ElastiCacheGlobalReplicationGroupInfo(
                GlobalReplicationGroupId: g.GlobalReplicationGroupId,
                GlobalReplicationGroupDescription:
                    g.GlobalReplicationGroupDescription,
                Status: g.Status,
                Engine: g.Engine,
                EngineVersion: g.EngineVersion,
                CacheNodeType: g.CacheNodeType,
                ClusterEnabled: g.ClusterEnabled);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify global replication group");
        }
    }

    // ── User operations ──────────────────────────────────────────────

    /// <summary>
    /// Create a user.
    /// </summary>
    public static async Task<ElastiCacheUserInfo> CreateUserAsync(
        CreateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserAsync(request);
            return new ElastiCacheUserInfo(
                UserId: resp.UserId,
                UserName: resp.UserName,
                Status: resp.Status,
                Engine: resp.Engine,
                AccessString: resp.AccessString,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create user");
        }
    }

    /// <summary>
    /// Delete a user.
    /// </summary>
    public static async Task<ElastiCacheUserInfo> DeleteUserAsync(
        string userId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteUserAsync(new DeleteUserRequest
            {
                UserId = userId
            });
            return new ElastiCacheUserInfo(
                UserId: resp.UserId,
                UserName: resp.UserName,
                Status: resp.Status,
                Engine: resp.Engine,
                AccessString: resp.AccessString,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete user '{userId}'");
        }
    }

    /// <summary>
    /// Describe users.
    /// </summary>
    public static async Task<List<ElastiCacheUserInfo>> DescribeUsersAsync(
        string? userId = null,
        string? engine = null,
        int? maxRecords = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeUsersRequest();
        if (userId != null) request.UserId = userId;
        if (engine != null) request.Engine = engine;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeUsersAsync(request);
            return resp.Users.Select(u => new ElastiCacheUserInfo(
                UserId: u.UserId,
                UserName: u.UserName,
                Status: u.Status,
                Engine: u.Engine,
                AccessString: u.AccessString,
                ARN: u.ARN)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe users");
        }
    }

    /// <summary>
    /// Modify a user.
    /// </summary>
    public static async Task<ElastiCacheUserInfo> ModifyUserAsync(
        ModifyUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyUserAsync(request);
            return new ElastiCacheUserInfo(
                UserId: resp.UserId,
                UserName: resp.UserName,
                Status: resp.Status,
                Engine: resp.Engine,
                AccessString: resp.AccessString,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify user");
        }
    }

    // ── User Group operations ────────────────────────────────────────

    /// <summary>
    /// Create a user group.
    /// </summary>
    public static async Task<ElastiCacheUserGroupInfo> CreateUserGroupAsync(
        CreateUserGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserGroupAsync(request);
            return new ElastiCacheUserGroupInfo(
                UserGroupId: resp.UserGroupId,
                Status: resp.Status,
                Engine: resp.Engine,
                UserIds: resp.UserIds,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create user group");
        }
    }

    /// <summary>
    /// Delete a user group.
    /// </summary>
    public static async Task<ElastiCacheUserGroupInfo> DeleteUserGroupAsync(
        string userGroupId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteUserGroupAsync(new DeleteUserGroupRequest
            {
                UserGroupId = userGroupId
            });
            return new ElastiCacheUserGroupInfo(
                UserGroupId: resp.UserGroupId,
                Status: resp.Status,
                Engine: resp.Engine,
                UserIds: resp.UserIds,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete user group '{userGroupId}'");
        }
    }

    /// <summary>
    /// Describe user groups.
    /// </summary>
    public static async Task<List<ElastiCacheUserGroupInfo>>
        DescribeUserGroupsAsync(
            string? userGroupId = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeUserGroupsRequest();
        if (userGroupId != null) request.UserGroupId = userGroupId;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeUserGroupsAsync(request);
            return resp.UserGroups.Select(g => new ElastiCacheUserGroupInfo(
                UserGroupId: g.UserGroupId,
                Status: g.Status,
                Engine: g.Engine,
                UserIds: g.UserIds,
                ARN: g.ARN)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe user groups");
        }
    }

    /// <summary>
    /// Modify a user group.
    /// </summary>
    public static async Task<ElastiCacheUserGroupInfo> ModifyUserGroupAsync(
        ModifyUserGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyUserGroupAsync(request);
            return new ElastiCacheUserGroupInfo(
                UserGroupId: resp.UserGroupId,
                Status: resp.Status,
                Engine: resp.Engine,
                UserIds: resp.UserIds,
                ARN: resp.ARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify user group");
        }
    }

    // ── Engine Versions & Reserved Nodes ─────────────────────────────

    /// <summary>
    /// Describe cache engine versions.
    /// </summary>
    public static async Task<List<ElastiCacheEngineVersionInfo>>
        DescribeCacheEngineVersionsAsync(
            string? engine = null,
            string? engineVersion = null,
            string? cacheParameterGroupFamily = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCacheEngineVersionsRequest();
        if (engine != null) request.Engine = engine;
        if (engineVersion != null) request.EngineVersion = engineVersion;
        if (cacheParameterGroupFamily != null)
            request.CacheParameterGroupFamily = cacheParameterGroupFamily;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeCacheEngineVersionsAsync(request);
            return resp.CacheEngineVersions.Select(v =>
                new ElastiCacheEngineVersionInfo(
                    Engine: v.Engine,
                    EngineVersion: v.EngineVersion,
                    CacheParameterGroupFamily: v.CacheParameterGroupFamily,
                    CacheEngineDescription: v.CacheEngineDescription,
                    CacheEngineVersionDescription:
                        v.CacheEngineVersionDescription)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe cache engine versions");
        }
    }

    /// <summary>
    /// Describe reserved cache nodes.
    /// </summary>
    public static async Task<List<ElastiCacheReservedNodeInfo>>
        DescribeReservedCacheNodesAsync(
            string? reservedCacheNodeId = null,
            string? cacheNodeType = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReservedCacheNodesRequest();
        if (reservedCacheNodeId != null)
            request.ReservedCacheNodeId = reservedCacheNodeId;
        if (cacheNodeType != null) request.CacheNodeType = cacheNodeType;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeReservedCacheNodesAsync(request);
            return resp.ReservedCacheNodes.Select(n =>
                new ElastiCacheReservedNodeInfo(
                    ReservedCacheNodeId: n.ReservedCacheNodeId,
                    ReservedCacheNodesOfferingId: n.ReservedCacheNodesOfferingId,
                    CacheNodeType: n.CacheNodeType,
                    CacheNodeCount: n.CacheNodeCount,
                    State: n.State,
                    FixedPrice: n.FixedPrice,
                    UsagePrice: n.UsagePrice,
                    OfferingType: n.OfferingType)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe reserved cache nodes");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Add tags to an ElastiCache resource.
    /// </summary>
    public static async Task<ElastiCacheTagResult> AddTagsToResourceAsync(
        string resourceName,
        List<Amazon.ElastiCache.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddTagsToResourceAsync(
                new AddTagsToResourceRequest
                {
                    ResourceName = resourceName,
                    Tags = tags
                });
            return new ElastiCacheTagResult(Tags: resp.TagList);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add tags to resource '{resourceName}'");
        }
    }

    /// <summary>
    /// Remove tags from an ElastiCache resource.
    /// </summary>
    public static async Task<ElastiCacheTagResult> RemoveTagsFromResourceAsync(
        string resourceName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RemoveTagsFromResourceAsync(
                new RemoveTagsFromResourceRequest
                {
                    ResourceName = resourceName,
                    TagKeys = tagKeys
                });
            return new ElastiCacheTagResult(Tags: resp.TagList);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove tags from resource '{resourceName}'");
        }
    }

    /// <summary>
    /// List tags for an ElastiCache resource.
    /// </summary>
    public static async Task<ElastiCacheTagResult> ListTagsForResourceAsync(
        string resourceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest
                {
                    ResourceName = resourceName
                });
            return new ElastiCacheTagResult(Tags: resp.TagList);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceName}'");
        }
    }

    // ── Failover & Events ────────────────────────────────────────────

    /// <summary>
    /// Test automatic failover on a replication group.
    /// </summary>
    public static async Task<ElastiCacheTestFailoverResult> TestFailoverAsync(
        string replicationGroupId,
        string nodeGroupId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TestFailoverAsync(new TestFailoverRequest
            {
                ReplicationGroupId = replicationGroupId,
                NodeGroupId = nodeGroupId
            });
            var r = resp.ReplicationGroup;
            return new ElastiCacheTestFailoverResult(
                ReplicationGroupId: r.ReplicationGroupId,
                Status: r.Status);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to test failover for '{replicationGroupId}'");
        }
    }

    /// <summary>
    /// Describe ElastiCache events.
    /// </summary>
    public static async Task<List<ElastiCacheEventInfo>> DescribeEventsAsync(
        string? sourceIdentifier = null,
        string? sourceType = null,
        int? maxRecords = null,
        string? marker = null,
        int? duration = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventsRequest();
        if (sourceIdentifier != null) request.SourceIdentifier = sourceIdentifier;
        if (sourceType != null) request.SourceType = sourceType;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;
        if (duration.HasValue) request.Duration = duration.Value;

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return resp.Events.Select(e => new ElastiCacheEventInfo(
                SourceIdentifier: e.SourceIdentifier,
                SourceType: e.SourceType?.Value,
                Message: e.Message,
                Date: e.Date)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe events");
        }
    }

    // ── Serverless Cache operations ──────────────────────────────────

    /// <summary>
    /// Create a serverless cache.
    /// </summary>
    public static async Task<ElastiCacheServerlessCacheInfo>
        CreateServerlessCacheAsync(
            CreateServerlessCacheRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServerlessCacheAsync(request);
            var s = resp.ServerlessCache;
            return new ElastiCacheServerlessCacheInfo(
                ServerlessCacheName: s.ServerlessCacheName,
                Status: s.Status,
                Engine: s.Engine,
                MajorEngineVersion: s.MajorEngineVersion,
                ARN: s.ARN,
                CreateTime: s.CreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create serverless cache");
        }
    }

    /// <summary>
    /// Delete a serverless cache.
    /// </summary>
    public static async Task<ElastiCacheServerlessCacheInfo>
        DeleteServerlessCacheAsync(
            string serverlessCacheName,
            string? finalSnapshotName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteServerlessCacheRequest
        {
            ServerlessCacheName = serverlessCacheName
        };
        if (finalSnapshotName != null)
            request.FinalSnapshotName = finalSnapshotName;

        try
        {
            var resp = await client.DeleteServerlessCacheAsync(request);
            var s = resp.ServerlessCache;
            return new ElastiCacheServerlessCacheInfo(
                ServerlessCacheName: s.ServerlessCacheName,
                Status: s.Status,
                Engine: s.Engine,
                MajorEngineVersion: s.MajorEngineVersion,
                ARN: s.ARN,
                CreateTime: s.CreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete serverless cache '{serverlessCacheName}'");
        }
    }

    /// <summary>
    /// Describe serverless caches.
    /// </summary>
    public static async Task<List<ElastiCacheServerlessCacheInfo>>
        DescribeServerlessCachesAsync(
            string? serverlessCacheName = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeServerlessCachesRequest();
        if (serverlessCacheName != null)
            request.ServerlessCacheName = serverlessCacheName;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeServerlessCachesAsync(request);
            return resp.ServerlessCaches.Select(s =>
                new ElastiCacheServerlessCacheInfo(
                    ServerlessCacheName: s.ServerlessCacheName,
                    Status: s.Status,
                    Engine: s.Engine,
                    MajorEngineVersion: s.MajorEngineVersion,
                    ARN: s.ARN,
                    CreateTime: s.CreateTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe serverless caches");
        }
    }

    /// <summary>
    /// Modify a serverless cache.
    /// </summary>
    public static async Task<ElastiCacheServerlessCacheInfo>
        ModifyServerlessCacheAsync(
            ModifyServerlessCacheRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyServerlessCacheAsync(request);
            var s = resp.ServerlessCache;
            return new ElastiCacheServerlessCacheInfo(
                ServerlessCacheName: s.ServerlessCacheName,
                Status: s.Status,
                Engine: s.Engine,
                MajorEngineVersion: s.MajorEngineVersion,
                ARN: s.ARN,
                CreateTime: s.CreateTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify serverless cache");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateCacheClusterAsync"/>.</summary>
    public static ElastiCacheClusterInfo CreateCacheCluster(CreateCacheClusterRequest request, RegionEndpoint? region = null)
        => CreateCacheClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCacheClusterAsync"/>.</summary>
    public static ElastiCacheClusterInfo DeleteCacheCluster(string cacheClusterId, string? finalSnapshotIdentifier = null, RegionEndpoint? region = null)
        => DeleteCacheClusterAsync(cacheClusterId, finalSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheClustersAsync"/>.</summary>
    public static List<ElastiCacheClusterInfo> DescribeCacheClusters(string? cacheClusterId = null, bool? showCacheNodeInfo = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeCacheClustersAsync(cacheClusterId, showCacheNodeInfo, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyCacheClusterAsync"/>.</summary>
    public static ElastiCacheClusterInfo ModifyCacheCluster(ModifyCacheClusterRequest request, RegionEndpoint? region = null)
        => ModifyCacheClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RebootCacheClusterAsync"/>.</summary>
    public static ElastiCacheClusterInfo RebootCacheCluster(string cacheClusterId, List<string> cacheNodeIdsToReboot, RegionEndpoint? region = null)
        => RebootCacheClusterAsync(cacheClusterId, cacheNodeIdsToReboot, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateReplicationGroupAsync"/>.</summary>
    public static ElastiCacheReplicationGroupInfo CreateReplicationGroup(CreateReplicationGroupRequest request, RegionEndpoint? region = null)
        => CreateReplicationGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteReplicationGroupAsync"/>.</summary>
    public static ElastiCacheReplicationGroupInfo DeleteReplicationGroup(string replicationGroupId, bool? retainPrimaryCluster = null, string? finalSnapshotIdentifier = null, RegionEndpoint? region = null)
        => DeleteReplicationGroupAsync(replicationGroupId, retainPrimaryCluster, finalSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeReplicationGroupsAsync"/>.</summary>
    public static List<ElastiCacheReplicationGroupInfo> DescribeReplicationGroups(string? replicationGroupId = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeReplicationGroupsAsync(replicationGroupId, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyReplicationGroupAsync"/>.</summary>
    public static ElastiCacheReplicationGroupInfo ModifyReplicationGroup(ModifyReplicationGroupRequest request, RegionEndpoint? region = null)
        => ModifyReplicationGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="IncreaseReplicaCountAsync"/>.</summary>
    public static ElastiCacheReplicationGroupInfo IncreaseReplicaCount(IncreaseReplicaCountRequest request, RegionEndpoint? region = null)
        => IncreaseReplicaCountAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DecreaseReplicaCountAsync"/>.</summary>
    public static ElastiCacheReplicationGroupInfo DecreaseReplicaCount(DecreaseReplicaCountRequest request, RegionEndpoint? region = null)
        => DecreaseReplicaCountAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCacheSubnetGroupAsync"/>.</summary>
    public static ElastiCacheSubnetGroupInfo CreateCacheSubnetGroup(string cacheSubnetGroupName, string cacheSubnetGroupDescription, List<string> subnetIds, List<Amazon.ElastiCache.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateCacheSubnetGroupAsync(cacheSubnetGroupName, cacheSubnetGroupDescription, subnetIds, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCacheSubnetGroupAsync"/>.</summary>
    public static void DeleteCacheSubnetGroup(string cacheSubnetGroupName, RegionEndpoint? region = null)
        => DeleteCacheSubnetGroupAsync(cacheSubnetGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheSubnetGroupsAsync"/>.</summary>
    public static List<ElastiCacheSubnetGroupInfo> DescribeCacheSubnetGroups(string? cacheSubnetGroupName = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeCacheSubnetGroupsAsync(cacheSubnetGroupName, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyCacheSubnetGroupAsync"/>.</summary>
    public static ElastiCacheSubnetGroupInfo ModifyCacheSubnetGroup(string cacheSubnetGroupName, string? cacheSubnetGroupDescription = null, List<string>? subnetIds = null, RegionEndpoint? region = null)
        => ModifyCacheSubnetGroupAsync(cacheSubnetGroupName, cacheSubnetGroupDescription, subnetIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCacheParameterGroupAsync"/>.</summary>
    public static ElastiCacheParameterGroupInfo CreateCacheParameterGroup(string cacheParameterGroupName, string cacheParameterGroupFamily, string description, List<Amazon.ElastiCache.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateCacheParameterGroupAsync(cacheParameterGroupName, cacheParameterGroupFamily, description, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCacheParameterGroupAsync"/>.</summary>
    public static void DeleteCacheParameterGroup(string cacheParameterGroupName, RegionEndpoint? region = null)
        => DeleteCacheParameterGroupAsync(cacheParameterGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheParameterGroupsAsync"/>.</summary>
    public static List<ElastiCacheParameterGroupInfo> DescribeCacheParameterGroups(string? cacheParameterGroupName = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeCacheParameterGroupsAsync(cacheParameterGroupName, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyCacheParameterGroupAsync"/>.</summary>
    public static ElastiCacheModifyResult ModifyCacheParameterGroup(string cacheParameterGroupName, List<ParameterNameValue> parameterNameValues, RegionEndpoint? region = null)
        => ModifyCacheParameterGroupAsync(cacheParameterGroupName, parameterNameValues, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheParametersAsync"/>.</summary>
    public static List<ElastiCacheParameterInfo> DescribeCacheParameters(string cacheParameterGroupName, string? source = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeCacheParametersAsync(cacheParameterGroupName, source, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSnapshotAsync"/>.</summary>
    public static ElastiCacheSnapshotInfo CreateSnapshot(CreateSnapshotRequest request, RegionEndpoint? region = null)
        => CreateSnapshotAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSnapshotAsync"/>.</summary>
    public static ElastiCacheSnapshotInfo DeleteSnapshot(string snapshotName, RegionEndpoint? region = null)
        => DeleteSnapshotAsync(snapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSnapshotsAsync"/>.</summary>
    public static List<ElastiCacheSnapshotInfo> DescribeSnapshots(string? cacheClusterId = null, string? snapshotName = null, string? replicationGroupId = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeSnapshotsAsync(cacheClusterId, snapshotName, replicationGroupId, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CopySnapshotAsync"/>.</summary>
    public static ElastiCacheSnapshotInfo CopySnapshot(string sourceSnapshotName, string targetSnapshotName, string? targetBucket = null, string? kmsKeyId = null, List<Amazon.ElastiCache.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CopySnapshotAsync(sourceSnapshotName, targetSnapshotName, targetBucket, kmsKeyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateGlobalReplicationGroupAsync"/>.</summary>
    public static ElastiCacheGlobalReplicationGroupInfo CreateGlobalReplicationGroup(CreateGlobalReplicationGroupRequest request, RegionEndpoint? region = null)
        => CreateGlobalReplicationGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGlobalReplicationGroupAsync"/>.</summary>
    public static ElastiCacheGlobalReplicationGroupInfo DeleteGlobalReplicationGroup(string globalReplicationGroupId, bool retainPrimaryReplicationGroup, RegionEndpoint? region = null)
        => DeleteGlobalReplicationGroupAsync(globalReplicationGroupId, retainPrimaryReplicationGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeGlobalReplicationGroupsAsync"/>.</summary>
    public static List<ElastiCacheGlobalReplicationGroupInfo> DescribeGlobalReplicationGroups(string? globalReplicationGroupId = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeGlobalReplicationGroupsAsync(globalReplicationGroupId, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyGlobalReplicationGroupAsync"/>.</summary>
    public static ElastiCacheGlobalReplicationGroupInfo ModifyGlobalReplicationGroup(ModifyGlobalReplicationGroupRequest request, RegionEndpoint? region = null)
        => ModifyGlobalReplicationGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserAsync"/>.</summary>
    public static ElastiCacheUserInfo CreateUser(CreateUserRequest request, RegionEndpoint? region = null)
        => CreateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserAsync"/>.</summary>
    public static ElastiCacheUserInfo DeleteUser(string userId, RegionEndpoint? region = null)
        => DeleteUserAsync(userId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUsersAsync"/>.</summary>
    public static List<ElastiCacheUserInfo> DescribeUsers(string? userId = null, string? engine = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeUsersAsync(userId, engine, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyUserAsync"/>.</summary>
    public static ElastiCacheUserInfo ModifyUser(ModifyUserRequest request, RegionEndpoint? region = null)
        => ModifyUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserGroupAsync"/>.</summary>
    public static ElastiCacheUserGroupInfo CreateUserGroup(CreateUserGroupRequest request, RegionEndpoint? region = null)
        => CreateUserGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserGroupAsync"/>.</summary>
    public static ElastiCacheUserGroupInfo DeleteUserGroup(string userGroupId, RegionEndpoint? region = null)
        => DeleteUserGroupAsync(userGroupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUserGroupsAsync"/>.</summary>
    public static List<ElastiCacheUserGroupInfo> DescribeUserGroups(string? userGroupId = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeUserGroupsAsync(userGroupId, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyUserGroupAsync"/>.</summary>
    public static ElastiCacheUserGroupInfo ModifyUserGroup(ModifyUserGroupRequest request, RegionEndpoint? region = null)
        => ModifyUserGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheEngineVersionsAsync"/>.</summary>
    public static List<ElastiCacheEngineVersionInfo> DescribeCacheEngineVersions(string? engine = null, string? engineVersion = null, string? cacheParameterGroupFamily = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeCacheEngineVersionsAsync(engine, engineVersion, cacheParameterGroupFamily, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeReservedCacheNodesAsync"/>.</summary>
    public static List<ElastiCacheReservedNodeInfo> DescribeReservedCacheNodes(string? reservedCacheNodeId = null, string? cacheNodeType = null, int? maxRecords = null, string? marker = null, RegionEndpoint? region = null)
        => DescribeReservedCacheNodesAsync(reservedCacheNodeId, cacheNodeType, maxRecords, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToResourceAsync"/>.</summary>
    public static ElastiCacheTagResult AddTagsToResource(string resourceName, List<Amazon.ElastiCache.Model.Tag> tags, RegionEndpoint? region = null)
        => AddTagsToResourceAsync(resourceName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromResourceAsync"/>.</summary>
    public static ElastiCacheTagResult RemoveTagsFromResource(string resourceName, List<string> tagKeys, RegionEndpoint? region = null)
        => RemoveTagsFromResourceAsync(resourceName, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ElastiCacheTagResult ListTagsForResource(string resourceName, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TestFailoverAsync"/>.</summary>
    public static ElastiCacheTestFailoverResult TestFailover(string replicationGroupId, string nodeGroupId, RegionEndpoint? region = null)
        => TestFailoverAsync(replicationGroupId, nodeGroupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEventsAsync"/>.</summary>
    public static List<ElastiCacheEventInfo> DescribeEvents(string? sourceIdentifier = null, string? sourceType = null, int? maxRecords = null, string? marker = null, int? duration = null, RegionEndpoint? region = null)
        => DescribeEventsAsync(sourceIdentifier, sourceType, maxRecords, marker, duration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateServerlessCacheAsync"/>.</summary>
    public static ElastiCacheServerlessCacheInfo CreateServerlessCache(CreateServerlessCacheRequest request, RegionEndpoint? region = null)
        => CreateServerlessCacheAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServerlessCacheAsync"/>.</summary>
    public static ElastiCacheServerlessCacheInfo DeleteServerlessCache(string serverlessCacheName, string? finalSnapshotName = null, RegionEndpoint? region = null)
        => DeleteServerlessCacheAsync(serverlessCacheName, finalSnapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeServerlessCachesAsync"/>.</summary>
    public static List<ElastiCacheServerlessCacheInfo> DescribeServerlessCaches(string? serverlessCacheName = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => DescribeServerlessCachesAsync(serverlessCacheName, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyServerlessCacheAsync"/>.</summary>
    public static ElastiCacheServerlessCacheInfo ModifyServerlessCache(ModifyServerlessCacheRequest request, RegionEndpoint? region = null)
        => ModifyServerlessCacheAsync(request, region).GetAwaiter().GetResult();

}
