using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null,
    string? Engine = null,
    string? DBInstanceClass = null);

public sealed record DeleteDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record DescribeDBInstancesResult(
    List<DBInstance>? DBInstances = null,
    string? Marker = null);

public sealed record ModifyDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record RebootDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record StartDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record StopDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record CreateDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null,
    string? Engine = null);

public sealed record DeleteDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DescribeDBClustersResult(
    List<DBCluster>? DBClusters = null,
    string? Marker = null);

public sealed record ModifyDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record CreateDBSnapshotResult(
    string? DBSnapshotIdentifier = null,
    string? DBSnapshotArn = null,
    string? Status = null);

public sealed record DeleteDBSnapshotResult(
    string? DBSnapshotIdentifier = null,
    string? DBSnapshotArn = null,
    string? Status = null);

public sealed record DescribeDBSnapshotsResult(
    List<DBSnapshot>? DBSnapshots = null,
    string? Marker = null);

public sealed record RestoreDBInstanceFromDBSnapshotResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record CreateDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record DeleteDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record DescribeDBClusterSnapshotsResult(
    List<DBClusterSnapshot>? DBClusterSnapshots = null,
    string? Marker = null);

public sealed record CreateDBSubnetGroupResult(
    string? DBSubnetGroupName = null,
    string? DBSubnetGroupArn = null,
    string? DBSubnetGroupDescription = null);

public sealed record DescribeDBSubnetGroupsResult(
    List<DBSubnetGroup>? DBSubnetGroups = null,
    string? Marker = null);

public sealed record CreateDBParameterGroupResult(
    string? DBParameterGroupName = null,
    string? DBParameterGroupArn = null,
    string? DBParameterGroupFamily = null);

public sealed record DescribeDBParameterGroupsResult(
    List<DBParameterGroup>? DBParameterGroups = null,
    string? Marker = null);

public sealed record ModifyDBParameterGroupResult(
    string? DBParameterGroupName = null);

public sealed record DescribeDBParametersResult(
    List<Parameter>? Parameters = null,
    string? Marker = null);

public sealed record DescribeDBEngineVersionsResult(
    List<DBEngineVersion>? DBEngineVersions = null,
    string? Marker = null);

public sealed record DescribeOrderableDBInstanceOptionsResult(
    List<OrderableDBInstanceOption>? OrderableDBInstanceOptions = null,
    string? Marker = null);

public sealed record CreateEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DeleteEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DescribeEventSubscriptionsResult(
    List<EventSubscription>? EventSubscriptionsList = null,
    string? Marker = null);

public sealed record DescribeEventsResult(
    List<Event>? Events = null,
    string? Marker = null);

public sealed record ListTagsForResourceResult(
    List<Tag>? TagList = null);

public sealed record PromoteReadReplicaResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record CreateDBInstanceReadReplicaResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record DescribeDBLogFilesResult(
    List<DescribeDBLogFilesDetails>? DescribeDBLogFiles = null,
    string? Marker = null);

public sealed record DownloadDBLogFilePortionResult(
    string? LogFileData = null,
    string? Marker = null,
    bool? AdditionalDataPending = null);

public sealed record FailoverDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record SwitchoverReadReplicaResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon RDS.
/// </summary>
public static class RdsService
{
    private static AmazonRDSClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRDSClient>(region);

    /// <summary>
    /// Create a new RDS DB instance.
    /// </summary>
    public static async Task<CreateDBInstanceResult> CreateDBInstanceAsync(
        CreateDBInstanceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new CreateDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus,
                Engine: db.Engine,
                DBInstanceClass: db.DBInstanceClass);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB instance");
        }
    }

    /// <summary>
    /// Delete an RDS DB instance.
    /// </summary>
    public static async Task<DeleteDBInstanceResult> DeleteDBInstanceAsync(
        string dbInstanceIdentifier,
        bool skipFinalSnapshot = false,
        string? finalDBSnapshotIdentifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDBInstanceRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier,
            SkipFinalSnapshot = skipFinalSnapshot
        };
        if (finalDBSnapshotIdentifier != null)
            request.FinalDBSnapshotIdentifier = finalDBSnapshotIdentifier;

        try
        {
            var resp = await client.DeleteDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new DeleteDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Describe RDS DB instances.
    /// </summary>
    public static async Task<DescribeDBInstancesResult> DescribeDBInstancesAsync(
        string? dbInstanceIdentifier = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBInstancesRequest();
        if (dbInstanceIdentifier != null) request.DBInstanceIdentifier = dbInstanceIdentifier;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBInstancesAsync(request);
            return new DescribeDBInstancesResult(
                DBInstances: resp.DBInstances,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB instances");
        }
    }

    /// <summary>
    /// Modify an RDS DB instance.
    /// </summary>
    public static async Task<ModifyDBInstanceResult> ModifyDBInstanceAsync(
        ModifyDBInstanceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new ModifyDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to modify DB instance");
        }
    }

    /// <summary>
    /// Reboot an RDS DB instance.
    /// </summary>
    public static async Task<RebootDBInstanceResult> RebootDBInstanceAsync(
        string dbInstanceIdentifier,
        bool? forceFailover = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RebootDBInstanceRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier
        };
        if (forceFailover.HasValue) request.ForceFailover = forceFailover.Value;

        try
        {
            var resp = await client.RebootDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new RebootDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to reboot DB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Start a stopped RDS DB instance.
    /// </summary>
    public static async Task<StartDBInstanceResult> StartDBInstanceAsync(
        string dbInstanceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartDBInstanceAsync(new StartDBInstanceRequest
            {
                DBInstanceIdentifier = dbInstanceIdentifier
            });
            var db = resp.DBInstance;
            return new StartDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start DB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Stop a running RDS DB instance.
    /// </summary>
    public static async Task<StopDBInstanceResult> StopDBInstanceAsync(
        string dbInstanceIdentifier,
        string? dbSnapshotIdentifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopDBInstanceRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier
        };
        if (dbSnapshotIdentifier != null) request.DBSnapshotIdentifier = dbSnapshotIdentifier;

        try
        {
            var resp = await client.StopDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new StopDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop DB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Create a new RDS DB cluster.
    /// </summary>
    public static async Task<CreateDBClusterResult> CreateDBClusterAsync(
        CreateDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBClusterAsync(request);
            var c = resp.DBCluster;
            return new CreateDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status,
                Engine: c.Engine);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB cluster");
        }
    }

    /// <summary>
    /// Delete an RDS DB cluster.
    /// </summary>
    public static async Task<DeleteDBClusterResult> DeleteDBClusterAsync(
        string dbClusterIdentifier,
        bool skipFinalSnapshot = false,
        string? finalDBSnapshotIdentifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDBClusterRequest
        {
            DBClusterIdentifier = dbClusterIdentifier,
            SkipFinalSnapshot = skipFinalSnapshot
        };
        if (finalDBSnapshotIdentifier != null)
            request.FinalDBSnapshotIdentifier = finalDBSnapshotIdentifier;

        try
        {
            var resp = await client.DeleteDBClusterAsync(request);
            var c = resp.DBCluster;
            return new DeleteDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe RDS DB clusters.
    /// </summary>
    public static async Task<DescribeDBClustersResult> DescribeDBClustersAsync(
        string? dbClusterIdentifier = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBClustersRequest();
        if (dbClusterIdentifier != null) request.DBClusterIdentifier = dbClusterIdentifier;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBClustersAsync(request);
            return new DescribeDBClustersResult(
                DBClusters: resp.DBClusters,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB clusters");
        }
    }

    /// <summary>
    /// Modify an RDS DB cluster.
    /// </summary>
    public static async Task<ModifyDBClusterResult> ModifyDBClusterAsync(
        ModifyDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBClusterAsync(request);
            var c = resp.DBCluster;
            return new ModifyDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to modify DB cluster");
        }
    }

    /// <summary>
    /// Create a manual DB snapshot.
    /// </summary>
    public static async Task<CreateDBSnapshotResult> CreateDBSnapshotAsync(
        string dbInstanceIdentifier,
        string dbSnapshotIdentifier,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDBSnapshotRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier,
            DBSnapshotIdentifier = dbSnapshotIdentifier
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDBSnapshotAsync(request);
            var s = resp.DBSnapshot;
            return new CreateDBSnapshotResult(
                DBSnapshotIdentifier: s.DBSnapshotIdentifier,
                DBSnapshotArn: s.DBSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB snapshot");
        }
    }

    /// <summary>
    /// Delete a manual DB snapshot.
    /// </summary>
    public static async Task<DeleteDBSnapshotResult> DeleteDBSnapshotAsync(
        string dbSnapshotIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDBSnapshotAsync(new DeleteDBSnapshotRequest
            {
                DBSnapshotIdentifier = dbSnapshotIdentifier
            });
            var s = resp.DBSnapshot;
            return new DeleteDBSnapshotResult(
                DBSnapshotIdentifier: s.DBSnapshotIdentifier,
                DBSnapshotArn: s.DBSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB snapshot '{dbSnapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DB snapshots.
    /// </summary>
    public static async Task<DescribeDBSnapshotsResult> DescribeDBSnapshotsAsync(
        string? dbInstanceIdentifier = null,
        string? dbSnapshotIdentifier = null,
        string? snapshotType = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBSnapshotsRequest();
        if (dbInstanceIdentifier != null) request.DBInstanceIdentifier = dbInstanceIdentifier;
        if (dbSnapshotIdentifier != null) request.DBSnapshotIdentifier = dbSnapshotIdentifier;
        if (snapshotType != null) request.SnapshotType = snapshotType;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBSnapshotsAsync(request);
            return new DescribeDBSnapshotsResult(
                DBSnapshots: resp.DBSnapshots,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB snapshots");
        }
    }

    /// <summary>
    /// Restore a DB instance from a snapshot.
    /// </summary>
    public static async Task<RestoreDBInstanceFromDBSnapshotResult> RestoreDBInstanceFromDBSnapshotAsync(
        RestoreDBInstanceFromDBSnapshotRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreDBInstanceFromDBSnapshotAsync(request);
            var db = resp.DBInstance;
            return new RestoreDBInstanceFromDBSnapshotResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to restore DB instance from snapshot");
        }
    }

    /// <summary>
    /// Create a DB cluster snapshot.
    /// </summary>
    public static async Task<CreateDBClusterSnapshotResult> CreateDBClusterSnapshotAsync(
        string dbClusterIdentifier,
        string dbClusterSnapshotIdentifier,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDBClusterSnapshotRequest
        {
            DBClusterIdentifier = dbClusterIdentifier,
            DBClusterSnapshotIdentifier = dbClusterSnapshotIdentifier
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDBClusterSnapshotAsync(request);
            var s = resp.DBClusterSnapshot;
            return new CreateDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB cluster snapshot");
        }
    }

    /// <summary>
    /// Delete a DB cluster snapshot.
    /// </summary>
    public static async Task<DeleteDBClusterSnapshotResult> DeleteDBClusterSnapshotAsync(
        string dbClusterSnapshotIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDBClusterSnapshotAsync(new DeleteDBClusterSnapshotRequest
            {
                DBClusterSnapshotIdentifier = dbClusterSnapshotIdentifier
            });
            var s = resp.DBClusterSnapshot;
            return new DeleteDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB cluster snapshot '{dbClusterSnapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DB cluster snapshots.
    /// </summary>
    public static async Task<DescribeDBClusterSnapshotsResult> DescribeDBClusterSnapshotsAsync(
        string? dbClusterIdentifier = null,
        string? dbClusterSnapshotIdentifier = null,
        string? snapshotType = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBClusterSnapshotsRequest();
        if (dbClusterIdentifier != null) request.DBClusterIdentifier = dbClusterIdentifier;
        if (dbClusterSnapshotIdentifier != null) request.DBClusterSnapshotIdentifier = dbClusterSnapshotIdentifier;
        if (snapshotType != null) request.SnapshotType = snapshotType;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBClusterSnapshotsAsync(request);
            return new DescribeDBClusterSnapshotsResult(
                DBClusterSnapshots: resp.DBClusterSnapshots,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB cluster snapshots");
        }
    }

    /// <summary>
    /// Create a DB subnet group.
    /// </summary>
    public static async Task<CreateDBSubnetGroupResult> CreateDBSubnetGroupAsync(
        string dbSubnetGroupName,
        string dbSubnetGroupDescription,
        List<string> subnetIds,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDBSubnetGroupRequest
        {
            DBSubnetGroupName = dbSubnetGroupName,
            DBSubnetGroupDescription = dbSubnetGroupDescription,
            SubnetIds = subnetIds
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDBSubnetGroupAsync(request);
            var g = resp.DBSubnetGroup;
            return new CreateDBSubnetGroupResult(
                DBSubnetGroupName: g.DBSubnetGroupName,
                DBSubnetGroupArn: g.DBSubnetGroupArn,
                DBSubnetGroupDescription: g.DBSubnetGroupDescription);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB subnet group");
        }
    }

    /// <summary>
    /// Delete a DB subnet group.
    /// </summary>
    public static async Task DeleteDBSubnetGroupAsync(
        string dbSubnetGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDBSubnetGroupAsync(new DeleteDBSubnetGroupRequest
            {
                DBSubnetGroupName = dbSubnetGroupName
            });
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB subnet group '{dbSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe DB subnet groups.
    /// </summary>
    public static async Task<DescribeDBSubnetGroupsResult> DescribeDBSubnetGroupsAsync(
        string? dbSubnetGroupName = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBSubnetGroupsRequest();
        if (dbSubnetGroupName != null) request.DBSubnetGroupName = dbSubnetGroupName;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBSubnetGroupsAsync(request);
            return new DescribeDBSubnetGroupsResult(
                DBSubnetGroups: resp.DBSubnetGroups,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB subnet groups");
        }
    }

    /// <summary>
    /// Create a DB parameter group.
    /// </summary>
    public static async Task<CreateDBParameterGroupResult> CreateDBParameterGroupAsync(
        string dbParameterGroupName,
        string dbParameterGroupFamily,
        string description,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDBParameterGroupRequest
        {
            DBParameterGroupName = dbParameterGroupName,
            DBParameterGroupFamily = dbParameterGroupFamily,
            Description = description
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDBParameterGroupAsync(request);
            var g = resp.DBParameterGroup;
            return new CreateDBParameterGroupResult(
                DBParameterGroupName: g.DBParameterGroupName,
                DBParameterGroupArn: g.DBParameterGroupArn,
                DBParameterGroupFamily: g.DBParameterGroupFamily);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create DB parameter group");
        }
    }

    /// <summary>
    /// Delete a DB parameter group.
    /// </summary>
    public static async Task DeleteDBParameterGroupAsync(
        string dbParameterGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDBParameterGroupAsync(new DeleteDBParameterGroupRequest
            {
                DBParameterGroupName = dbParameterGroupName
            });
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete DB parameter group '{dbParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe DB parameter groups.
    /// </summary>
    public static async Task<DescribeDBParameterGroupsResult> DescribeDBParameterGroupsAsync(
        string? dbParameterGroupName = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBParameterGroupsRequest();
        if (dbParameterGroupName != null) request.DBParameterGroupName = dbParameterGroupName;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBParameterGroupsAsync(request);
            return new DescribeDBParameterGroupsResult(
                DBParameterGroups: resp.DBParameterGroups,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB parameter groups");
        }
    }

    /// <summary>
    /// Modify a DB parameter group.
    /// </summary>
    public static async Task<ModifyDBParameterGroupResult> ModifyDBParameterGroupAsync(
        string dbParameterGroupName,
        List<Parameter> parameters,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBParameterGroupAsync(new ModifyDBParameterGroupRequest
            {
                DBParameterGroupName = dbParameterGroupName,
                Parameters = parameters
            });
            return new ModifyDBParameterGroupResult(
                DBParameterGroupName: resp.DBParameterGroupName);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to modify DB parameter group '{dbParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe parameters in a DB parameter group.
    /// </summary>
    public static async Task<DescribeDBParametersResult> DescribeDBParametersAsync(
        string dbParameterGroupName,
        string? source = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBParametersRequest
        {
            DBParameterGroupName = dbParameterGroupName
        };
        if (source != null) request.Source = source;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBParametersAsync(request);
            return new DescribeDBParametersResult(
                Parameters: resp.Parameters,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe DB parameters for '{dbParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe DB engine versions.
    /// </summary>
    public static async Task<DescribeDBEngineVersionsResult> DescribeDBEngineVersionsAsync(
        string? engine = null,
        string? engineVersion = null,
        string? dbParameterGroupFamily = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBEngineVersionsRequest();
        if (engine != null) request.Engine = engine;
        if (engineVersion != null) request.EngineVersion = engineVersion;
        if (dbParameterGroupFamily != null) request.DBParameterGroupFamily = dbParameterGroupFamily;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBEngineVersionsAsync(request);
            return new DescribeDBEngineVersionsResult(
                DBEngineVersions: resp.DBEngineVersions,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe DB engine versions");
        }
    }

    /// <summary>
    /// Describe orderable DB instance options.
    /// </summary>
    public static async Task<DescribeOrderableDBInstanceOptionsResult> DescribeOrderableDBInstanceOptionsAsync(
        string engine,
        string? engineVersion = null,
        string? dbInstanceClass = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeOrderableDBInstanceOptionsRequest
        {
            Engine = engine
        };
        if (engineVersion != null) request.EngineVersion = engineVersion;
        if (dbInstanceClass != null) request.DBInstanceClass = dbInstanceClass;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeOrderableDBInstanceOptionsAsync(request);
            return new DescribeOrderableDBInstanceOptionsResult(
                OrderableDBInstanceOptions: resp.OrderableDBInstanceOptions,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe orderable DB instance options");
        }
    }

    /// <summary>
    /// Create an event notification subscription.
    /// </summary>
    public static async Task<CreateEventSubscriptionResult> CreateEventSubscriptionAsync(
        CreateEventSubscriptionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEventSubscriptionAsync(request);
            var s = resp.EventSubscription;
            return new CreateEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create event subscription");
        }
    }

    /// <summary>
    /// Delete an event notification subscription.
    /// </summary>
    public static async Task<DeleteEventSubscriptionResult> DeleteEventSubscriptionAsync(
        string subscriptionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteEventSubscriptionAsync(new DeleteEventSubscriptionRequest
            {
                SubscriptionName = subscriptionName
            });
            var s = resp.EventSubscription;
            return new DeleteEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete event subscription '{subscriptionName}'");
        }
    }

    /// <summary>
    /// Describe event notification subscriptions.
    /// </summary>
    public static async Task<DescribeEventSubscriptionsResult> DescribeEventSubscriptionsAsync(
        string? subscriptionName = null,
        List<Filter>? filters = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventSubscriptionsRequest();
        if (subscriptionName != null) request.SubscriptionName = subscriptionName;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeEventSubscriptionsAsync(request);
            return new DescribeEventSubscriptionsResult(
                EventSubscriptionsList: resp.EventSubscriptionsList,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe event subscriptions");
        }
    }

    /// <summary>
    /// Describe RDS events.
    /// </summary>
    public static async Task<DescribeEventsResult> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new DescribeEventsResult(
                Events: resp.Events,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe events");
        }
    }

    /// <summary>
    /// Add tags to an RDS resource.
    /// </summary>
    public static async Task AddTagsToResourceAsync(
        string resourceName,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToResourceAsync(new AddTagsToResourceRequest
            {
                ResourceName = resourceName,
                Tags = tags
            });
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add tags to resource");
        }
    }

    /// <summary>
    /// Remove tags from an RDS resource.
    /// </summary>
    public static async Task RemoveTagsFromResourceAsync(
        string resourceName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
            {
                ResourceName = resourceName,
                TagKeys = tagKeys
            });
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove tags from resource");
        }
    }

    /// <summary>
    /// List tags for an RDS resource.
    /// </summary>
    public static async Task<ListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(new ListTagsForResourceRequest
            {
                ResourceName = resourceName
            });
            return new ListTagsForResourceResult(TagList: resp.TagList);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags for resource");
        }
    }

    /// <summary>
    /// Promote a read replica to a standalone DB instance.
    /// </summary>
    public static async Task<PromoteReadReplicaResult> PromoteReadReplicaAsync(
        string dbInstanceIdentifier,
        string? backupRetentionPeriod = null,
        string? preferredBackupWindow = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PromoteReadReplicaRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier
        };
        if (backupRetentionPeriod != null)
            request.BackupRetentionPeriod = int.Parse(backupRetentionPeriod);
        if (preferredBackupWindow != null)
            request.PreferredBackupWindow = preferredBackupWindow;

        try
        {
            var resp = await client.PromoteReadReplicaAsync(request);
            var db = resp.DBInstance;
            return new PromoteReadReplicaResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to promote read replica '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Create a read replica of an existing DB instance.
    /// </summary>
    public static async Task<CreateDBInstanceReadReplicaResult> CreateDBInstanceReadReplicaAsync(
        CreateDBInstanceReadReplicaRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBInstanceReadReplicaAsync(request);
            var db = resp.DBInstance;
            return new CreateDBInstanceReadReplicaResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create read replica");
        }
    }

    /// <summary>
    /// Describe DB log files for an instance.
    /// </summary>
    public static async Task<DescribeDBLogFilesResult> DescribeDBLogFilesAsync(
        string dbInstanceIdentifier,
        string? filenameContains = null,
        long? fileLastWritten = null,
        long? fileSize = null,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBLogFilesRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier
        };
        if (filenameContains != null) request.FilenameContains = filenameContains;
        if (fileLastWritten.HasValue) request.FileLastWritten = fileLastWritten.Value;
        if (fileSize.HasValue) request.FileSize = fileSize.Value;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBLogFilesAsync(request);
            return new DescribeDBLogFilesResult(
                DescribeDBLogFiles: resp.DescribeDBLogFiles,
                Marker: resp.Marker);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe DB log files for '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Download a portion of a DB log file.
    /// </summary>
    public static async Task<DownloadDBLogFilePortionResult> DownloadDBLogFilePortionAsync(
        string dbInstanceIdentifier,
        string logFileName,
        string? marker = null,
        int? numberOfLines = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DownloadDBLogFilePortionRequest
        {
            DBInstanceIdentifier = dbInstanceIdentifier,
            LogFileName = logFileName
        };
        if (marker != null) request.Marker = marker;
        if (numberOfLines.HasValue) request.NumberOfLines = numberOfLines.Value;

        try
        {
            var resp = await client.DownloadDBLogFilePortionAsync(request);
            return new DownloadDBLogFilePortionResult(
                LogFileData: resp.LogFileData,
                Marker: resp.Marker,
                AdditionalDataPending: resp.AdditionalDataPending);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to download DB log file portion for '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Force a failover for a DB cluster.
    /// </summary>
    public static async Task<FailoverDBClusterResult> FailoverDBClusterAsync(
        string dbClusterIdentifier,
        string? targetDBInstanceIdentifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new FailoverDBClusterRequest
        {
            DBClusterIdentifier = dbClusterIdentifier
        };
        if (targetDBInstanceIdentifier != null)
            request.TargetDBInstanceIdentifier = targetDBInstanceIdentifier;

        try
        {
            var resp = await client.FailoverDBClusterAsync(request);
            var c = resp.DBCluster;
            return new FailoverDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to failover DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Switchover a read replica to become the new primary instance.
    /// </summary>
    public static async Task<SwitchoverReadReplicaResult> SwitchoverReadReplicaAsync(
        string dbInstanceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SwitchoverReadReplicaAsync(new SwitchoverReadReplicaRequest
            {
                DBInstanceIdentifier = dbInstanceIdentifier
            });
            var db = resp.DBInstance;
            return new SwitchoverReadReplicaResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonRDSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to switchover read replica '{dbInstanceIdentifier}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateDBInstanceAsync"/>.</summary>
    public static CreateDBInstanceResult CreateDBInstance(CreateDBInstanceRequest request, RegionEndpoint? region = null)
        => CreateDBInstanceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBInstanceAsync"/>.</summary>
    public static DeleteDBInstanceResult DeleteDBInstance(string dbInstanceIdentifier, bool skipFinalSnapshot = false, string? finalDBSnapshotIdentifier = null, RegionEndpoint? region = null)
        => DeleteDBInstanceAsync(dbInstanceIdentifier, skipFinalSnapshot, finalDBSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBInstancesAsync"/>.</summary>
    public static DescribeDBInstancesResult DescribeDBInstances(string? dbInstanceIdentifier = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBInstancesAsync(dbInstanceIdentifier, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyDBInstanceAsync"/>.</summary>
    public static ModifyDBInstanceResult ModifyDBInstance(ModifyDBInstanceRequest request, RegionEndpoint? region = null)
        => ModifyDBInstanceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RebootDBInstanceAsync"/>.</summary>
    public static RebootDBInstanceResult RebootDBInstance(string dbInstanceIdentifier, bool? forceFailover = null, RegionEndpoint? region = null)
        => RebootDBInstanceAsync(dbInstanceIdentifier, forceFailover, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartDBInstanceAsync"/>.</summary>
    public static StartDBInstanceResult StartDBInstance(string dbInstanceIdentifier, RegionEndpoint? region = null)
        => StartDBInstanceAsync(dbInstanceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopDBInstanceAsync"/>.</summary>
    public static StopDBInstanceResult StopDBInstance(string dbInstanceIdentifier, string? dbSnapshotIdentifier = null, RegionEndpoint? region = null)
        => StopDBInstanceAsync(dbInstanceIdentifier, dbSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBClusterAsync"/>.</summary>
    public static CreateDBClusterResult CreateDBCluster(CreateDBClusterRequest request, RegionEndpoint? region = null)
        => CreateDBClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBClusterAsync"/>.</summary>
    public static DeleteDBClusterResult DeleteDBCluster(string dbClusterIdentifier, bool skipFinalSnapshot = false, string? finalDBSnapshotIdentifier = null, RegionEndpoint? region = null)
        => DeleteDBClusterAsync(dbClusterIdentifier, skipFinalSnapshot, finalDBSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBClustersAsync"/>.</summary>
    public static DescribeDBClustersResult DescribeDBClusters(string? dbClusterIdentifier = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBClustersAsync(dbClusterIdentifier, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyDBClusterAsync"/>.</summary>
    public static ModifyDBClusterResult ModifyDBCluster(ModifyDBClusterRequest request, RegionEndpoint? region = null)
        => ModifyDBClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBSnapshotAsync"/>.</summary>
    public static CreateDBSnapshotResult CreateDBSnapshot(string dbInstanceIdentifier, string dbSnapshotIdentifier, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDBSnapshotAsync(dbInstanceIdentifier, dbSnapshotIdentifier, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBSnapshotAsync"/>.</summary>
    public static DeleteDBSnapshotResult DeleteDBSnapshot(string dbSnapshotIdentifier, RegionEndpoint? region = null)
        => DeleteDBSnapshotAsync(dbSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBSnapshotsAsync"/>.</summary>
    public static DescribeDBSnapshotsResult DescribeDBSnapshots(string? dbInstanceIdentifier = null, string? dbSnapshotIdentifier = null, string? snapshotType = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBSnapshotsAsync(dbInstanceIdentifier, dbSnapshotIdentifier, snapshotType, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RestoreDBInstanceFromDBSnapshotAsync"/>.</summary>
    public static RestoreDBInstanceFromDBSnapshotResult RestoreDBInstanceFromDBSnapshot(RestoreDBInstanceFromDBSnapshotRequest request, RegionEndpoint? region = null)
        => RestoreDBInstanceFromDBSnapshotAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBClusterSnapshotAsync"/>.</summary>
    public static CreateDBClusterSnapshotResult CreateDBClusterSnapshot(string dbClusterIdentifier, string dbClusterSnapshotIdentifier, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDBClusterSnapshotAsync(dbClusterIdentifier, dbClusterSnapshotIdentifier, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBClusterSnapshotAsync"/>.</summary>
    public static DeleteDBClusterSnapshotResult DeleteDBClusterSnapshot(string dbClusterSnapshotIdentifier, RegionEndpoint? region = null)
        => DeleteDBClusterSnapshotAsync(dbClusterSnapshotIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBClusterSnapshotsAsync"/>.</summary>
    public static DescribeDBClusterSnapshotsResult DescribeDBClusterSnapshots(string? dbClusterIdentifier = null, string? dbClusterSnapshotIdentifier = null, string? snapshotType = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBClusterSnapshotsAsync(dbClusterIdentifier, dbClusterSnapshotIdentifier, snapshotType, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBSubnetGroupAsync"/>.</summary>
    public static CreateDBSubnetGroupResult CreateDBSubnetGroup(string dbSubnetGroupName, string dbSubnetGroupDescription, List<string> subnetIds, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDBSubnetGroupAsync(dbSubnetGroupName, dbSubnetGroupDescription, subnetIds, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBSubnetGroupAsync"/>.</summary>
    public static void DeleteDBSubnetGroup(string dbSubnetGroupName, RegionEndpoint? region = null)
        => DeleteDBSubnetGroupAsync(dbSubnetGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBSubnetGroupsAsync"/>.</summary>
    public static DescribeDBSubnetGroupsResult DescribeDBSubnetGroups(string? dbSubnetGroupName = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBSubnetGroupsAsync(dbSubnetGroupName, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBParameterGroupAsync"/>.</summary>
    public static CreateDBParameterGroupResult CreateDBParameterGroup(string dbParameterGroupName, string dbParameterGroupFamily, string description, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDBParameterGroupAsync(dbParameterGroupName, dbParameterGroupFamily, description, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDBParameterGroupAsync"/>.</summary>
    public static void DeleteDBParameterGroup(string dbParameterGroupName, RegionEndpoint? region = null)
        => DeleteDBParameterGroupAsync(dbParameterGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBParameterGroupsAsync"/>.</summary>
    public static DescribeDBParameterGroupsResult DescribeDBParameterGroups(string? dbParameterGroupName = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBParameterGroupsAsync(dbParameterGroupName, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyDBParameterGroupAsync"/>.</summary>
    public static ModifyDBParameterGroupResult ModifyDBParameterGroup(string dbParameterGroupName, List<Parameter> parameters, RegionEndpoint? region = null)
        => ModifyDBParameterGroupAsync(dbParameterGroupName, parameters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBParametersAsync"/>.</summary>
    public static DescribeDBParametersResult DescribeDBParameters(string dbParameterGroupName, string? source = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBParametersAsync(dbParameterGroupName, source, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBEngineVersionsAsync"/>.</summary>
    public static DescribeDBEngineVersionsResult DescribeDBEngineVersions(string? engine = null, string? engineVersion = null, string? dbParameterGroupFamily = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBEngineVersionsAsync(engine, engineVersion, dbParameterGroupFamily, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeOrderableDBInstanceOptionsAsync"/>.</summary>
    public static DescribeOrderableDBInstanceOptionsResult DescribeOrderableDBInstanceOptions(string engine, string? engineVersion = null, string? dbInstanceClass = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeOrderableDBInstanceOptionsAsync(engine, engineVersion, dbInstanceClass, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateEventSubscriptionAsync"/>.</summary>
    public static CreateEventSubscriptionResult CreateEventSubscription(CreateEventSubscriptionRequest request, RegionEndpoint? region = null)
        => CreateEventSubscriptionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteEventSubscriptionAsync"/>.</summary>
    public static DeleteEventSubscriptionResult DeleteEventSubscription(string subscriptionName, RegionEndpoint? region = null)
        => DeleteEventSubscriptionAsync(subscriptionName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEventSubscriptionsAsync"/>.</summary>
    public static DescribeEventSubscriptionsResult DescribeEventSubscriptions(string? subscriptionName = null, List<Filter>? filters = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeEventSubscriptionsAsync(subscriptionName, filters, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEventsAsync"/>.</summary>
    public static DescribeEventsResult DescribeEvents(DescribeEventsRequest? request = null, RegionEndpoint? region = null)
        => DescribeEventsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToResourceAsync"/>.</summary>
    public static void AddTagsToResource(string resourceName, List<Tag> tags, RegionEndpoint? region = null)
        => AddTagsToResourceAsync(resourceName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromResourceAsync"/>.</summary>
    public static void RemoveTagsFromResource(string resourceName, List<string> tagKeys, RegionEndpoint? region = null)
        => RemoveTagsFromResourceAsync(resourceName, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListTagsForResourceResult ListTagsForResource(string resourceName, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PromoteReadReplicaAsync"/>.</summary>
    public static PromoteReadReplicaResult PromoteReadReplica(string dbInstanceIdentifier, string? backupRetentionPeriod = null, string? preferredBackupWindow = null, RegionEndpoint? region = null)
        => PromoteReadReplicaAsync(dbInstanceIdentifier, backupRetentionPeriod, preferredBackupWindow, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDBInstanceReadReplicaAsync"/>.</summary>
    public static CreateDBInstanceReadReplicaResult CreateDBInstanceReadReplica(CreateDBInstanceReadReplicaRequest request, RegionEndpoint? region = null)
        => CreateDBInstanceReadReplicaAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDBLogFilesAsync"/>.</summary>
    public static DescribeDBLogFilesResult DescribeDBLogFiles(string dbInstanceIdentifier, string? filenameContains = null, long? fileLastWritten = null, long? fileSize = null, string? marker = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeDBLogFilesAsync(dbInstanceIdentifier, filenameContains, fileLastWritten, fileSize, marker, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DownloadDBLogFilePortionAsync"/>.</summary>
    public static DownloadDBLogFilePortionResult DownloadDBLogFilePortion(string dbInstanceIdentifier, string logFileName, string? marker = null, int? numberOfLines = null, RegionEndpoint? region = null)
        => DownloadDBLogFilePortionAsync(dbInstanceIdentifier, logFileName, marker, numberOfLines, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="FailoverDBClusterAsync"/>.</summary>
    public static FailoverDBClusterResult FailoverDBCluster(string dbClusterIdentifier, string? targetDBInstanceIdentifier = null, RegionEndpoint? region = null)
        => FailoverDBClusterAsync(dbClusterIdentifier, targetDBInstanceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SwitchoverReadReplicaAsync"/>.</summary>
    public static SwitchoverReadReplicaResult SwitchoverReadReplica(string dbInstanceIdentifier, RegionEndpoint? region = null)
        => SwitchoverReadReplicaAsync(dbInstanceIdentifier, region).GetAwaiter().GetResult();

}
