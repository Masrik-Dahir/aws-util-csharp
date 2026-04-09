using Amazon;
using Amazon.DocDB;
using Amazon.DocDB.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record DocDbCreateDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null,
    string? Engine = null);

public sealed record DocDbDeleteDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbDescribeDBClustersResult(
    List<DBCluster>? DBClusters = null,
    string? Marker = null);

public sealed record DocDbModifyDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbCreateDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null,
    string? Engine = null,
    string? DBInstanceClass = null);

public sealed record DocDbDeleteDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record DocDbDescribeDBInstancesResult(
    List<DBInstance>? DBInstances = null,
    string? Marker = null);

public sealed record DocDbModifyDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record DocDbRebootDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record DocDbCreateDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record DocDbDeleteDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record DocDbDescribeDBClusterSnapshotsResult(
    List<DBClusterSnapshot>? DBClusterSnapshots = null,
    string? Marker = null);

public sealed record DocDbRestoreDBClusterFromSnapshotResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbCreateDBClusterParameterGroupResult(
    string? DBClusterParameterGroupName = null,
    string? DBClusterParameterGroupArn = null,
    string? DBParameterGroupFamily = null);

public sealed record DocDbDescribeDBClusterParameterGroupsResult(
    List<DBClusterParameterGroup>? DBClusterParameterGroups = null,
    string? Marker = null);

public sealed record DocDbModifyDBClusterParameterGroupResult(
    string? DBClusterParameterGroupName = null);

public sealed record DocDbDescribeDBClusterParametersResult(
    List<Parameter>? Parameters = null,
    string? Marker = null);

public sealed record DocDbCreateDBSubnetGroupResult(
    string? DBSubnetGroupName = null,
    string? DBSubnetGroupArn = null,
    string? DBSubnetGroupDescription = null);

public sealed record DocDbDescribeDBSubnetGroupsResult(
    List<DBSubnetGroup>? DBSubnetGroups = null,
    string? Marker = null);

public sealed record DocDbCreateEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DocDbDeleteEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DocDbDescribeEventSubscriptionsResult(
    List<EventSubscription>? EventSubscriptionsList = null,
    string? Marker = null);

public sealed record DocDbDescribeEventsResult(
    List<Event>? Events = null,
    string? Marker = null);

public sealed record DocDbListTagsForResourceResult(
    List<Tag>? TagList = null);

public sealed record DocDbFailoverDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbStopDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbStartDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbCopyDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record DocDbRestoreDBClusterToPointInTimeResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record DocDbDescribeDBEngineVersionsResult(
    List<DBEngineVersion>? DBEngineVersions = null,
    string? Marker = null);

public sealed record DocDbDescribeOrderableDBInstanceOptionsResult(
    List<OrderableDBInstanceOption>? OrderableDBInstanceOptions = null,
    string? Marker = null);

public sealed record DocDbDescribeCertificatesResult(
    List<Certificate>? Certificates = null,
    string? Marker = null);

public sealed record DocDbCreateGlobalClusterResult(
    string? GlobalClusterIdentifier = null,
    string? GlobalClusterArn = null,
    string? Status = null);

public sealed record DocDbDeleteGlobalClusterResult(
    string? GlobalClusterIdentifier = null,
    string? GlobalClusterArn = null,
    string? Status = null);

public sealed record DocDbDescribeGlobalClustersResult(
    List<GlobalCluster>? GlobalClusters = null,
    string? Marker = null);

public sealed record DocDbModifyGlobalClusterResult(
    string? GlobalClusterIdentifier = null,
    string? GlobalClusterArn = null,
    string? Status = null);

public sealed record DocDbRemoveFromGlobalClusterResult(
    string? GlobalClusterIdentifier = null,
    string? GlobalClusterArn = null,
    string? Status = null);

public sealed record DocDbSwitchoverGlobalClusterResult(
    string? GlobalClusterIdentifier = null,
    string? GlobalClusterArn = null,
    string? Status = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon DocumentDB.
/// </summary>
public static class DocumentDbService
{
    private static AmazonDocDBClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonDocDBClient>(region);

    // ── Cluster operations ──────────────────────────────────────────

    /// <summary>
    /// Create a new DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbCreateDBClusterResult> CreateDBClusterAsync(
        CreateDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBClusterAsync(request);
            var c = resp.DBCluster;
            return new DocDbCreateDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status,
                Engine: c.Engine);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB cluster");
        }
    }

    /// <summary>
    /// Delete a DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbDeleteDBClusterResult> DeleteDBClusterAsync(
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
            return new DocDbDeleteDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB clusters.
    /// </summary>
    public static async Task<DocDbDescribeDBClustersResult>
        DescribeDBClustersAsync(
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
            return new DocDbDescribeDBClustersResult(
                DBClusters: resp.DBClusters,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB clusters");
        }
    }

    /// <summary>
    /// Modify a DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbModifyDBClusterResult> ModifyDBClusterAsync(
        ModifyDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBClusterAsync(request);
            var c = resp.DBCluster;
            return new DocDbModifyDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DocumentDB cluster");
        }
    }

    // ── Instance operations ─────────────────────────────────────────

    /// <summary>
    /// Create a new DocumentDB instance.
    /// </summary>
    public static async Task<DocDbCreateDBInstanceResult>
        CreateDBInstanceAsync(
            CreateDBInstanceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new DocDbCreateDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus,
                Engine: db.Engine,
                DBInstanceClass: db.DBInstanceClass);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB instance");
        }
    }

    /// <summary>
    /// Delete a DocumentDB instance.
    /// </summary>
    public static async Task<DocDbDeleteDBInstanceResult>
        DeleteDBInstanceAsync(
            string dbInstanceIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDBInstanceAsync(
                new DeleteDBInstanceRequest
                {
                    DBInstanceIdentifier = dbInstanceIdentifier
                });
            var db = resp.DBInstance;
            return new DocDbDeleteDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB instances.
    /// </summary>
    public static async Task<DocDbDescribeDBInstancesResult>
        DescribeDBInstancesAsync(
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
            return new DocDbDescribeDBInstancesResult(
                DBInstances: resp.DBInstances,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB instances");
        }
    }

    /// <summary>
    /// Modify a DocumentDB instance.
    /// </summary>
    public static async Task<DocDbModifyDBInstanceResult>
        ModifyDBInstanceAsync(
            ModifyDBInstanceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new DocDbModifyDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DocumentDB instance");
        }
    }

    /// <summary>
    /// Reboot a DocumentDB instance.
    /// </summary>
    public static async Task<DocDbRebootDBInstanceResult>
        RebootDBInstanceAsync(
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
            return new DocDbRebootDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot DocumentDB instance '{dbInstanceIdentifier}'");
        }
    }

    // ── Cluster snapshot operations ─────────────────────────────────

    /// <summary>
    /// Create a DocumentDB cluster snapshot.
    /// </summary>
    public static async Task<DocDbCreateDBClusterSnapshotResult>
        CreateDBClusterSnapshotAsync(
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
            return new DocDbCreateDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB cluster snapshot");
        }
    }

    /// <summary>
    /// Delete a DocumentDB cluster snapshot.
    /// </summary>
    public static async Task<DocDbDeleteDBClusterSnapshotResult>
        DeleteDBClusterSnapshotAsync(
            string dbClusterSnapshotIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDBClusterSnapshotAsync(
                new DeleteDBClusterSnapshotRequest
                {
                    DBClusterSnapshotIdentifier = dbClusterSnapshotIdentifier
                });
            var s = resp.DBClusterSnapshot;
            return new DocDbDeleteDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB cluster snapshot '{dbClusterSnapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB cluster snapshots.
    /// </summary>
    public static async Task<DocDbDescribeDBClusterSnapshotsResult>
        DescribeDBClusterSnapshotsAsync(
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
        if (dbClusterSnapshotIdentifier != null)
            request.DBClusterSnapshotIdentifier = dbClusterSnapshotIdentifier;
        if (snapshotType != null) request.SnapshotType = snapshotType;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBClusterSnapshotsAsync(request);
            return new DocDbDescribeDBClusterSnapshotsResult(
                DBClusterSnapshots: resp.DBClusterSnapshots,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB cluster snapshots");
        }
    }

    /// <summary>
    /// Restore a DocumentDB cluster from a snapshot.
    /// </summary>
    public static async Task<DocDbRestoreDBClusterFromSnapshotResult>
        RestoreDBClusterFromSnapshotAsync(
            RestoreDBClusterFromSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreDBClusterFromSnapshotAsync(request);
            var c = resp.DBCluster;
            return new DocDbRestoreDBClusterFromSnapshotResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore DocumentDB cluster from snapshot");
        }
    }

    // ── Cluster parameter group operations ──────────────────────────

    /// <summary>
    /// Create a DocumentDB cluster parameter group.
    /// </summary>
    public static async Task<DocDbCreateDBClusterParameterGroupResult>
        CreateDBClusterParameterGroupAsync(
            string dbClusterParameterGroupName,
            string dbParameterGroupFamily,
            string description,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDBClusterParameterGroupRequest
        {
            DBClusterParameterGroupName = dbClusterParameterGroupName,
            DBParameterGroupFamily = dbParameterGroupFamily,
            Description = description
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDBClusterParameterGroupAsync(request);
            var g = resp.DBClusterParameterGroup;
            return new DocDbCreateDBClusterParameterGroupResult(
                DBClusterParameterGroupName: g.DBClusterParameterGroupName,
                DBClusterParameterGroupArn: g.DBClusterParameterGroupArn,
                DBParameterGroupFamily: g.DBParameterGroupFamily);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB cluster parameter group");
        }
    }

    /// <summary>
    /// Delete a DocumentDB cluster parameter group.
    /// </summary>
    public static async Task DeleteDBClusterParameterGroupAsync(
        string dbClusterParameterGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDBClusterParameterGroupAsync(
                new DeleteDBClusterParameterGroupRequest
                {
                    DBClusterParameterGroupName = dbClusterParameterGroupName
                });
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB cluster parameter group '{dbClusterParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB cluster parameter groups.
    /// </summary>
    public static async Task<DocDbDescribeDBClusterParameterGroupsResult>
        DescribeDBClusterParameterGroupsAsync(
            string? dbClusterParameterGroupName = null,
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBClusterParameterGroupsRequest();
        if (dbClusterParameterGroupName != null)
            request.DBClusterParameterGroupName = dbClusterParameterGroupName;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBClusterParameterGroupsAsync(request);
            return new DocDbDescribeDBClusterParameterGroupsResult(
                DBClusterParameterGroups: resp.DBClusterParameterGroups,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB cluster parameter groups");
        }
    }

    /// <summary>
    /// Modify a DocumentDB cluster parameter group.
    /// </summary>
    public static async Task<DocDbModifyDBClusterParameterGroupResult>
        ModifyDBClusterParameterGroupAsync(
            string dbClusterParameterGroupName,
            List<Parameter> parameters,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBClusterParameterGroupAsync(
                new ModifyDBClusterParameterGroupRequest
                {
                    DBClusterParameterGroupName = dbClusterParameterGroupName,
                    Parameters = parameters
                });
            return new DocDbModifyDBClusterParameterGroupResult(
                DBClusterParameterGroupName: resp.DBClusterParameterGroupName);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify DocumentDB cluster parameter group '{dbClusterParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe parameters in a DocumentDB cluster parameter group.
    /// </summary>
    public static async Task<DocDbDescribeDBClusterParametersResult>
        DescribeDBClusterParametersAsync(
            string dbClusterParameterGroupName,
            string? source = null,
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDBClusterParametersRequest
        {
            DBClusterParameterGroupName = dbClusterParameterGroupName
        };
        if (source != null) request.Source = source;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBClusterParametersAsync(request);
            return new DocDbDescribeDBClusterParametersResult(
                Parameters: resp.Parameters,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DocumentDB cluster parameters for '{dbClusterParameterGroupName}'");
        }
    }

    // ── Subnet group operations ─────────────────────────────────────

    /// <summary>
    /// Create a DocumentDB subnet group.
    /// </summary>
    public static async Task<DocDbCreateDBSubnetGroupResult>
        CreateDBSubnetGroupAsync(
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
            return new DocDbCreateDBSubnetGroupResult(
                DBSubnetGroupName: g.DBSubnetGroupName,
                DBSubnetGroupArn: g.DBSubnetGroupArn,
                DBSubnetGroupDescription: g.DBSubnetGroupDescription);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB subnet group");
        }
    }

    /// <summary>
    /// Delete a DocumentDB subnet group.
    /// </summary>
    public static async Task DeleteDBSubnetGroupAsync(
        string dbSubnetGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDBSubnetGroupAsync(
                new DeleteDBSubnetGroupRequest
                {
                    DBSubnetGroupName = dbSubnetGroupName
                });
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB subnet group '{dbSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB subnet groups.
    /// </summary>
    public static async Task<DocDbDescribeDBSubnetGroupsResult>
        DescribeDBSubnetGroupsAsync(
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
            return new DocDbDescribeDBSubnetGroupsResult(
                DBSubnetGroups: resp.DBSubnetGroups,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB subnet groups");
        }
    }

    // ── Event subscription operations ───────────────────────────────

    /// <summary>
    /// Create a DocumentDB event notification subscription.
    /// </summary>
    public static async Task<DocDbCreateEventSubscriptionResult>
        CreateEventSubscriptionAsync(
            CreateEventSubscriptionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEventSubscriptionAsync(request);
            var s = resp.EventSubscription;
            return new DocDbCreateEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB event subscription");
        }
    }

    /// <summary>
    /// Delete a DocumentDB event notification subscription.
    /// </summary>
    public static async Task<DocDbDeleteEventSubscriptionResult>
        DeleteEventSubscriptionAsync(
            string subscriptionName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteEventSubscriptionAsync(
                new DeleteEventSubscriptionRequest
                {
                    SubscriptionName = subscriptionName
                });
            var s = resp.EventSubscription;
            return new DocDbDeleteEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB event subscription '{subscriptionName}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB event notification subscriptions.
    /// </summary>
    public static async Task<DocDbDescribeEventSubscriptionsResult>
        DescribeEventSubscriptionsAsync(
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
            return new DocDbDescribeEventSubscriptionsResult(
                EventSubscriptionsList: resp.EventSubscriptionsList,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB event subscriptions");
        }
    }

    /// <summary>
    /// Describe DocumentDB events.
    /// </summary>
    public static async Task<DocDbDescribeEventsResult> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new DocDbDescribeEventsResult(
                Events: resp.Events,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB events");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Add tags to a DocumentDB resource.
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
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to add tags to DocumentDB resource");
        }
    }

    /// <summary>
    /// Remove tags from a DocumentDB resource.
    /// </summary>
    public static async Task RemoveTagsFromResourceAsync(
        string resourceName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromResourceAsync(
                new RemoveTagsFromResourceRequest
                {
                    ResourceName = resourceName,
                    TagKeys = tagKeys
                });
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to remove tags from DocumentDB resource");
        }
    }

    /// <summary>
    /// List tags for a DocumentDB resource.
    /// </summary>
    public static async Task<DocDbListTagsForResourceResult>
        ListTagsForResourceAsync(
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
            return new DocDbListTagsForResourceResult(TagList: resp.TagList);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for DocumentDB resource");
        }
    }

    // ── Cluster management operations ───────────────────────────────

    /// <summary>
    /// Failover a DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbFailoverDBClusterResult>
        FailoverDBClusterAsync(
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
            return new DocDbFailoverDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to failover DocumentDB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Stop a running DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbStopDBClusterResult> StopDBClusterAsync(
        string dbClusterIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopDBClusterAsync(
                new StopDBClusterRequest
                {
                    DBClusterIdentifier = dbClusterIdentifier
                });
            var c = resp.DBCluster;
            return new DocDbStopDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop DocumentDB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Start a stopped DocumentDB cluster.
    /// </summary>
    public static async Task<DocDbStartDBClusterResult> StartDBClusterAsync(
        string dbClusterIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartDBClusterAsync(
                new StartDBClusterRequest
                {
                    DBClusterIdentifier = dbClusterIdentifier
                });
            var c = resp.DBCluster;
            return new DocDbStartDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start DocumentDB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Copy a DocumentDB cluster snapshot.
    /// </summary>
    public static async Task<DocDbCopyDBClusterSnapshotResult>
        CopyDBClusterSnapshotAsync(
            string sourceDBClusterSnapshotIdentifier,
            string targetDBClusterSnapshotIdentifier,
            string? kmsKeyId = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CopyDBClusterSnapshotRequest
        {
            SourceDBClusterSnapshotIdentifier = sourceDBClusterSnapshotIdentifier,
            TargetDBClusterSnapshotIdentifier = targetDBClusterSnapshotIdentifier
        };
        if (kmsKeyId != null) request.KmsKeyId = kmsKeyId;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CopyDBClusterSnapshotAsync(request);
            var s = resp.DBClusterSnapshot;
            return new DocDbCopyDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to copy DocumentDB cluster snapshot");
        }
    }

    /// <summary>
    /// Restore a DocumentDB cluster to a point in time.
    /// </summary>
    public static async Task<DocDbRestoreDBClusterToPointInTimeResult>
        RestoreDBClusterToPointInTimeAsync(
            RestoreDBClusterToPointInTimeRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreDBClusterToPointInTimeAsync(request);
            var c = resp.DBCluster;
            return new DocDbRestoreDBClusterToPointInTimeResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore DocumentDB cluster to point in time");
        }
    }

    // ── Engine version & orderable options ──────────────────────────

    /// <summary>
    /// Describe DocumentDB engine versions.
    /// </summary>
    public static async Task<DocDbDescribeDBEngineVersionsResult>
        DescribeDBEngineVersionsAsync(
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
        if (dbParameterGroupFamily != null)
            request.DBParameterGroupFamily = dbParameterGroupFamily;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeDBEngineVersionsAsync(request);
            return new DocDbDescribeDBEngineVersionsResult(
                DBEngineVersions: resp.DBEngineVersions,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB engine versions");
        }
    }

    /// <summary>
    /// Describe orderable DocumentDB instance options.
    /// </summary>
    public static async Task<DocDbDescribeOrderableDBInstanceOptionsResult>
        DescribeOrderableDBInstanceOptionsAsync(
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
            return new DocDbDescribeOrderableDBInstanceOptionsResult(
                OrderableDBInstanceOptions: resp.OrderableDBInstanceOptions,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe orderable DocumentDB instance options");
        }
    }

    /// <summary>
    /// Describe DocumentDB certificates.
    /// </summary>
    public static async Task<DocDbDescribeCertificatesResult>
        DescribeCertificatesAsync(
            string? certificateIdentifier = null,
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeCertificatesRequest();
        if (certificateIdentifier != null)
            request.CertificateIdentifier = certificateIdentifier;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeCertificatesAsync(request);
            return new DocDbDescribeCertificatesResult(
                Certificates: resp.Certificates,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB certificates");
        }
    }

    // ── Global cluster operations ───────────────────────────────────

    /// <summary>
    /// Create a DocumentDB global cluster.
    /// </summary>
    public static async Task<DocDbCreateGlobalClusterResult>
        CreateGlobalClusterAsync(
            CreateGlobalClusterRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGlobalClusterAsync(request);
            var g = resp.GlobalCluster;
            return new DocDbCreateGlobalClusterResult(
                GlobalClusterIdentifier: g.GlobalClusterIdentifier,
                GlobalClusterArn: g.GlobalClusterArn,
                Status: g.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DocumentDB global cluster");
        }
    }

    /// <summary>
    /// Delete a DocumentDB global cluster.
    /// </summary>
    public static async Task<DocDbDeleteGlobalClusterResult>
        DeleteGlobalClusterAsync(
            string globalClusterIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGlobalClusterAsync(
                new DeleteGlobalClusterRequest
                {
                    GlobalClusterIdentifier = globalClusterIdentifier
                });
            var g = resp.GlobalCluster;
            return new DocDbDeleteGlobalClusterResult(
                GlobalClusterIdentifier: g.GlobalClusterIdentifier,
                GlobalClusterArn: g.GlobalClusterArn,
                Status: g.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DocumentDB global cluster '{globalClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DocumentDB global clusters.
    /// </summary>
    public static async Task<DocDbDescribeGlobalClustersResult>
        DescribeGlobalClustersAsync(
            string? globalClusterIdentifier = null,
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeGlobalClustersRequest();
        if (globalClusterIdentifier != null)
            request.GlobalClusterIdentifier = globalClusterIdentifier;
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeGlobalClustersAsync(request);
            return new DocDbDescribeGlobalClustersResult(
                GlobalClusters: resp.GlobalClusters,
                Marker: resp.Marker);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DocumentDB global clusters");
        }
    }

    /// <summary>
    /// Modify a DocumentDB global cluster.
    /// </summary>
    public static async Task<DocDbModifyGlobalClusterResult>
        ModifyGlobalClusterAsync(
            ModifyGlobalClusterRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyGlobalClusterAsync(request);
            var g = resp.GlobalCluster;
            return new DocDbModifyGlobalClusterResult(
                GlobalClusterIdentifier: g.GlobalClusterIdentifier,
                GlobalClusterArn: g.GlobalClusterArn,
                Status: g.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DocumentDB global cluster");
        }
    }

    /// <summary>
    /// Remove a cluster from a DocumentDB global cluster.
    /// </summary>
    public static async Task<DocDbRemoveFromGlobalClusterResult>
        RemoveFromGlobalClusterAsync(
            string globalClusterIdentifier,
            string dbClusterIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RemoveFromGlobalClusterAsync(
                new RemoveFromGlobalClusterRequest
                {
                    GlobalClusterIdentifier = globalClusterIdentifier,
                    DbClusterIdentifier = dbClusterIdentifier
                });
            var g = resp.GlobalCluster;
            return new DocDbRemoveFromGlobalClusterResult(
                GlobalClusterIdentifier: g.GlobalClusterIdentifier,
                GlobalClusterArn: g.GlobalClusterArn,
                Status: g.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove from DocumentDB global cluster '{globalClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Switchover a DocumentDB global cluster to a different region.
    /// </summary>
    public static async Task<DocDbSwitchoverGlobalClusterResult>
        SwitchoverGlobalClusterAsync(
            SwitchoverGlobalClusterRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SwitchoverGlobalClusterAsync(request);
            var g = resp.GlobalCluster;
            return new DocDbSwitchoverGlobalClusterResult(
                GlobalClusterIdentifier: g.GlobalClusterIdentifier,
                GlobalClusterArn: g.GlobalClusterArn,
                Status: g.Status);
        }
        catch (AmazonDocDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to switchover DocumentDB global cluster");
        }
    }
}
