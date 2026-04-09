using Amazon;
using Amazon.Neptune;
using Amazon.Neptune.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record NeptuneCreateDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null,
    string? Engine = null);

public sealed record NeptuneDeleteDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneDescribeDBClustersResult(
    List<DBCluster>? DBClusters = null,
    string? Marker = null);

public sealed record NeptuneModifyDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneCreateDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null,
    string? Engine = null,
    string? DBInstanceClass = null);

public sealed record NeptuneDeleteDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record NeptuneDescribeDBInstancesResult(
    List<DBInstance>? DBInstances = null,
    string? Marker = null);

public sealed record NeptuneModifyDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record NeptuneRebootDBInstanceResult(
    string? DBInstanceIdentifier = null,
    string? DBInstanceArn = null,
    string? DBInstanceStatus = null);

public sealed record NeptuneCreateDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record NeptuneDeleteDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

public sealed record NeptuneDescribeDBClusterSnapshotsResult(
    List<DBClusterSnapshot>? DBClusterSnapshots = null,
    string? Marker = null);

public sealed record NeptuneRestoreDBClusterFromSnapshotResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneCreateDBClusterParameterGroupResult(
    string? DBClusterParameterGroupName = null,
    string? DBClusterParameterGroupArn = null,
    string? DBParameterGroupFamily = null);

public sealed record NeptuneDescribeDBClusterParameterGroupsResult(
    List<DBClusterParameterGroup>? DBClusterParameterGroups = null,
    string? Marker = null);

public sealed record NeptuneModifyDBClusterParameterGroupResult(
    string? DBClusterParameterGroupName = null);

public sealed record NeptuneDescribeDBClusterParametersResult(
    List<Parameter>? Parameters = null,
    string? Marker = null);

public sealed record NeptuneCreateDBSubnetGroupResult(
    string? DBSubnetGroupName = null,
    string? DBSubnetGroupArn = null,
    string? DBSubnetGroupDescription = null);

public sealed record NeptuneDescribeDBSubnetGroupsResult(
    List<DBSubnetGroup>? DBSubnetGroups = null,
    string? Marker = null);

public sealed record NeptuneCreateEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record NeptuneDeleteEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record NeptuneDescribeEventSubscriptionsResult(
    List<EventSubscription>? EventSubscriptionsList = null,
    string? Marker = null);

public sealed record NeptuneDescribeEventsResult(
    List<Event>? Events = null,
    string? Marker = null);

public sealed record NeptuneListTagsForResourceResult(
    List<Tag>? TagList = null);

public sealed record NeptuneFailoverDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneDescribeDBEngineVersionsResult(
    List<DBEngineVersion>? DBEngineVersions = null,
    string? Marker = null);

public sealed record NeptuneDescribeOrderableDBInstanceOptionsResult(
    List<OrderableDBInstanceOption>? OrderableDBInstanceOptions = null,
    string? Marker = null);

public sealed record NeptuneStartDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneStopDBClusterResult(
    string? DBClusterIdentifier = null,
    string? DBClusterArn = null,
    string? Status = null);

public sealed record NeptuneCopyDBClusterSnapshotResult(
    string? DBClusterSnapshotIdentifier = null,
    string? DBClusterSnapshotArn = null,
    string? Status = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Neptune.
/// </summary>
public static class NeptuneService
{
    private static AmazonNeptuneClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonNeptuneClient>(region);

    // ── Cluster operations ──────────────────────────────────────────

    /// <summary>
    /// Create a new Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneCreateDBClusterResult> CreateDBClusterAsync(
        CreateDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBClusterAsync(request);
            var c = resp.DBCluster;
            return new NeptuneCreateDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status,
                Engine: c.Engine);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune DB cluster");
        }
    }

    /// <summary>
    /// Delete a Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneDeleteDBClusterResult> DeleteDBClusterAsync(
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
            return new NeptuneDeleteDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB clusters.
    /// </summary>
    public static async Task<NeptuneDescribeDBClustersResult> DescribeDBClustersAsync(
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
            return new NeptuneDescribeDBClustersResult(
                DBClusters: resp.DBClusters,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB clusters");
        }
    }

    /// <summary>
    /// Modify a Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneModifyDBClusterResult> ModifyDBClusterAsync(
        ModifyDBClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBClusterAsync(request);
            var c = resp.DBCluster;
            return new NeptuneModifyDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify Neptune DB cluster");
        }
    }

    // ── Instance operations ─────────────────────────────────────────

    /// <summary>
    /// Create a new Neptune DB instance.
    /// </summary>
    public static async Task<NeptuneCreateDBInstanceResult> CreateDBInstanceAsync(
        CreateDBInstanceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new NeptuneCreateDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus,
                Engine: db.Engine,
                DBInstanceClass: db.DBInstanceClass);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune DB instance");
        }
    }

    /// <summary>
    /// Delete a Neptune DB instance.
    /// </summary>
    public static async Task<NeptuneDeleteDBInstanceResult> DeleteDBInstanceAsync(
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
            return new NeptuneDeleteDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune DB instance '{dbInstanceIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB instances.
    /// </summary>
    public static async Task<NeptuneDescribeDBInstancesResult> DescribeDBInstancesAsync(
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
            return new NeptuneDescribeDBInstancesResult(
                DBInstances: resp.DBInstances,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB instances");
        }
    }

    /// <summary>
    /// Modify a Neptune DB instance.
    /// </summary>
    public static async Task<NeptuneModifyDBInstanceResult> ModifyDBInstanceAsync(
        ModifyDBInstanceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyDBInstanceAsync(request);
            var db = resp.DBInstance;
            return new NeptuneModifyDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify Neptune DB instance");
        }
    }

    /// <summary>
    /// Reboot a Neptune DB instance.
    /// </summary>
    public static async Task<NeptuneRebootDBInstanceResult> RebootDBInstanceAsync(
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
            return new NeptuneRebootDBInstanceResult(
                DBInstanceIdentifier: db.DBInstanceIdentifier,
                DBInstanceArn: db.DBInstanceArn,
                DBInstanceStatus: db.DBInstanceStatus);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot Neptune DB instance '{dbInstanceIdentifier}'");
        }
    }

    // ── Cluster snapshot operations ─────────────────────────────────

    /// <summary>
    /// Create a Neptune DB cluster snapshot.
    /// </summary>
    public static async Task<NeptuneCreateDBClusterSnapshotResult>
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
            return new NeptuneCreateDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune DB cluster snapshot");
        }
    }

    /// <summary>
    /// Delete a Neptune DB cluster snapshot.
    /// </summary>
    public static async Task<NeptuneDeleteDBClusterSnapshotResult>
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
            return new NeptuneDeleteDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune DB cluster snapshot '{dbClusterSnapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB cluster snapshots.
    /// </summary>
    public static async Task<NeptuneDescribeDBClusterSnapshotsResult>
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
            return new NeptuneDescribeDBClusterSnapshotsResult(
                DBClusterSnapshots: resp.DBClusterSnapshots,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB cluster snapshots");
        }
    }

    /// <summary>
    /// Restore a Neptune DB cluster from a snapshot.
    /// </summary>
    public static async Task<NeptuneRestoreDBClusterFromSnapshotResult>
        RestoreDBClusterFromSnapshotAsync(
            RestoreDBClusterFromSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreDBClusterFromSnapshotAsync(request);
            var c = resp.DBCluster;
            return new NeptuneRestoreDBClusterFromSnapshotResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore Neptune DB cluster from snapshot");
        }
    }

    // ── Cluster parameter group operations ──────────────────────────

    /// <summary>
    /// Create a Neptune DB cluster parameter group.
    /// </summary>
    public static async Task<NeptuneCreateDBClusterParameterGroupResult>
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
            return new NeptuneCreateDBClusterParameterGroupResult(
                DBClusterParameterGroupName: g.DBClusterParameterGroupName,
                DBClusterParameterGroupArn: g.DBClusterParameterGroupArn,
                DBParameterGroupFamily: g.DBParameterGroupFamily);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune DB cluster parameter group");
        }
    }

    /// <summary>
    /// Delete a Neptune DB cluster parameter group.
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
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune DB cluster parameter group '{dbClusterParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB cluster parameter groups.
    /// </summary>
    public static async Task<NeptuneDescribeDBClusterParameterGroupsResult>
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
            return new NeptuneDescribeDBClusterParameterGroupsResult(
                DBClusterParameterGroups: resp.DBClusterParameterGroups,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB cluster parameter groups");
        }
    }

    /// <summary>
    /// Modify a Neptune DB cluster parameter group.
    /// </summary>
    public static async Task<NeptuneModifyDBClusterParameterGroupResult>
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
            return new NeptuneModifyDBClusterParameterGroupResult(
                DBClusterParameterGroupName: resp.DBClusterParameterGroupName);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify Neptune DB cluster parameter group '{dbClusterParameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe parameters in a Neptune DB cluster parameter group.
    /// </summary>
    public static async Task<NeptuneDescribeDBClusterParametersResult>
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
            return new NeptuneDescribeDBClusterParametersResult(
                Parameters: resp.Parameters,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Neptune DB cluster parameters for '{dbClusterParameterGroupName}'");
        }
    }

    // ── Subnet group operations ─────────────────────────────────────

    /// <summary>
    /// Create a Neptune DB subnet group.
    /// </summary>
    public static async Task<NeptuneCreateDBSubnetGroupResult>
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
            return new NeptuneCreateDBSubnetGroupResult(
                DBSubnetGroupName: g.DBSubnetGroupName,
                DBSubnetGroupArn: g.DBSubnetGroupArn,
                DBSubnetGroupDescription: g.DBSubnetGroupDescription);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune DB subnet group");
        }
    }

    /// <summary>
    /// Delete a Neptune DB subnet group.
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
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune DB subnet group '{dbSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB subnet groups.
    /// </summary>
    public static async Task<NeptuneDescribeDBSubnetGroupsResult>
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
            return new NeptuneDescribeDBSubnetGroupsResult(
                DBSubnetGroups: resp.DBSubnetGroups,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB subnet groups");
        }
    }

    // ── Event subscription operations ───────────────────────────────

    /// <summary>
    /// Create a Neptune event notification subscription.
    /// </summary>
    public static async Task<NeptuneCreateEventSubscriptionResult>
        CreateEventSubscriptionAsync(
            CreateEventSubscriptionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEventSubscriptionAsync(request);
            var s = resp.EventSubscription;
            return new NeptuneCreateEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Neptune event subscription");
        }
    }

    /// <summary>
    /// Delete a Neptune event notification subscription.
    /// </summary>
    public static async Task<NeptuneDeleteEventSubscriptionResult>
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
            return new NeptuneDeleteEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: s.EventSubscriptionArn,
                Status: s.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Neptune event subscription '{subscriptionName}'");
        }
    }

    /// <summary>
    /// Describe Neptune event notification subscriptions.
    /// </summary>
    public static async Task<NeptuneDescribeEventSubscriptionsResult>
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
            return new NeptuneDescribeEventSubscriptionsResult(
                EventSubscriptionsList: resp.EventSubscriptionsList,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune event subscriptions");
        }
    }

    /// <summary>
    /// Describe Neptune events.
    /// </summary>
    public static async Task<NeptuneDescribeEventsResult> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new NeptuneDescribeEventsResult(
                Events: resp.Events,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune events");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Add tags to a Neptune resource.
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
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to add tags to Neptune resource");
        }
    }

    /// <summary>
    /// Remove tags from a Neptune resource.
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
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to remove tags from Neptune resource");
        }
    }

    /// <summary>
    /// List tags for a Neptune resource.
    /// </summary>
    public static async Task<NeptuneListTagsForResourceResult>
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
            return new NeptuneListTagsForResourceResult(TagList: resp.TagList);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for Neptune resource");
        }
    }

    // ── Cluster management operations ───────────────────────────────

    /// <summary>
    /// Failover a Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneFailoverDBClusterResult>
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
            return new NeptuneFailoverDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to failover Neptune DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Neptune DB engine versions.
    /// </summary>
    public static async Task<NeptuneDescribeDBEngineVersionsResult>
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
            return new NeptuneDescribeDBEngineVersionsResult(
                DBEngineVersions: resp.DBEngineVersions,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Neptune DB engine versions");
        }
    }

    /// <summary>
    /// Describe orderable Neptune DB instance options.
    /// </summary>
    public static async Task<NeptuneDescribeOrderableDBInstanceOptionsResult>
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
            return new NeptuneDescribeOrderableDBInstanceOptionsResult(
                OrderableDBInstanceOptions: resp.OrderableDBInstanceOptions,
                Marker: resp.Marker);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe orderable Neptune DB instance options");
        }
    }

    /// <summary>
    /// Start a stopped Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneStartDBClusterResult> StartDBClusterAsync(
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
            return new NeptuneStartDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start Neptune DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Stop a running Neptune DB cluster.
    /// </summary>
    public static async Task<NeptuneStopDBClusterResult> StopDBClusterAsync(
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
            return new NeptuneStopDBClusterResult(
                DBClusterIdentifier: c.DBClusterIdentifier,
                DBClusterArn: c.DBClusterArn,
                Status: c.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Neptune DB cluster '{dbClusterIdentifier}'");
        }
    }

    /// <summary>
    /// Copy a Neptune DB cluster snapshot.
    /// </summary>
    public static async Task<NeptuneCopyDBClusterSnapshotResult>
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
            return new NeptuneCopyDBClusterSnapshotResult(
                DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
                DBClusterSnapshotArn: s.DBClusterSnapshotArn,
                Status: s.Status);
        }
        catch (AmazonNeptuneException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to copy Neptune DB cluster snapshot");
        }
    }
}
