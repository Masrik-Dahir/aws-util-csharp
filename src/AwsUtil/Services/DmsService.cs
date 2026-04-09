using Amazon;
using Amazon.DatabaseMigrationService;
using Amazon.DatabaseMigrationService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record DmsCreateReplicationInstanceResult(
    string? ReplicationInstanceIdentifier = null,
    string? ReplicationInstanceArn = null,
    string? ReplicationInstanceStatus = null,
    string? ReplicationInstanceClass = null);

public sealed record DmsDeleteReplicationInstanceResult(
    string? ReplicationInstanceIdentifier = null,
    string? ReplicationInstanceArn = null,
    string? ReplicationInstanceStatus = null);

public sealed record DmsDescribeReplicationInstancesResult(
    List<ReplicationInstance>? ReplicationInstances = null,
    string? Marker = null);

public sealed record DmsModifyReplicationInstanceResult(
    string? ReplicationInstanceIdentifier = null,
    string? ReplicationInstanceArn = null,
    string? ReplicationInstanceStatus = null);

public sealed record DmsCreateEndpointResult(
    string? EndpointIdentifier = null,
    string? EndpointArn = null,
    string? EndpointType = null,
    string? EngineName = null,
    string? Status = null);

public sealed record DmsDeleteEndpointResult(
    string? EndpointIdentifier = null,
    string? EndpointArn = null,
    string? Status = null);

public sealed record DmsDescribeEndpointsResult(
    List<Endpoint>? Endpoints = null,
    string? Marker = null);

public sealed record DmsModifyEndpointResult(
    string? EndpointIdentifier = null,
    string? EndpointArn = null,
    string? Status = null);

public sealed record DmsTestConnectionResult(
    string? ReplicationInstanceArn = null,
    string? EndpointArn = null,
    string? Status = null);

public sealed record DmsDescribeConnectionsResult(
    List<Connection>? Connections = null,
    string? Marker = null);

public sealed record DmsCreateReplicationTaskResult(
    string? ReplicationTaskIdentifier = null,
    string? ReplicationTaskArn = null,
    string? Status = null,
    string? MigrationType = null);

public sealed record DmsDeleteReplicationTaskResult(
    string? ReplicationTaskIdentifier = null,
    string? ReplicationTaskArn = null,
    string? Status = null);

public sealed record DmsDescribeReplicationTasksResult(
    List<ReplicationTask>? ReplicationTasks = null,
    string? Marker = null);

public sealed record DmsModifyReplicationTaskResult(
    string? ReplicationTaskIdentifier = null,
    string? ReplicationTaskArn = null,
    string? Status = null);

public sealed record DmsStartReplicationTaskResult(
    string? ReplicationTaskIdentifier = null,
    string? ReplicationTaskArn = null,
    string? Status = null);

public sealed record DmsStopReplicationTaskResult(
    string? ReplicationTaskIdentifier = null,
    string? ReplicationTaskArn = null,
    string? Status = null);

public sealed record DmsCreateReplicationSubnetGroupResult(
    string? ReplicationSubnetGroupIdentifier = null,
    string? ReplicationSubnetGroupDescription = null);

public sealed record DmsDescribeReplicationSubnetGroupsResult(
    List<ReplicationSubnetGroup>? ReplicationSubnetGroups = null,
    string? Marker = null);

public sealed record DmsModifyReplicationSubnetGroupResult(
    string? ReplicationSubnetGroupIdentifier = null,
    string? ReplicationSubnetGroupDescription = null);

public sealed record DmsDescribeTableStatisticsResult(
    List<TableStatistics>? TableStatistics = null,
    string? ReplicationTaskArn = null,
    string? Marker = null);

public sealed record DmsDescribeEndpointTypesResult(
    List<SupportedEndpointType>? SupportedEndpointTypes = null,
    string? Marker = null);

public sealed record DmsDescribeOrderableReplicationInstancesResult(
    List<OrderableReplicationInstance>? OrderableReplicationInstances = null,
    string? Marker = null);

public sealed record DmsCreateEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DmsDeleteEventSubscriptionResult(
    string? CustSubscriptionId = null,
    string? EventSubscriptionArn = null,
    string? Status = null);

public sealed record DmsDescribeEventSubscriptionsResult(
    List<EventSubscription>? EventSubscriptionsList = null,
    string? Marker = null);

public sealed record DmsDescribeEventsResult(
    List<DMSEvent>? Events = null,
    string? Marker = null);

public sealed record DmsListTagsForResourceResult(
    List<Tag>? TagList = null);

public sealed record DmsDescribeSchemasResult(
    List<string>? Schemas = null,
    string? Marker = null);

public sealed record DmsDescribeReplicationTaskAssessmentRunsResult(
    List<ReplicationTaskAssessmentRun>? ReplicationTaskAssessmentRuns = null,
    string? Marker = null);

public sealed record DmsStartReplicationTaskAssessmentRunResult(
    string? ReplicationTaskAssessmentRunArn = null,
    string? ReplicationTaskArn = null,
    string? Status = null);

public sealed record DmsCancelReplicationTaskAssessmentRunResult(
    string? ReplicationTaskAssessmentRunArn = null,
    string? Status = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Database Migration Service (DMS).
/// </summary>
public static class DmsService
{
    private static AmazonDatabaseMigrationServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonDatabaseMigrationServiceClient>(region);

    // ── Replication instance operations ─────────────────────────────

    /// <summary>
    /// Create a DMS replication instance.
    /// </summary>
    public static async Task<DmsCreateReplicationInstanceResult>
        CreateReplicationInstanceAsync(
            CreateReplicationInstanceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateReplicationInstanceAsync(request);
            var ri = resp.ReplicationInstance;
            return new DmsCreateReplicationInstanceResult(
                ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
                ReplicationInstanceArn: ri.ReplicationInstanceArn,
                ReplicationInstanceStatus: ri.ReplicationInstanceStatus,
                ReplicationInstanceClass: ri.ReplicationInstanceClass);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DMS replication instance");
        }
    }

    /// <summary>
    /// Delete a DMS replication instance.
    /// </summary>
    public static async Task<DmsDeleteReplicationInstanceResult>
        DeleteReplicationInstanceAsync(
            string replicationInstanceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteReplicationInstanceAsync(
                new DeleteReplicationInstanceRequest
                {
                    ReplicationInstanceArn = replicationInstanceArn
                });
            var ri = resp.ReplicationInstance;
            return new DmsDeleteReplicationInstanceResult(
                ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
                ReplicationInstanceArn: ri.ReplicationInstanceArn,
                ReplicationInstanceStatus: ri.ReplicationInstanceStatus);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DMS replication instance '{replicationInstanceArn}'");
        }
    }

    /// <summary>
    /// Describe DMS replication instances.
    /// </summary>
    public static async Task<DmsDescribeReplicationInstancesResult>
        DescribeReplicationInstancesAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationInstancesRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeReplicationInstancesAsync(request);
            return new DmsDescribeReplicationInstancesResult(
                ReplicationInstances: resp.ReplicationInstances,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS replication instances");
        }
    }

    /// <summary>
    /// Modify a DMS replication instance.
    /// </summary>
    public static async Task<DmsModifyReplicationInstanceResult>
        ModifyReplicationInstanceAsync(
            ModifyReplicationInstanceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyReplicationInstanceAsync(request);
            var ri = resp.ReplicationInstance;
            return new DmsModifyReplicationInstanceResult(
                ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
                ReplicationInstanceArn: ri.ReplicationInstanceArn,
                ReplicationInstanceStatus: ri.ReplicationInstanceStatus);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DMS replication instance");
        }
    }

    // ── Endpoint operations ─────────────────────────────────────────

    /// <summary>
    /// Create a DMS endpoint.
    /// </summary>
    public static async Task<DmsCreateEndpointResult> CreateEndpointAsync(
        CreateEndpointRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEndpointAsync(request);
            var ep = resp.Endpoint;
            return new DmsCreateEndpointResult(
                EndpointIdentifier: ep.EndpointIdentifier,
                EndpointArn: ep.EndpointArn,
                EndpointType: ep.EndpointType?.Value,
                EngineName: ep.EngineName,
                Status: ep.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DMS endpoint");
        }
    }

    /// <summary>
    /// Delete a DMS endpoint.
    /// </summary>
    public static async Task<DmsDeleteEndpointResult> DeleteEndpointAsync(
        string endpointArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteEndpointAsync(
                new DeleteEndpointRequest
                {
                    EndpointArn = endpointArn
                });
            var ep = resp.Endpoint;
            return new DmsDeleteEndpointResult(
                EndpointIdentifier: ep.EndpointIdentifier,
                EndpointArn: ep.EndpointArn,
                Status: ep.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DMS endpoint '{endpointArn}'");
        }
    }

    /// <summary>
    /// Describe DMS endpoints.
    /// </summary>
    public static async Task<DmsDescribeEndpointsResult>
        DescribeEndpointsAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEndpointsRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeEndpointsAsync(request);
            return new DmsDescribeEndpointsResult(
                Endpoints: resp.Endpoints,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS endpoints");
        }
    }

    /// <summary>
    /// Modify a DMS endpoint.
    /// </summary>
    public static async Task<DmsModifyEndpointResult> ModifyEndpointAsync(
        ModifyEndpointRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyEndpointAsync(request);
            var ep = resp.Endpoint;
            return new DmsModifyEndpointResult(
                EndpointIdentifier: ep.EndpointIdentifier,
                EndpointArn: ep.EndpointArn,
                Status: ep.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DMS endpoint");
        }
    }

    // ── Connection operations ───────────────────────────────────────

    /// <summary>
    /// Test a DMS connection between a replication instance and an endpoint.
    /// </summary>
    public static async Task<DmsTestConnectionResult> TestConnectionAsync(
        string replicationInstanceArn,
        string endpointArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TestConnectionAsync(
                new TestConnectionRequest
                {
                    ReplicationInstanceArn = replicationInstanceArn,
                    EndpointArn = endpointArn
                });
            var c = resp.Connection;
            return new DmsTestConnectionResult(
                ReplicationInstanceArn: c.ReplicationInstanceArn,
                EndpointArn: c.EndpointArn,
                Status: c.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to test DMS connection");
        }
    }

    /// <summary>
    /// Describe DMS connections.
    /// </summary>
    public static async Task<DmsDescribeConnectionsResult>
        DescribeConnectionsAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConnectionsRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeConnectionsAsync(request);
            return new DmsDescribeConnectionsResult(
                Connections: resp.Connections,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS connections");
        }
    }

    // ── Replication task operations ─────────────────────────────────

    /// <summary>
    /// Create a DMS replication task.
    /// </summary>
    public static async Task<DmsCreateReplicationTaskResult>
        CreateReplicationTaskAsync(
            CreateReplicationTaskRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateReplicationTaskAsync(request);
            var t = resp.ReplicationTask;
            return new DmsCreateReplicationTaskResult(
                ReplicationTaskIdentifier: t.ReplicationTaskIdentifier,
                ReplicationTaskArn: t.ReplicationTaskArn,
                Status: t.Status,
                MigrationType: t.MigrationType?.Value);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DMS replication task");
        }
    }

    /// <summary>
    /// Delete a DMS replication task.
    /// </summary>
    public static async Task<DmsDeleteReplicationTaskResult>
        DeleteReplicationTaskAsync(
            string replicationTaskArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteReplicationTaskAsync(
                new DeleteReplicationTaskRequest
                {
                    ReplicationTaskArn = replicationTaskArn
                });
            var t = resp.ReplicationTask;
            return new DmsDeleteReplicationTaskResult(
                ReplicationTaskIdentifier: t.ReplicationTaskIdentifier,
                ReplicationTaskArn: t.ReplicationTaskArn,
                Status: t.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DMS replication task '{replicationTaskArn}'");
        }
    }

    /// <summary>
    /// Describe DMS replication tasks.
    /// </summary>
    public static async Task<DmsDescribeReplicationTasksResult>
        DescribeReplicationTasksAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            bool? withoutSettings = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationTasksRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (withoutSettings.HasValue) request.WithoutSettings = withoutSettings.Value;

        try
        {
            var resp = await client.DescribeReplicationTasksAsync(request);
            return new DmsDescribeReplicationTasksResult(
                ReplicationTasks: resp.ReplicationTasks,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS replication tasks");
        }
    }

    /// <summary>
    /// Modify a DMS replication task.
    /// </summary>
    public static async Task<DmsModifyReplicationTaskResult>
        ModifyReplicationTaskAsync(
            ModifyReplicationTaskRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyReplicationTaskAsync(request);
            var t = resp.ReplicationTask;
            return new DmsModifyReplicationTaskResult(
                ReplicationTaskIdentifier: t.ReplicationTaskIdentifier,
                ReplicationTaskArn: t.ReplicationTaskArn,
                Status: t.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify DMS replication task");
        }
    }

    /// <summary>
    /// Start a DMS replication task.
    /// </summary>
    public static async Task<DmsStartReplicationTaskResult>
        StartReplicationTaskAsync(
            string replicationTaskArn,
            string startReplicationTaskType,
            string? cdcStartTime = null,
            string? cdcStartPosition = null,
            string? cdcStopPosition = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartReplicationTaskRequest
        {
            ReplicationTaskArn = replicationTaskArn,
            StartReplicationTaskType = startReplicationTaskType
        };
        if (cdcStartTime != null)
            request.CdcStartTime = DateTime.Parse(cdcStartTime);
        if (cdcStartPosition != null) request.CdcStartPosition = cdcStartPosition;
        if (cdcStopPosition != null) request.CdcStopPosition = cdcStopPosition;

        try
        {
            var resp = await client.StartReplicationTaskAsync(request);
            var t = resp.ReplicationTask;
            return new DmsStartReplicationTaskResult(
                ReplicationTaskIdentifier: t.ReplicationTaskIdentifier,
                ReplicationTaskArn: t.ReplicationTaskArn,
                Status: t.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start DMS replication task '{replicationTaskArn}'");
        }
    }

    /// <summary>
    /// Stop a DMS replication task.
    /// </summary>
    public static async Task<DmsStopReplicationTaskResult>
        StopReplicationTaskAsync(
            string replicationTaskArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopReplicationTaskAsync(
                new StopReplicationTaskRequest
                {
                    ReplicationTaskArn = replicationTaskArn
                });
            var t = resp.ReplicationTask;
            return new DmsStopReplicationTaskResult(
                ReplicationTaskIdentifier: t.ReplicationTaskIdentifier,
                ReplicationTaskArn: t.ReplicationTaskArn,
                Status: t.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop DMS replication task '{replicationTaskArn}'");
        }
    }

    // ── Replication subnet group operations ─────────────────────────

    /// <summary>
    /// Create a DMS replication subnet group.
    /// </summary>
    public static async Task<DmsCreateReplicationSubnetGroupResult>
        CreateReplicationSubnetGroupAsync(
            string replicationSubnetGroupIdentifier,
            string replicationSubnetGroupDescription,
            List<string> subnetIds,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateReplicationSubnetGroupRequest
        {
            ReplicationSubnetGroupIdentifier = replicationSubnetGroupIdentifier,
            ReplicationSubnetGroupDescription = replicationSubnetGroupDescription,
            SubnetIds = subnetIds
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateReplicationSubnetGroupAsync(request);
            var g = resp.ReplicationSubnetGroup;
            return new DmsCreateReplicationSubnetGroupResult(
                ReplicationSubnetGroupIdentifier: g.ReplicationSubnetGroupIdentifier,
                ReplicationSubnetGroupDescription: g.ReplicationSubnetGroupDescription);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DMS replication subnet group");
        }
    }

    /// <summary>
    /// Delete a DMS replication subnet group.
    /// </summary>
    public static async Task DeleteReplicationSubnetGroupAsync(
        string replicationSubnetGroupIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteReplicationSubnetGroupAsync(
                new DeleteReplicationSubnetGroupRequest
                {
                    ReplicationSubnetGroupIdentifier = replicationSubnetGroupIdentifier
                });
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DMS replication subnet group '{replicationSubnetGroupIdentifier}'");
        }
    }

    /// <summary>
    /// Describe DMS replication subnet groups.
    /// </summary>
    public static async Task<DmsDescribeReplicationSubnetGroupsResult>
        DescribeReplicationSubnetGroupsAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationSubnetGroupsRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeReplicationSubnetGroupsAsync(request);
            return new DmsDescribeReplicationSubnetGroupsResult(
                ReplicationSubnetGroups: resp.ReplicationSubnetGroups,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS replication subnet groups");
        }
    }

    /// <summary>
    /// Modify a DMS replication subnet group.
    /// </summary>
    public static async Task<DmsModifyReplicationSubnetGroupResult>
        ModifyReplicationSubnetGroupAsync(
            string replicationSubnetGroupIdentifier,
            List<string> subnetIds,
            string? replicationSubnetGroupDescription = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ModifyReplicationSubnetGroupRequest
        {
            ReplicationSubnetGroupIdentifier = replicationSubnetGroupIdentifier,
            SubnetIds = subnetIds
        };
        if (replicationSubnetGroupDescription != null)
            request.ReplicationSubnetGroupDescription = replicationSubnetGroupDescription;

        try
        {
            var resp = await client.ModifyReplicationSubnetGroupAsync(request);
            var g = resp.ReplicationSubnetGroup;
            return new DmsModifyReplicationSubnetGroupResult(
                ReplicationSubnetGroupIdentifier: g.ReplicationSubnetGroupIdentifier,
                ReplicationSubnetGroupDescription: g.ReplicationSubnetGroupDescription);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify DMS replication subnet group '{replicationSubnetGroupIdentifier}'");
        }
    }

    // ── Table & schema operations ───────────────────────────────────

    /// <summary>
    /// Describe table statistics for a DMS replication task.
    /// </summary>
    public static async Task<DmsDescribeTableStatisticsResult>
        DescribeTableStatisticsAsync(
            string replicationTaskArn,
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTableStatisticsRequest
        {
            ReplicationTaskArn = replicationTaskArn
        };
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeTableStatisticsAsync(request);
            return new DmsDescribeTableStatisticsResult(
                TableStatistics: resp.TableStatistics,
                ReplicationTaskArn: resp.ReplicationTaskArn,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DMS table statistics for '{replicationTaskArn}'");
        }
    }

    /// <summary>
    /// Describe DMS endpoint types.
    /// </summary>
    public static async Task<DmsDescribeEndpointTypesResult>
        DescribeEndpointTypesAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEndpointTypesRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeEndpointTypesAsync(request);
            return new DmsDescribeEndpointTypesResult(
                SupportedEndpointTypes: resp.SupportedEndpointTypes,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS endpoint types");
        }
    }

    /// <summary>
    /// Describe orderable DMS replication instances.
    /// </summary>
    public static async Task<DmsDescribeOrderableReplicationInstancesResult>
        DescribeOrderableReplicationInstancesAsync(
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeOrderableReplicationInstancesRequest();
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeOrderableReplicationInstancesAsync(request);
            return new DmsDescribeOrderableReplicationInstancesResult(
                OrderableReplicationInstances: resp.OrderableReplicationInstances,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe orderable DMS replication instances");
        }
    }

    // ── Event subscription operations ───────────────────────────────

    /// <summary>
    /// Create a DMS event notification subscription.
    /// </summary>
    public static async Task<DmsCreateEventSubscriptionResult>
        CreateEventSubscriptionAsync(
            CreateEventSubscriptionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEventSubscriptionAsync(request);
            var s = resp.EventSubscription;
            return new DmsCreateEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: null,
                Status: s.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create DMS event subscription");
        }
    }

    /// <summary>
    /// Delete a DMS event notification subscription.
    /// </summary>
    public static async Task<DmsDeleteEventSubscriptionResult>
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
            return new DmsDeleteEventSubscriptionResult(
                CustSubscriptionId: s.CustSubscriptionId,
                EventSubscriptionArn: null,
                Status: s.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DMS event subscription '{subscriptionName}'");
        }
    }

    /// <summary>
    /// Describe DMS event notification subscriptions.
    /// </summary>
    public static async Task<DmsDescribeEventSubscriptionsResult>
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
            return new DmsDescribeEventSubscriptionsResult(
                EventSubscriptionsList: resp.EventSubscriptionsList,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS event subscriptions");
        }
    }

    /// <summary>
    /// Describe DMS events.
    /// </summary>
    public static async Task<DmsDescribeEventsResult> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new DmsDescribeEventsResult(
                Events: resp.Events,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS events");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Add tags to a DMS resource.
    /// </summary>
    public static async Task AddTagsToResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToResourceAsync(new AddTagsToResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to add tags to DMS resource");
        }
    }

    /// <summary>
    /// Remove tags from a DMS resource.
    /// </summary>
    public static async Task RemoveTagsFromResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromResourceAsync(
                new RemoveTagsFromResourceRequest
                {
                    ResourceArn = resourceArn,
                    TagKeys = tagKeys
                });
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to remove tags from DMS resource");
        }
    }

    /// <summary>
    /// List tags for a DMS resource.
    /// </summary>
    public static async Task<DmsListTagsForResourceResult>
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
            return new DmsListTagsForResourceResult(TagList: resp.TagList);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for DMS resource");
        }
    }

    // ── Reload & refresh operations ─────────────────────────────────

    /// <summary>
    /// Reload tables for a DMS replication task.
    /// </summary>
    public static async Task ReloadTablesAsync(
        string replicationTaskArn,
        List<TableToReload> tablesToReload,
        string? reloadOption = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ReloadTablesRequest
        {
            ReplicationTaskArn = replicationTaskArn,
            TablesToReload = tablesToReload
        };
        if (reloadOption != null) request.ReloadOption = reloadOption;

        try
        {
            await client.ReloadTablesAsync(request);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reload DMS tables for '{replicationTaskArn}'");
        }
    }

    /// <summary>
    /// Refresh schemas for a DMS endpoint.
    /// </summary>
    public static async Task RefreshSchemasAsync(
        string endpointArn,
        string replicationInstanceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RefreshSchemasAsync(new RefreshSchemasRequest
            {
                EndpointArn = endpointArn,
                ReplicationInstanceArn = replicationInstanceArn
            });
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to refresh DMS schemas for endpoint '{endpointArn}'");
        }
    }

    /// <summary>
    /// Describe schemas for a DMS endpoint.
    /// </summary>
    public static async Task<DmsDescribeSchemasResult> DescribeSchemasAsync(
        string endpointArn,
        string? marker = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSchemasRequest
        {
            EndpointArn = endpointArn
        };
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeSchemasAsync(request);
            return new DmsDescribeSchemasResult(
                Schemas: resp.Schemas,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DMS schemas for endpoint '{endpointArn}'");
        }
    }

    // ── Assessment run operations ───────────────────────────────────

    /// <summary>
    /// Describe DMS replication task assessment runs.
    /// </summary>
    public static async Task<DmsDescribeReplicationTaskAssessmentRunsResult>
        DescribeReplicationTaskAssessmentRunsAsync(
            List<Filter>? filters = null,
            string? marker = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationTaskAssessmentRunsRequest();
        if (filters != null) request.Filters = filters;
        if (marker != null) request.Marker = marker;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeReplicationTaskAssessmentRunsAsync(request);
            return new DmsDescribeReplicationTaskAssessmentRunsResult(
                ReplicationTaskAssessmentRuns: resp.ReplicationTaskAssessmentRuns,
                Marker: resp.Marker);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe DMS replication task assessment runs");
        }
    }

    /// <summary>
    /// Start a DMS replication task assessment run.
    /// </summary>
    public static async Task<DmsStartReplicationTaskAssessmentRunResult>
        StartReplicationTaskAssessmentRunAsync(
            StartReplicationTaskAssessmentRunRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartReplicationTaskAssessmentRunAsync(request);
            var r = resp.ReplicationTaskAssessmentRun;
            return new DmsStartReplicationTaskAssessmentRunResult(
                ReplicationTaskAssessmentRunArn: r.ReplicationTaskAssessmentRunArn,
                ReplicationTaskArn: r.ReplicationTaskArn,
                Status: r.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start DMS replication task assessment run");
        }
    }

    /// <summary>
    /// Cancel a DMS replication task assessment run.
    /// </summary>
    public static async Task<DmsCancelReplicationTaskAssessmentRunResult>
        CancelReplicationTaskAssessmentRunAsync(
            string replicationTaskAssessmentRunArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelReplicationTaskAssessmentRunAsync(
                new CancelReplicationTaskAssessmentRunRequest
                {
                    ReplicationTaskAssessmentRunArn = replicationTaskAssessmentRunArn
                });
            var r = resp.ReplicationTaskAssessmentRun;
            return new DmsCancelReplicationTaskAssessmentRunResult(
                ReplicationTaskAssessmentRunArn: r.ReplicationTaskAssessmentRunArn,
                Status: r.Status);
        }
        catch (AmazonDatabaseMigrationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel DMS replication task assessment run '{replicationTaskAssessmentRunArn}'");
        }
    }
}
