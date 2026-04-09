using Amazon;
using Amazon.Redshift;
using Amazon.Redshift.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null,
    string? ClusterArn = null);

public sealed record DeleteRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record DescribeRedshiftClustersResult(
    List<Cluster>? Clusters = null,
    string? Marker = null);

public sealed record ModifyRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record RebootRedshiftClusterResult(
    string? ClusterIdentifier = null);

public sealed record PauseRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record ResumeRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record ResizeRedshiftClusterResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record CreateRedshiftSnapshotResult(
    string? SnapshotIdentifier = null,
    string? ClusterIdentifier = null,
    string? Status = null);

public sealed record DeleteRedshiftSnapshotResult(
    string? SnapshotIdentifier = null);

public sealed record DescribeRedshiftSnapshotsResult(
    List<Snapshot>? Snapshots = null,
    string? Marker = null);

public sealed record RestoreFromRedshiftSnapshotResult(
    string? ClusterIdentifier = null,
    string? ClusterStatus = null);

public sealed record CreateRedshiftSubnetGroupResult(
    string? ClusterSubnetGroupName = null);

public sealed record DescribeRedshiftSubnetGroupsResult(
    List<ClusterSubnetGroup>? ClusterSubnetGroups = null,
    string? Marker = null);

public sealed record CreateRedshiftParameterGroupResult(
    string? ParameterGroupName = null,
    string? ParameterGroupFamily = null);

public sealed record DescribeRedshiftParameterGroupsResult(
    List<ClusterParameterGroup>? ParameterGroups = null,
    string? Marker = null);

public sealed record DescribeRedshiftParametersResult(
    List<Parameter>? Parameters = null,
    string? Marker = null);

public sealed record ModifyRedshiftParameterGroupResult(
    string? ParameterGroupName = null,
    string? ParameterGroupStatus = null);

public sealed record CreateRedshiftSecurityGroupResult(
    string? ClusterSecurityGroupName = null);

public sealed record DescribeRedshiftSecurityGroupsResult(
    List<ClusterSecurityGroup>? ClusterSecurityGroups = null,
    string? Marker = null);

public sealed record AuthorizeRedshiftIngressResult(
    string? ClusterSecurityGroupName = null,
    string? Status = null);

public sealed record RevokeRedshiftIngressResult(
    string? ClusterSecurityGroupName = null,
    string? Status = null);

public sealed record DescribeRedshiftTagsResult(
    List<TaggedResource>? TaggedResources = null,
    string? Marker = null);

public sealed record DescribeRedshiftLoggingStatusResult(
    bool? LoggingEnabled = null,
    string? BucketName = null,
    string? S3KeyPrefix = null,
    DateTime? LastSuccessfulDeliveryTime = null,
    DateTime? LastFailureTime = null,
    string? LastFailureMessage = null);

public sealed record EnableRedshiftLoggingResult(
    bool? LoggingEnabled = null,
    string? BucketName = null);

public sealed record DisableRedshiftLoggingResult(
    bool? LoggingEnabled = null);

public sealed record GetRedshiftClusterCredentialsResult(
    string? DbUser = null,
    string? DbPassword = null,
    DateTime? Expiration = null);

public sealed record DescribeNodeConfigurationOptionsResult(
    List<NodeConfigurationOption>? NodeConfigurationOptionList = null,
    string? Marker = null);

public sealed record DescribeOrderableClusterOptionsResult(
    List<OrderableClusterOption>? OrderableClusterOptions = null,
    string? Marker = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Redshift.
/// </summary>
public static class RedshiftService
{
    private static AmazonRedshiftClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRedshiftClient>(region);

    // ──────────────────────────── Clusters ─────────────────────────────

    /// <summary>
    /// Create a Redshift cluster.
    /// </summary>
    public static async Task<CreateRedshiftClusterResult> CreateClusterAsync(
        CreateClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterAsync(request);
            var c = resp.Cluster;
            return new CreateRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus,
                ClusterArn: c.ClusterNamespaceArn);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create Redshift cluster");
        }
    }

    /// <summary>
    /// Delete a Redshift cluster.
    /// </summary>
    public static async Task<DeleteRedshiftClusterResult> DeleteClusterAsync(
        string clusterIdentifier,
        bool? skipFinalClusterSnapshot = null,
        string? finalClusterSnapshotIdentifier = null,
        int? finalClusterSnapshotRetentionPeriod = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteClusterRequest
        {
            ClusterIdentifier = clusterIdentifier
        };
        if (skipFinalClusterSnapshot.HasValue)
            request.SkipFinalClusterSnapshot = skipFinalClusterSnapshot.Value;
        if (finalClusterSnapshotIdentifier != null)
            request.FinalClusterSnapshotIdentifier =
                finalClusterSnapshotIdentifier;
        if (finalClusterSnapshotRetentionPeriod.HasValue)
            request.FinalClusterSnapshotRetentionPeriod =
                finalClusterSnapshotRetentionPeriod.Value;

        try
        {
            var resp = await client.DeleteClusterAsync(request);
            var c = resp.Cluster;
            return new DeleteRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Redshift clusters.
    /// </summary>
    public static async Task<DescribeRedshiftClustersResult>
        DescribeClustersAsync(
            string? clusterIdentifier = null,
            int? maxRecords = null,
            string? marker = null,
            List<string>? tagKeys = null,
            List<string>? tagValues = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClustersRequest();
        if (clusterIdentifier != null)
            request.ClusterIdentifier = clusterIdentifier;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;
        if (tagKeys != null) request.TagKeys = tagKeys;
        if (tagValues != null) request.TagValues = tagValues;

        try
        {
            var resp = await client.DescribeClustersAsync(request);
            return new DescribeRedshiftClustersResult(
                Clusters: resp.Clusters,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift clusters");
        }
    }

    /// <summary>
    /// Modify a Redshift cluster.
    /// </summary>
    public static async Task<ModifyRedshiftClusterResult> ModifyClusterAsync(
        ModifyClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyClusterAsync(request);
            var c = resp.Cluster;
            return new ModifyRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify Redshift cluster");
        }
    }

    /// <summary>
    /// Reboot a Redshift cluster.
    /// </summary>
    public static async Task<RebootRedshiftClusterResult> RebootClusterAsync(
        string clusterIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RebootClusterAsync(new RebootClusterRequest
            {
                ClusterIdentifier = clusterIdentifier
            });
            return new RebootRedshiftClusterResult(
                ClusterIdentifier: resp.Cluster?.ClusterIdentifier);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Pause a Redshift cluster.
    /// </summary>
    public static async Task<PauseRedshiftClusterResult> PauseClusterAsync(
        string clusterIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PauseClusterAsync(new PauseClusterRequest
            {
                ClusterIdentifier = clusterIdentifier
            });
            var c = resp.Cluster;
            return new PauseRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to pause Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Resume a paused Redshift cluster.
    /// </summary>
    public static async Task<ResumeRedshiftClusterResult> ResumeClusterAsync(
        string clusterIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ResumeClusterAsync(
                new ResumeClusterRequest
                {
                    ClusterIdentifier = clusterIdentifier
                });
            var c = resp.Cluster;
            return new ResumeRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to resume Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Resize a Redshift cluster.
    /// </summary>
    public static async Task<ResizeRedshiftClusterResult> ResizeClusterAsync(
        ResizeClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ResizeClusterAsync(request);
            var c = resp.Cluster;
            return new ResizeRedshiftClusterResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to resize Redshift cluster");
        }
    }

    // ──────────────────────────── Snapshots ────────────────────────────

    /// <summary>
    /// Create a Redshift cluster snapshot.
    /// </summary>
    public static async Task<CreateRedshiftSnapshotResult>
        CreateClusterSnapshotAsync(
            string clusterIdentifier,
            string snapshotIdentifier,
            int? manualSnapshotRetentionPeriod = null,
            List<Amazon.Redshift.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateClusterSnapshotRequest
        {
            ClusterIdentifier = clusterIdentifier,
            SnapshotIdentifier = snapshotIdentifier
        };
        if (manualSnapshotRetentionPeriod.HasValue)
            request.ManualSnapshotRetentionPeriod =
                manualSnapshotRetentionPeriod.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateClusterSnapshotAsync(request);
            var s = resp.Snapshot;
            return new CreateRedshiftSnapshotResult(
                SnapshotIdentifier: s.SnapshotIdentifier,
                ClusterIdentifier: s.ClusterIdentifier,
                Status: s.Status);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Redshift snapshot '{snapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Delete a Redshift cluster snapshot.
    /// </summary>
    public static async Task<DeleteRedshiftSnapshotResult>
        DeleteClusterSnapshotAsync(
            string snapshotIdentifier,
            string? snapshotClusterIdentifier = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteClusterSnapshotRequest
        {
            SnapshotIdentifier = snapshotIdentifier
        };
        if (snapshotClusterIdentifier != null)
            request.SnapshotClusterIdentifier = snapshotClusterIdentifier;

        try
        {
            await client.DeleteClusterSnapshotAsync(request);
            return new DeleteRedshiftSnapshotResult(
                SnapshotIdentifier: snapshotIdentifier);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift snapshot '{snapshotIdentifier}'");
        }
    }

    /// <summary>
    /// Describe Redshift cluster snapshots.
    /// </summary>
    public static async Task<DescribeRedshiftSnapshotsResult>
        DescribeClusterSnapshotsAsync(
            string? clusterIdentifier = null,
            string? snapshotIdentifier = null,
            string? snapshotType = null,
            DateTime? startTime = null,
            DateTime? endTime = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClusterSnapshotsRequest();
        if (clusterIdentifier != null)
            request.ClusterIdentifier = clusterIdentifier;
        if (snapshotIdentifier != null)
            request.SnapshotIdentifier = snapshotIdentifier;
        if (snapshotType != null) request.SnapshotType = snapshotType;
        if (startTime.HasValue) request.StartTime = startTime.Value;
        if (endTime.HasValue) request.EndTime = endTime.Value;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeClusterSnapshotsAsync(request);
            return new DescribeRedshiftSnapshotsResult(
                Snapshots: resp.Snapshots,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift snapshots");
        }
    }

    /// <summary>
    /// Restore a Redshift cluster from a snapshot.
    /// </summary>
    public static async Task<RestoreFromRedshiftSnapshotResult>
        RestoreFromClusterSnapshotAsync(
            RestoreFromClusterSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreFromClusterSnapshotAsync(request);
            var c = resp.Cluster;
            return new RestoreFromRedshiftSnapshotResult(
                ClusterIdentifier: c.ClusterIdentifier,
                ClusterStatus: c.ClusterStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore Redshift cluster from snapshot");
        }
    }

    // ──────────────────────── Subnet Groups ───────────────────────────

    /// <summary>
    /// Create a Redshift cluster subnet group.
    /// </summary>
    public static async Task<CreateRedshiftSubnetGroupResult>
        CreateClusterSubnetGroupAsync(
            string clusterSubnetGroupName,
            string description,
            List<string> subnetIds,
            List<Amazon.Redshift.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateClusterSubnetGroupRequest
        {
            ClusterSubnetGroupName = clusterSubnetGroupName,
            Description = description,
            SubnetIds = subnetIds
        };
        if (tags != null) request.Tags = tags;

        try
        {
            await client.CreateClusterSubnetGroupAsync(request);
            return new CreateRedshiftSubnetGroupResult(
                ClusterSubnetGroupName: clusterSubnetGroupName);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Redshift subnet group '{clusterSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Delete a Redshift cluster subnet group.
    /// </summary>
    public static async Task DeleteClusterSubnetGroupAsync(
        string clusterSubnetGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteClusterSubnetGroupAsync(
                new DeleteClusterSubnetGroupRequest
                {
                    ClusterSubnetGroupName = clusterSubnetGroupName
                });
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift subnet group '{clusterSubnetGroupName}'");
        }
    }

    /// <summary>
    /// Describe Redshift cluster subnet groups.
    /// </summary>
    public static async Task<DescribeRedshiftSubnetGroupsResult>
        DescribeClusterSubnetGroupsAsync(
            string? clusterSubnetGroupName = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClusterSubnetGroupsRequest();
        if (clusterSubnetGroupName != null)
            request.ClusterSubnetGroupName = clusterSubnetGroupName;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeClusterSubnetGroupsAsync(request);
            return new DescribeRedshiftSubnetGroupsResult(
                ClusterSubnetGroups: resp.ClusterSubnetGroups,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift subnet groups");
        }
    }

    // ──────────────────────── Parameter Groups ─────────────────────────

    /// <summary>
    /// Create a Redshift cluster parameter group.
    /// </summary>
    public static async Task<CreateRedshiftParameterGroupResult>
        CreateClusterParameterGroupAsync(
            string parameterGroupName,
            string parameterGroupFamily,
            string description,
            List<Amazon.Redshift.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateClusterParameterGroupRequest
        {
            ParameterGroupName = parameterGroupName,
            ParameterGroupFamily = parameterGroupFamily,
            Description = description
        };
        if (tags != null) request.Tags = tags;

        try
        {
            await client.CreateClusterParameterGroupAsync(request);
            return new CreateRedshiftParameterGroupResult(
                ParameterGroupName: parameterGroupName,
                ParameterGroupFamily: parameterGroupFamily);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Redshift parameter group '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Delete a Redshift cluster parameter group.
    /// </summary>
    public static async Task DeleteClusterParameterGroupAsync(
        string parameterGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteClusterParameterGroupAsync(
                new DeleteClusterParameterGroupRequest
                {
                    ParameterGroupName = parameterGroupName
                });
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift parameter group '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Describe Redshift cluster parameter groups.
    /// </summary>
    public static async Task<DescribeRedshiftParameterGroupsResult>
        DescribeClusterParameterGroupsAsync(
            string? parameterGroupName = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClusterParameterGroupsRequest();
        if (parameterGroupName != null)
            request.ParameterGroupName = parameterGroupName;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp =
                await client.DescribeClusterParameterGroupsAsync(request);
            return new DescribeRedshiftParameterGroupsResult(
                ParameterGroups: resp.ParameterGroups,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift parameter groups");
        }
    }

    /// <summary>
    /// Describe parameters in a Redshift cluster parameter group.
    /// </summary>
    public static async Task<DescribeRedshiftParametersResult>
        DescribeClusterParametersAsync(
            string parameterGroupName,
            string? source = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClusterParametersRequest
        {
            ParameterGroupName = parameterGroupName
        };
        if (source != null) request.Source = source;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeClusterParametersAsync(request);
            return new DescribeRedshiftParametersResult(
                Parameters: resp.Parameters,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe parameters for Redshift group '{parameterGroupName}'");
        }
    }

    /// <summary>
    /// Modify a Redshift cluster parameter group.
    /// </summary>
    public static async Task<ModifyRedshiftParameterGroupResult>
        ModifyClusterParameterGroupAsync(
            string parameterGroupName,
            List<Parameter> parameters,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyClusterParameterGroupAsync(
                new ModifyClusterParameterGroupRequest
                {
                    ParameterGroupName = parameterGroupName,
                    Parameters = parameters
                });
            return new ModifyRedshiftParameterGroupResult(
                ParameterGroupName: resp.ParameterGroupName,
                ParameterGroupStatus: resp.ParameterGroupStatus);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify Redshift parameter group '{parameterGroupName}'");
        }
    }

    // ──────────────────────── Security Groups ──────────────────────────

    /// <summary>
    /// Create a Redshift cluster security group.
    /// </summary>
    public static async Task<CreateRedshiftSecurityGroupResult>
        CreateClusterSecurityGroupAsync(
            string clusterSecurityGroupName,
            string description,
            List<Amazon.Redshift.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateClusterSecurityGroupRequest
        {
            ClusterSecurityGroupName = clusterSecurityGroupName,
            Description = description
        };
        if (tags != null) request.Tags = tags;

        try
        {
            await client.CreateClusterSecurityGroupAsync(request);
            return new CreateRedshiftSecurityGroupResult(
                ClusterSecurityGroupName: clusterSecurityGroupName);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Redshift security group '{clusterSecurityGroupName}'");
        }
    }

    /// <summary>
    /// Delete a Redshift cluster security group.
    /// </summary>
    public static async Task DeleteClusterSecurityGroupAsync(
        string clusterSecurityGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteClusterSecurityGroupAsync(
                new DeleteClusterSecurityGroupRequest
                {
                    ClusterSecurityGroupName = clusterSecurityGroupName
                });
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Redshift security group '{clusterSecurityGroupName}'");
        }
    }

    /// <summary>
    /// Describe Redshift cluster security groups.
    /// </summary>
    public static async Task<DescribeRedshiftSecurityGroupsResult>
        DescribeClusterSecurityGroupsAsync(
            string? clusterSecurityGroupName = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeClusterSecurityGroupsRequest();
        if (clusterSecurityGroupName != null)
            request.ClusterSecurityGroupName = clusterSecurityGroupName;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp =
                await client.DescribeClusterSecurityGroupsAsync(request);
            return new DescribeRedshiftSecurityGroupsResult(
                ClusterSecurityGroups: resp.ClusterSecurityGroups,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift security groups");
        }
    }

    /// <summary>
    /// Authorize ingress to a Redshift cluster security group.
    /// </summary>
    public static async Task<AuthorizeRedshiftIngressResult>
        AuthorizeClusterSecurityGroupIngressAsync(
            string clusterSecurityGroupName,
            string? cidrip = null,
            string? ec2SecurityGroupName = null,
            string? ec2SecurityGroupOwnerId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AuthorizeClusterSecurityGroupIngressRequest
        {
            ClusterSecurityGroupName = clusterSecurityGroupName
        };
        if (cidrip != null) request.CIDRIP = cidrip;
        if (ec2SecurityGroupName != null)
            request.EC2SecurityGroupName = ec2SecurityGroupName;
        if (ec2SecurityGroupOwnerId != null)
            request.EC2SecurityGroupOwnerId = ec2SecurityGroupOwnerId;

        try
        {
            var resp =
                await client
                    .AuthorizeClusterSecurityGroupIngressAsync(request);
            var sg = resp.ClusterSecurityGroup;
            return new AuthorizeRedshiftIngressResult(
                ClusterSecurityGroupName: sg?.ClusterSecurityGroupName,
                Status: sg?.Description);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to authorize ingress for Redshift security group '{clusterSecurityGroupName}'");
        }
    }

    /// <summary>
    /// Revoke ingress from a Redshift cluster security group.
    /// </summary>
    public static async Task<RevokeRedshiftIngressResult>
        RevokeClusterSecurityGroupIngressAsync(
            string clusterSecurityGroupName,
            string? cidrip = null,
            string? ec2SecurityGroupName = null,
            string? ec2SecurityGroupOwnerId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RevokeClusterSecurityGroupIngressRequest
        {
            ClusterSecurityGroupName = clusterSecurityGroupName
        };
        if (cidrip != null) request.CIDRIP = cidrip;
        if (ec2SecurityGroupName != null)
            request.EC2SecurityGroupName = ec2SecurityGroupName;
        if (ec2SecurityGroupOwnerId != null)
            request.EC2SecurityGroupOwnerId = ec2SecurityGroupOwnerId;

        try
        {
            var resp =
                await client
                    .RevokeClusterSecurityGroupIngressAsync(request);
            var sg = resp.ClusterSecurityGroup;
            return new RevokeRedshiftIngressResult(
                ClusterSecurityGroupName: sg?.ClusterSecurityGroupName,
                Status: sg?.Description);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to revoke ingress for Redshift security group '{clusterSecurityGroupName}'");
        }
    }

    // ──────────────────────────── Tags ─────────────────────────────────

    /// <summary>
    /// Add tags to a Redshift resource.
    /// </summary>
    public static async Task CreateTagsAsync(
        string resourceName,
        List<Amazon.Redshift.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateTagsAsync(new CreateTagsRequest
            {
                ResourceName = resourceName,
                Tags = tags
            });
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create tags for Redshift resource '{resourceName}'");
        }
    }

    /// <summary>
    /// Delete tags from a Redshift resource.
    /// </summary>
    public static async Task DeleteTagsAsync(
        string resourceName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTagsAsync(new DeleteTagsRequest
            {
                ResourceName = resourceName,
                TagKeys = tagKeys
            });
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete tags from Redshift resource '{resourceName}'");
        }
    }

    /// <summary>
    /// Describe tags for Redshift resources.
    /// </summary>
    public static async Task<DescribeRedshiftTagsResult> DescribeTagsAsync(
        string? resourceName = null,
        string? resourceType = null,
        int? maxRecords = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTagsRequest();
        if (resourceName != null) request.ResourceName = resourceName;
        if (resourceType != null) request.ResourceType = resourceType;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.DescribeTagsAsync(request);
            return new DescribeRedshiftTagsResult(
                TaggedResources: resp.TaggedResources,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift tags");
        }
    }

    // ──────────────────────────── Logging ──────────────────────────────

    /// <summary>
    /// Describe the logging status of a Redshift cluster.
    /// </summary>
    public static async Task<DescribeRedshiftLoggingStatusResult>
        DescribeLoggingStatusAsync(
            string clusterIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLoggingStatusAsync(
                new DescribeLoggingStatusRequest
                {
                    ClusterIdentifier = clusterIdentifier
                });
            return new DescribeRedshiftLoggingStatusResult(
                LoggingEnabled: resp.LoggingEnabled,
                BucketName: resp.BucketName,
                S3KeyPrefix: resp.S3KeyPrefix,
                LastSuccessfulDeliveryTime: resp.LastSuccessfulDeliveryTime,
                LastFailureTime: resp.LastFailureTime,
                LastFailureMessage: resp.LastFailureMessage);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe logging status for Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Enable logging for a Redshift cluster.
    /// </summary>
    public static async Task<EnableRedshiftLoggingResult>
        EnableLoggingAsync(
            string clusterIdentifier,
            string? bucketName = null,
            string? s3KeyPrefix = null,
            string? logDestinationType = null,
            List<string>? logExports = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableLoggingRequest
        {
            ClusterIdentifier = clusterIdentifier
        };
        if (bucketName != null) request.BucketName = bucketName;
        if (s3KeyPrefix != null) request.S3KeyPrefix = s3KeyPrefix;
        if (logDestinationType != null)
            request.LogDestinationType =
                new LogDestinationType(logDestinationType);
        if (logExports != null) request.LogExports = logExports;

        try
        {
            var resp = await client.EnableLoggingAsync(request);
            return new EnableRedshiftLoggingResult(
                LoggingEnabled: resp.LoggingEnabled,
                BucketName: resp.BucketName);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable logging for Redshift cluster '{clusterIdentifier}'");
        }
    }

    /// <summary>
    /// Disable logging for a Redshift cluster.
    /// </summary>
    public static async Task<DisableRedshiftLoggingResult>
        DisableLoggingAsync(
            string clusterIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisableLoggingAsync(
                new DisableLoggingRequest
                {
                    ClusterIdentifier = clusterIdentifier
                });
            return new DisableRedshiftLoggingResult(
                LoggingEnabled: resp.LoggingEnabled);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable logging for Redshift cluster '{clusterIdentifier}'");
        }
    }

    // ──────────────────────── Credentials ──────────────────────────────

    /// <summary>
    /// Get temporary credentials for a Redshift cluster.
    /// </summary>
    public static async Task<GetRedshiftClusterCredentialsResult>
        GetClusterCredentialsAsync(
            string clusterIdentifier,
            string dbUser,
            string? dbName = null,
            List<string>? dbGroups = null,
            bool? autoCreate = null,
            int? durationSeconds = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetClusterCredentialsRequest
        {
            ClusterIdentifier = clusterIdentifier,
            DbUser = dbUser
        };
        if (dbName != null) request.DbName = dbName;
        if (dbGroups != null) request.DbGroups = dbGroups;
        if (autoCreate.HasValue) request.AutoCreate = autoCreate.Value;
        if (durationSeconds.HasValue)
            request.DurationSeconds = durationSeconds.Value;

        try
        {
            var resp = await client.GetClusterCredentialsAsync(request);
            return new GetRedshiftClusterCredentialsResult(
                DbUser: resp.DbUser,
                DbPassword: resp.DbPassword,
                Expiration: resp.Expiration);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get credentials for Redshift cluster '{clusterIdentifier}'");
        }
    }

    // ────────────────────── Node / Orderable ───────────────────────────

    /// <summary>
    /// Describe node configuration options for a Redshift cluster.
    /// </summary>
    public static async Task<DescribeNodeConfigurationOptionsResult>
        DescribeNodeConfigurationOptionsAsync(
            DescribeNodeConfigurationOptionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.DescribeNodeConfigurationOptionsAsync(request);
            return new DescribeNodeConfigurationOptionsResult(
                NodeConfigurationOptionList:
                    resp.NodeConfigurationOptionList,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift node configuration options");
        }
    }

    /// <summary>
    /// Describe orderable cluster options for Redshift.
    /// </summary>
    public static async Task<DescribeOrderableClusterOptionsResult>
        DescribeOrderableClusterOptionsAsync(
            string? clusterVersion = null,
            string? nodeType = null,
            int? maxRecords = null,
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeOrderableClusterOptionsRequest();
        if (clusterVersion != null) request.ClusterVersion = clusterVersion;
        if (nodeType != null) request.NodeType = nodeType;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp =
                await client.DescribeOrderableClusterOptionsAsync(request);
            return new DescribeOrderableClusterOptionsResult(
                OrderableClusterOptions: resp.OrderableClusterOptions,
                Marker: resp.Marker);
        }
        catch (AmazonRedshiftException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift orderable cluster options");
        }
    }
}
