using Amazon;
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of CreateFileSystem.</summary>
public sealed record CreateFileSystemResult(
    string? FileSystemId = null,
    string? FileSystemArn = null,
    string? LifeCycleState = null,
    string? Name = null,
    string? PerformanceMode = null,
    string? ThroughputMode = null,
    double? ProvisionedThroughputInMibps = null,
    bool? Encrypted = null,
    string? KmsKeyId = null,
    DateTime? CreationTime = null);

/// <summary>Result of DeleteFileSystem (void operation).</summary>
public sealed record DeleteFileSystemResult(bool Success = true);

/// <summary>Result of DescribeFileSystems.</summary>
public sealed record DescribeFileSystemsResult(
    List<FileSystemDescription>? FileSystems = null,
    string? NextMarker = null);

/// <summary>Result of UpdateFileSystem.</summary>
public sealed record UpdateFileSystemResult(
    string? FileSystemId = null,
    string? FileSystemArn = null,
    string? LifeCycleState = null,
    string? ThroughputMode = null,
    double? ProvisionedThroughputInMibps = null);

/// <summary>Result of CreateMountTarget.</summary>
public sealed record CreateMountTargetResult(
    string? MountTargetId = null,
    string? FileSystemId = null,
    string? SubnetId = null,
    string? LifeCycleState = null,
    string? IpAddress = null,
    string? NetworkInterfaceId = null,
    string? AvailabilityZoneId = null,
    string? AvailabilityZoneName = null,
    string? VpcId = null);

/// <summary>Result of DeleteMountTarget (void operation).</summary>
public sealed record DeleteMountTargetResult(bool Success = true);

/// <summary>Result of DescribeMountTargets.</summary>
public sealed record DescribeMountTargetsResult(
    List<MountTargetDescription>? MountTargets = null,
    string? NextMarker = null);

/// <summary>Result of DescribeMountTargetSecurityGroups.</summary>
public sealed record DescribeMountTargetSecurityGroupsResult(
    List<string>? SecurityGroups = null);

/// <summary>Result of ModifyMountTargetSecurityGroups (void operation).</summary>
public sealed record ModifyMountTargetSecurityGroupsResult(bool Success = true);

/// <summary>Result of CreateAccessPoint.</summary>
public sealed record CreateAccessPointResult(
    string? AccessPointId = null,
    string? AccessPointArn = null,
    string? FileSystemId = null,
    string? LifeCycleState = null,
    string? Name = null,
    string? ClientToken = null);

/// <summary>Result of DeleteAccessPoint (void operation).</summary>
public sealed record DeleteAccessPointResult(bool Success = true);

/// <summary>Result of DescribeAccessPoints.</summary>
public sealed record DescribeAccessPointsResult(
    List<AccessPointDescription>? AccessPoints = null,
    string? NextToken = null);

/// <summary>Result of PutFileSystemPolicy.</summary>
public sealed record PutFileSystemPolicyResult(
    string? FileSystemId = null,
    string? Policy = null);

/// <summary>Result of DescribeFileSystemPolicy.</summary>
public sealed record DescribeFileSystemPolicyResult(
    string? FileSystemId = null,
    string? Policy = null);

/// <summary>Result of DeleteFileSystemPolicy (void operation).</summary>
public sealed record DeleteFileSystemPolicyResult(bool Success = true);

/// <summary>Result of PutLifecycleConfiguration.</summary>
public sealed record PutLifecycleConfigurationResult(
    List<LifecyclePolicy>? LifecyclePolicies = null);

/// <summary>Result of DescribeLifecycleConfiguration.</summary>
public sealed record DescribeLifecycleConfigurationResult(
    List<LifecyclePolicy>? LifecyclePolicies = null);

/// <summary>Result of PutBackupPolicy.</summary>
public sealed record PutBackupPolicyResult(BackupPolicy? BackupPolicy = null);

/// <summary>Result of DescribeBackupPolicy.</summary>
public sealed record DescribeBackupPolicyResult(BackupPolicy? BackupPolicy = null);

/// <summary>Result of CreateReplicationConfiguration.</summary>
public sealed record CreateReplicationConfigurationResult(
    string? SourceFileSystemId = null,
    string? SourceFileSystemRegion = null,
    string? SourceFileSystemArn = null,
    string? OriginalSourceFileSystemArn = null,
    DateTime? CreationTime = null,
    List<Destination>? Destinations = null);

/// <summary>Result of DeleteReplicationConfiguration (void operation).</summary>
public sealed record DeleteReplicationConfigurationResult(bool Success = true);

/// <summary>Result of DescribeReplicationConfigurations.</summary>
public sealed record DescribeReplicationConfigurationsResult(
    List<ReplicationConfigurationDescription>? Replications = null,
    string? NextToken = null);

/// <summary>Result of TagResource (void operation).</summary>
public sealed record EfsTagResourceResult(bool Success = true);

/// <summary>Result of UntagResource (void operation).</summary>
public sealed record EfsUntagResourceResult(bool Success = true);

/// <summary>Result of ListTagsForResource.</summary>
public sealed record EfsListTagsForResourceResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

/// <summary>Result of DescribeAccountPreferences.</summary>
public sealed record DescribeAccountPreferencesResult(
    List<ResourceIdPreference>? ResourceIdPreference = null,
    string? NextToken = null);

/// <summary>Result of PutAccountPreferences.</summary>
public sealed record PutAccountPreferencesResult(
    ResourceIdPreference? ResourceIdPreference = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Elastic File System (EFS).
/// </summary>
public static class EfsService
{
    private static AmazonElasticFileSystemClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonElasticFileSystemClient>(region);

    // -----------------------------------------------------------------------
    // File system management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new EFS file system.
    /// </summary>
    public static async Task<CreateFileSystemResult> CreateFileSystemAsync(
        CreateFileSystemRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateFileSystemAsync(request);
            return new CreateFileSystemResult(
                FileSystemId: resp.FileSystemId,
                FileSystemArn: resp.FileSystemArn,
                LifeCycleState: resp.LifeCycleState?.Value,
                Name: resp.Name,
                PerformanceMode: resp.PerformanceMode?.Value,
                ThroughputMode: resp.ThroughputMode?.Value,
                ProvisionedThroughputInMibps: resp.ProvisionedThroughputInMibps,
                Encrypted: resp.Encrypted,
                KmsKeyId: resp.KmsKeyId,
                CreationTime: resp.CreationTime);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create file system");
        }
    }

    /// <summary>
    /// Delete an EFS file system.
    /// </summary>
    public static async Task<DeleteFileSystemResult> DeleteFileSystemAsync(
        string fileSystemId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFileSystemAsync(new DeleteFileSystemRequest
            {
                FileSystemId = fileSystemId
            });
            return new DeleteFileSystemResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete file system '{fileSystemId}'");
        }
    }

    /// <summary>
    /// Describe EFS file systems.
    /// </summary>
    public static async Task<DescribeFileSystemsResult> DescribeFileSystemsAsync(
        string? fileSystemId = null,
        string? creationToken = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeFileSystemsRequest();
        if (fileSystemId != null) request.FileSystemId = fileSystemId;
        if (creationToken != null) request.CreationToken = creationToken;
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.DescribeFileSystemsAsync(request);
            return new DescribeFileSystemsResult(
                FileSystems: resp.FileSystems,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe file systems");
        }
    }

    /// <summary>
    /// Update an EFS file system.
    /// </summary>
    public static async Task<UpdateFileSystemResult> UpdateFileSystemAsync(
        UpdateFileSystemRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateFileSystemAsync(request);
            return new UpdateFileSystemResult(
                FileSystemId: resp.FileSystemId,
                FileSystemArn: resp.FileSystemArn,
                LifeCycleState: resp.LifeCycleState?.Value,
                ThroughputMode: resp.ThroughputMode?.Value,
                ProvisionedThroughputInMibps: resp.ProvisionedThroughputInMibps);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update file system");
        }
    }

    // -----------------------------------------------------------------------
    // Mount targets
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a mount target for an EFS file system.
    /// </summary>
    public static async Task<CreateMountTargetResult> CreateMountTargetAsync(
        CreateMountTargetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateMountTargetAsync(request);
            return new CreateMountTargetResult(
                MountTargetId: resp.MountTargetId,
                FileSystemId: resp.FileSystemId,
                SubnetId: resp.SubnetId,
                LifeCycleState: resp.LifeCycleState?.Value,
                IpAddress: resp.IpAddress,
                NetworkInterfaceId: resp.NetworkInterfaceId,
                AvailabilityZoneId: resp.AvailabilityZoneId,
                AvailabilityZoneName: resp.AvailabilityZoneName,
                VpcId: resp.VpcId);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create mount target");
        }
    }

    /// <summary>
    /// Delete a mount target.
    /// </summary>
    public static async Task<DeleteMountTargetResult> DeleteMountTargetAsync(
        string mountTargetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteMountTargetAsync(new DeleteMountTargetRequest
            {
                MountTargetId = mountTargetId
            });
            return new DeleteMountTargetResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete mount target '{mountTargetId}'");
        }
    }

    /// <summary>
    /// Describe mount targets for a file system.
    /// </summary>
    public static async Task<DescribeMountTargetsResult> DescribeMountTargetsAsync(
        string? fileSystemId = null,
        string? mountTargetId = null,
        string? accessPointId = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeMountTargetsRequest();
        if (fileSystemId != null) request.FileSystemId = fileSystemId;
        if (mountTargetId != null) request.MountTargetId = mountTargetId;
        if (accessPointId != null) request.AccessPointId = accessPointId;
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.DescribeMountTargetsAsync(request);
            return new DescribeMountTargetsResult(
                MountTargets: resp.MountTargets,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe mount targets");
        }
    }

    /// <summary>
    /// Describe the security groups attached to a mount target.
    /// </summary>
    public static async Task<DescribeMountTargetSecurityGroupsResult>
        DescribeMountTargetSecurityGroupsAsync(
            string mountTargetId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeMountTargetSecurityGroupsAsync(
                new DescribeMountTargetSecurityGroupsRequest
                {
                    MountTargetId = mountTargetId
                });
            return new DescribeMountTargetSecurityGroupsResult(
                SecurityGroups: resp.SecurityGroups);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe security groups for mount target '{mountTargetId}'");
        }
    }

    /// <summary>
    /// Modify the security groups attached to a mount target.
    /// </summary>
    public static async Task<ModifyMountTargetSecurityGroupsResult>
        ModifyMountTargetSecurityGroupsAsync(
            string mountTargetId,
            List<string> securityGroups,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ModifyMountTargetSecurityGroupsAsync(
                new ModifyMountTargetSecurityGroupsRequest
                {
                    MountTargetId = mountTargetId,
                    SecurityGroups = securityGroups
                });
            return new ModifyMountTargetSecurityGroupsResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify security groups for mount target '{mountTargetId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Access points
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create an EFS access point.
    /// </summary>
    public static async Task<CreateAccessPointResult> CreateAccessPointAsync(
        CreateAccessPointRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAccessPointAsync(request);
            return new CreateAccessPointResult(
                AccessPointId: resp.AccessPointId,
                AccessPointArn: resp.AccessPointArn,
                FileSystemId: resp.FileSystemId,
                LifeCycleState: resp.LifeCycleState?.Value,
                Name: resp.Name,
                ClientToken: resp.ClientToken);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create access point");
        }
    }

    /// <summary>
    /// Delete an EFS access point.
    /// </summary>
    public static async Task<DeleteAccessPointResult> DeleteAccessPointAsync(
        string accessPointId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAccessPointAsync(new DeleteAccessPointRequest
            {
                AccessPointId = accessPointId
            });
            return new DeleteAccessPointResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete access point '{accessPointId}'");
        }
    }

    /// <summary>
    /// Describe EFS access points.
    /// </summary>
    public static async Task<DescribeAccessPointsResult> DescribeAccessPointsAsync(
        string? fileSystemId = null,
        string? accessPointId = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAccessPointsRequest();
        if (fileSystemId != null) request.FileSystemId = fileSystemId;
        if (accessPointId != null) request.AccessPointId = accessPointId;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAccessPointsAsync(request);
            return new DescribeAccessPointsResult(
                AccessPoints: resp.AccessPoints,
                NextToken: resp.NextToken);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe access points");
        }
    }

    // -----------------------------------------------------------------------
    // File system policy
    // -----------------------------------------------------------------------

    /// <summary>
    /// Set a file system policy.
    /// </summary>
    public static async Task<PutFileSystemPolicyResult> PutFileSystemPolicyAsync(
        PutFileSystemPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutFileSystemPolicyAsync(request);
            return new PutFileSystemPolicyResult(
                FileSystemId: resp.FileSystemId,
                Policy: resp.Policy);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put file system policy");
        }
    }

    /// <summary>
    /// Describe the file system policy.
    /// </summary>
    public static async Task<DescribeFileSystemPolicyResult> DescribeFileSystemPolicyAsync(
        string fileSystemId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeFileSystemPolicyAsync(
                new DescribeFileSystemPolicyRequest
                {
                    FileSystemId = fileSystemId
                });
            return new DescribeFileSystemPolicyResult(
                FileSystemId: resp.FileSystemId,
                Policy: resp.Policy);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe file system policy for '{fileSystemId}'");
        }
    }

    /// <summary>
    /// Delete the file system policy.
    /// </summary>
    public static async Task<DeleteFileSystemPolicyResult> DeleteFileSystemPolicyAsync(
        string fileSystemId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFileSystemPolicyAsync(new DeleteFileSystemPolicyRequest
            {
                FileSystemId = fileSystemId
            });
            return new DeleteFileSystemPolicyResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete file system policy for '{fileSystemId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle configuration
    // -----------------------------------------------------------------------

    /// <summary>
    /// Set the lifecycle configuration for a file system.
    /// </summary>
    public static async Task<PutLifecycleConfigurationResult> PutLifecycleConfigurationAsync(
        PutLifecycleConfigurationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutLifecycleConfigurationAsync(request);
            return new PutLifecycleConfigurationResult(
                LifecyclePolicies: resp.LifecyclePolicies);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put lifecycle configuration");
        }
    }

    /// <summary>
    /// Describe the lifecycle configuration for a file system.
    /// </summary>
    public static async Task<DescribeLifecycleConfigurationResult>
        DescribeLifecycleConfigurationAsync(
            string fileSystemId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLifecycleConfigurationAsync(
                new DescribeLifecycleConfigurationRequest
                {
                    FileSystemId = fileSystemId
                });
            return new DescribeLifecycleConfigurationResult(
                LifecyclePolicies: resp.LifecyclePolicies);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe lifecycle configuration for '{fileSystemId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Backup policy
    // -----------------------------------------------------------------------

    /// <summary>
    /// Set the backup policy for a file system.
    /// </summary>
    public static async Task<PutBackupPolicyResult> PutBackupPolicyAsync(
        string fileSystemId,
        BackupPolicy backupPolicy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutBackupPolicyAsync(new PutBackupPolicyRequest
            {
                FileSystemId = fileSystemId,
                BackupPolicy = backupPolicy
            });
            return new PutBackupPolicyResult(BackupPolicy: resp.BackupPolicy);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put backup policy for '{fileSystemId}'");
        }
    }

    /// <summary>
    /// Describe the backup policy for a file system.
    /// </summary>
    public static async Task<DescribeBackupPolicyResult> DescribeBackupPolicyAsync(
        string fileSystemId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeBackupPolicyAsync(
                new DescribeBackupPolicyRequest
                {
                    FileSystemId = fileSystemId
                });
            return new DescribeBackupPolicyResult(BackupPolicy: resp.BackupPolicy);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe backup policy for '{fileSystemId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Replication
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a replication configuration for a file system.
    /// </summary>
    public static async Task<CreateReplicationConfigurationResult>
        CreateReplicationConfigurationAsync(
            CreateReplicationConfigurationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateReplicationConfigurationAsync(request);
            return new CreateReplicationConfigurationResult(
                SourceFileSystemId: resp.SourceFileSystemId,
                SourceFileSystemRegion: resp.SourceFileSystemRegion,
                SourceFileSystemArn: resp.SourceFileSystemArn,
                OriginalSourceFileSystemArn: resp.OriginalSourceFileSystemArn,
                CreationTime: resp.CreationTime,
                Destinations: resp.Destinations);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create replication configuration");
        }
    }

    /// <summary>
    /// Delete a replication configuration.
    /// </summary>
    public static async Task<DeleteReplicationConfigurationResult>
        DeleteReplicationConfigurationAsync(
            string sourceFileSystemId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteReplicationConfigurationAsync(
                new DeleteReplicationConfigurationRequest
                {
                    SourceFileSystemId = sourceFileSystemId
                });
            return new DeleteReplicationConfigurationResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete replication configuration for '{sourceFileSystemId}'");
        }
    }

    /// <summary>
    /// Describe replication configurations.
    /// </summary>
    public static async Task<DescribeReplicationConfigurationsResult>
        DescribeReplicationConfigurationsAsync(
            string? fileSystemId = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeReplicationConfigurationsRequest();
        if (fileSystemId != null) request.FileSystemId = fileSystemId;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeReplicationConfigurationsAsync(request);
            return new DescribeReplicationConfigurationsResult(
                Replications: resp.Replications,
                NextToken: resp.NextToken);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe replication configurations");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Tag an EFS resource.
    /// </summary>
    public static async Task<EfsTagResourceResult> TagResourceAsync(
        string resourceId,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceId = resourceId,
                Tags = tags
            });
            return new EfsTagResourceResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceId}'");
        }
    }

    /// <summary>
    /// Remove tags from an EFS resource.
    /// </summary>
    public static async Task<EfsUntagResourceResult> UntagResourceAsync(
        string resourceId,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceId = resourceId,
                TagKeys = tagKeys
            });
            return new EfsUntagResourceResult();
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceId}'");
        }
    }

    /// <summary>
    /// List tags for an EFS resource.
    /// </summary>
    public static async Task<EfsListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest { ResourceId = resourceId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new EfsListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Account preferences
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe account preferences for EFS.
    /// </summary>
    public static async Task<DescribeAccountPreferencesResult>
        DescribeAccountPreferencesAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAccountPreferencesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAccountPreferencesAsync(request);
            return new DescribeAccountPreferencesResult(
                ResourceIdPreference: resp.ResourceIdPreference != null
                    ? new List<ResourceIdPreference> { resp.ResourceIdPreference }
                    : null,
                NextToken: resp.NextToken);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe account preferences");
        }
    }

    /// <summary>
    /// Set account preferences for EFS.
    /// </summary>
    public static async Task<PutAccountPreferencesResult> PutAccountPreferencesAsync(
        PutAccountPreferencesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutAccountPreferencesAsync(request);
            return new PutAccountPreferencesResult(
                ResourceIdPreference: resp.ResourceIdPreference);
        }
        catch (AmazonElasticFileSystemException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put account preferences");
        }
    }
}
