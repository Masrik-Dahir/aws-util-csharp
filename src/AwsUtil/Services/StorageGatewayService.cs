using Amazon;
using Amazon.StorageGateway;
using Amazon.StorageGateway.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record StorageGatewayInfo(
    string? GatewayARN = null,
    string? GatewayId = null,
    string? GatewayName = null,
    string? GatewayType = null,
    string? GatewayState = null,
    string? GatewayTimezone = null,
    string? Ec2InstanceId = null,
    string? Ec2InstanceRegion = null);

public sealed record StorageGatewayActivationResult(
    string? GatewayARN = null);

public sealed record StorageGatewayVolumeInfo(
    string? VolumeARN = null,
    string? VolumeId = null,
    string? VolumeType = null,
    string? VolumeStatus = null,
    long? VolumeSizeInBytes = null,
    double? VolumeProgress = null,
    string? GatewayARN = null,
    string? GatewayId = null);

public sealed record StorageGatewayStorediSCSIVolumeInfo(
    string? VolumeARN = null,
    string? VolumeId = null,
    string? VolumeType = null,
    string? VolumeStatus = null,
    long? VolumeSizeInBytes = null,
    double? VolumeProgress = null,
    string? VolumeDiskId = null,
    bool? PreservedExistingData = null,
    string? SourceSnapshotId = null);

public sealed record StorageGatewayCachediSCSIVolumeInfo(
    string? VolumeARN = null,
    string? VolumeId = null,
    string? VolumeType = null,
    string? VolumeStatus = null,
    long? VolumeSizeInBytes = null,
    double? VolumeProgress = null,
    string? SourceSnapshotId = null);

public sealed record StorageGatewayCreateVolumeResult(
    string? VolumeARN = null,
    string? TargetARN = null);

public sealed record StorageGatewaySnapshotResult(
    string? SnapshotId = null,
    string? VolumeARN = null);

public sealed record StorageGatewaySnapshotScheduleInfo(
    string? VolumeARN = null,
    int? StartAt = null,
    int? RecurrenceInHours = null,
    string? Description = null,
    string? Timezone = null);

public sealed record StorageGatewayFileShareInfo(
    string? FileShareARN = null,
    string? FileShareId = null,
    string? FileShareStatus = null,
    string? GatewayARN = null,
    string? FileShareType = null,
    string? LocationARN = null,
    string? Role = null,
    string? Path = null);

public sealed record StorageGatewayNFSFileShareInfo(
    string? FileShareARN = null,
    string? FileShareId = null,
    string? FileShareStatus = null,
    string? GatewayARN = null,
    string? LocationARN = null,
    string? Role = null,
    string? Path = null,
    string? DefaultStorageClass = null,
    string? Squash = null);

public sealed record StorageGatewaySMBFileShareInfo(
    string? FileShareARN = null,
    string? FileShareId = null,
    string? FileShareStatus = null,
    string? GatewayARN = null,
    string? LocationARN = null,
    string? Role = null,
    string? Path = null,
    string? DefaultStorageClass = null,
    bool? SMBACLEnabled = null);

public sealed record StorageGatewayTapeInfo(
    string? TapeARN = null,
    string? TapeBarcode = null,
    string? TapeStatus = null,
    long? TapeSizeInBytes = null,
    string? PoolId = null,
    string? GatewayARN = null);

public sealed record StorageGatewayTapePoolInfo(
    string? PoolARN = null,
    string? PoolName = null,
    string? StorageClass = null,
    string? PoolStatus = null);

public sealed record StorageGatewayCreateTapeResult(
    string? TapeARN = null);

public sealed record StorageGatewayLocalDiskInfo(
    string? DiskId = null,
    string? DiskPath = null,
    string? DiskNode = null,
    string? DiskStatus = null,
    long? DiskSizeInBytes = null,
    string? DiskAllocationType = null,
    string? DiskAllocationResource = null);

public sealed record StorageGatewayCacheInfo(
    string? GatewayARN = null,
    List<string>? DiskIds = null,
    long? CacheAllocatedInBytes = null,
    double? CacheUsedPercentage = null,
    double? CacheDirtyPercentage = null,
    double? CacheHitPercentage = null,
    double? CacheMissPercentage = null);

public sealed record StorageGatewayWorkingStorageInfo(
    string? GatewayARN = null,
    List<string>? DiskIds = null,
    long? WorkingStorageAllocatedInBytes = null,
    long? WorkingStorageUsedInBytes = null);

public sealed record StorageGatewayTagResult(bool Success = true);

public sealed record StorageGatewayListTagsResult(
    List<Tag>? Tags = null,
    string? Marker = null);

/// <summary>
/// Utility helpers for AWS Storage Gateway.
/// </summary>
public static class StorageGatewayService
{
    private static AmazonStorageGatewayClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonStorageGatewayClient>(region);

    // ── Gateway operations ───────────────────────────────────────────

    /// <summary>
    /// Activate a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        ActivateGatewayAsync(
            ActivateGatewayRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ActivateGatewayAsync(request);
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to activate gateway");
        }
    }

    /// <summary>
    /// Delete a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        DeleteGatewayAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGatewayAsync(new DeleteGatewayRequest
            {
                GatewayARN = gatewayARN
            });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Describe gateway information.
    /// </summary>
    public static async Task<StorageGatewayInfo>
        DescribeGatewayInformationAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeGatewayInformationAsync(
                new DescribeGatewayInformationRequest
                {
                    GatewayARN = gatewayARN
                });
            return new StorageGatewayInfo(
                GatewayARN: resp.GatewayARN,
                GatewayId: resp.GatewayId,
                GatewayName: resp.GatewayName,
                GatewayType: resp.GatewayType,
                GatewayState: resp.GatewayState,
                GatewayTimezone: resp.GatewayTimezone,
                Ec2InstanceId: resp.Ec2InstanceId,
                Ec2InstanceRegion: resp.Ec2InstanceRegion);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// List gateways.
    /// </summary>
    public static async Task<List<StorageGatewayInfo>> ListGatewaysAsync(
        string? marker = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGatewaysRequest();
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListGatewaysAsync(request);
            return resp.Gateways.Select(g => new StorageGatewayInfo(
                GatewayARN: g.GatewayARN,
                GatewayId: g.GatewayId,
                GatewayName: g.GatewayName,
                GatewayType: g.GatewayType)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list gateways");
        }
    }

    /// <summary>
    /// Shut down a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        ShutdownGatewayAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ShutdownGatewayAsync(
                new ShutdownGatewayRequest { GatewayARN = gatewayARN });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to shut down gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Start a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        StartGatewayAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartGatewayAsync(
                new StartGatewayRequest { GatewayARN = gatewayARN });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Update gateway information.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        UpdateGatewayInformationAsync(
            UpdateGatewayInformationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateGatewayInformationAsync(request);
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update gateway information");
        }
    }

    /// <summary>
    /// Update gateway software now.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        UpdateGatewaySoftwareNowAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateGatewaySoftwareNowAsync(
                new UpdateGatewaySoftwareNowRequest
                {
                    GatewayARN = gatewayARN
                });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update gateway software for '{gatewayARN}'");
        }
    }

    // ── Volume operations ────────────────────────────────────────────

    /// <summary>
    /// Create a cached iSCSI volume.
    /// </summary>
    public static async Task<StorageGatewayCreateVolumeResult>
        CreateCachediSCSIVolumeAsync(
            CreateCachediSCSIVolumeRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateCachediSCSIVolumeAsync(request);
            return new StorageGatewayCreateVolumeResult(
                VolumeARN: resp.VolumeARN,
                TargetARN: resp.TargetARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create cached iSCSI volume");
        }
    }

    /// <summary>
    /// Create a stored iSCSI volume.
    /// </summary>
    public static async Task<StorageGatewayCreateVolumeResult>
        CreateStorediSCSIVolumeAsync(
            CreateStorediSCSIVolumeRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStorediSCSIVolumeAsync(request);
            return new StorageGatewayCreateVolumeResult(
                VolumeARN: resp.VolumeARN,
                TargetARN: resp.TargetARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create stored iSCSI volume");
        }
    }

    /// <summary>
    /// Delete a volume.
    /// </summary>
    public static async Task<string?> DeleteVolumeAsync(
        string volumeARN,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteVolumeAsync(new DeleteVolumeRequest
            {
                VolumeARN = volumeARN
            });
            return resp.VolumeARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete volume '{volumeARN}'");
        }
    }

    /// <summary>
    /// Describe stored iSCSI volumes.
    /// </summary>
    public static async Task<List<StorageGatewayStorediSCSIVolumeInfo>>
        DescribeStorediSCSIVolumesAsync(
            List<string> volumeARNs,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStorediSCSIVolumesAsync(
                new DescribeStorediSCSIVolumesRequest
                {
                    VolumeARNs = volumeARNs
                });
            return resp.StorediSCSIVolumes.Select(v =>
                new StorageGatewayStorediSCSIVolumeInfo(
                    VolumeARN: v.VolumeARN,
                    VolumeId: v.VolumeId,
                    VolumeType: v.VolumeType,
                    VolumeStatus: v.VolumeStatus,
                    VolumeSizeInBytes: v.VolumeSizeInBytes,
                    VolumeProgress: v.VolumeProgress,
                    VolumeDiskId: v.VolumeDiskId,
                    PreservedExistingData: v.PreservedExistingData,
                    SourceSnapshotId: v.SourceSnapshotId)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe stored iSCSI volumes");
        }
    }

    /// <summary>
    /// Describe cached iSCSI volumes.
    /// </summary>
    public static async Task<List<StorageGatewayCachediSCSIVolumeInfo>>
        DescribeCachediSCSIVolumesAsync(
            List<string> volumeARNs,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCachediSCSIVolumesAsync(
                new DescribeCachediSCSIVolumesRequest
                {
                    VolumeARNs = volumeARNs
                });
            return resp.CachediSCSIVolumes.Select(v =>
                new StorageGatewayCachediSCSIVolumeInfo(
                    VolumeARN: v.VolumeARN,
                    VolumeId: v.VolumeId,
                    VolumeType: v.VolumeType,
                    VolumeStatus: v.VolumeStatus,
                    VolumeSizeInBytes: v.VolumeSizeInBytes,
                    VolumeProgress: v.VolumeProgress,
                    SourceSnapshotId: v.SourceSnapshotId)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe cached iSCSI volumes");
        }
    }

    /// <summary>
    /// List volumes for a gateway.
    /// </summary>
    public static async Task<List<StorageGatewayVolumeInfo>>
        ListVolumesAsync(
            string? gatewayARN = null,
            string? marker = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVolumesRequest();
        if (gatewayARN != null) request.GatewayARN = gatewayARN;
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListVolumesAsync(request);
            return resp.VolumeInfos.Select(v => new StorageGatewayVolumeInfo(
                VolumeARN: v.VolumeARN,
                VolumeId: v.VolumeId,
                VolumeType: v.VolumeType,
                VolumeSizeInBytes: v.VolumeSizeInBytes,
                GatewayARN: v.GatewayARN,
                GatewayId: v.GatewayId)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list volumes");
        }
    }

    // ── Snapshot operations ──────────────────────────────────────────

    /// <summary>
    /// Create a snapshot of a volume.
    /// </summary>
    public static async Task<StorageGatewaySnapshotResult>
        CreateSnapshotAsync(
            string volumeARN,
            string snapshotDescription,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSnapshotRequest
        {
            VolumeARN = volumeARN,
            SnapshotDescription = snapshotDescription
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            return new StorageGatewaySnapshotResult(
                SnapshotId: resp.SnapshotId,
                VolumeARN: resp.VolumeARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create snapshot for volume '{volumeARN}'");
        }
    }

    /// <summary>
    /// Delete a snapshot schedule.
    /// </summary>
    public static async Task<string?> DeleteSnapshotScheduleAsync(
        string volumeARN,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSnapshotScheduleAsync(
                new DeleteSnapshotScheduleRequest { VolumeARN = volumeARN });
            return resp.VolumeARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot schedule for '{volumeARN}'");
        }
    }

    /// <summary>
    /// Describe a snapshot schedule.
    /// </summary>
    public static async Task<StorageGatewaySnapshotScheduleInfo>
        DescribeSnapshotScheduleAsync(
            string volumeARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSnapshotScheduleAsync(
                new DescribeSnapshotScheduleRequest { VolumeARN = volumeARN });
            return new StorageGatewaySnapshotScheduleInfo(
                VolumeARN: resp.VolumeARN,
                StartAt: resp.StartAt,
                RecurrenceInHours: resp.RecurrenceInHours,
                Description: resp.Description,
                Timezone: resp.Timezone);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe snapshot schedule for '{volumeARN}'");
        }
    }

    /// <summary>
    /// Update a snapshot schedule.
    /// </summary>
    public static async Task<string?> UpdateSnapshotScheduleAsync(
        UpdateSnapshotScheduleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateSnapshotScheduleAsync(request);
            return resp.VolumeARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update snapshot schedule");
        }
    }

    // ── NFS File Share operations ────────────────────────────────────

    /// <summary>
    /// Create an NFS file share.
    /// </summary>
    public static async Task<string?> CreateNFSFileShareAsync(
        CreateNFSFileShareRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateNFSFileShareAsync(request);
            return resp.FileShareARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create NFS file share");
        }
    }

    /// <summary>
    /// Delete a file share.
    /// </summary>
    public static async Task<string?> DeleteFileShareAsync(
        string fileShareARN,
        bool? forceDelete = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteFileShareRequest
        {
            FileShareARN = fileShareARN
        };
        if (forceDelete.HasValue) request.ForceDelete = forceDelete.Value;

        try
        {
            var resp = await client.DeleteFileShareAsync(request);
            return resp.FileShareARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete file share '{fileShareARN}'");
        }
    }

    /// <summary>
    /// Describe NFS file shares.
    /// </summary>
    public static async Task<List<StorageGatewayNFSFileShareInfo>>
        DescribeNFSFileSharesAsync(
            List<string> fileShareARNList,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeNFSFileSharesAsync(
                new DescribeNFSFileSharesRequest
                {
                    FileShareARNList = fileShareARNList
                });
            return resp.NFSFileShareInfoList.Select(f =>
                new StorageGatewayNFSFileShareInfo(
                    FileShareARN: f.FileShareARN,
                    FileShareId: f.FileShareId,
                    FileShareStatus: f.FileShareStatus,
                    GatewayARN: f.GatewayARN,
                    LocationARN: f.LocationARN,
                    Role: f.Role,
                    Path: f.Path,
                    DefaultStorageClass: f.DefaultStorageClass,
                    Squash: f.Squash)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe NFS file shares");
        }
    }

    /// <summary>
    /// Update an NFS file share.
    /// </summary>
    public static async Task<string?> UpdateNFSFileShareAsync(
        UpdateNFSFileShareRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateNFSFileShareAsync(request);
            return resp.FileShareARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update NFS file share");
        }
    }

    // ── SMB File Share operations ────────────────────────────────────

    /// <summary>
    /// Create an SMB file share.
    /// </summary>
    public static async Task<string?> CreateSMBFileShareAsync(
        CreateSMBFileShareRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSMBFileShareAsync(request);
            return resp.FileShareARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create SMB file share");
        }
    }

    /// <summary>
    /// Describe SMB file shares.
    /// </summary>
    public static async Task<List<StorageGatewaySMBFileShareInfo>>
        DescribeSMBFileSharesAsync(
            List<string> fileShareARNList,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSMBFileSharesAsync(
                new DescribeSMBFileSharesRequest
                {
                    FileShareARNList = fileShareARNList
                });
            return resp.SMBFileShareInfoList.Select(f =>
                new StorageGatewaySMBFileShareInfo(
                    FileShareARN: f.FileShareARN,
                    FileShareId: f.FileShareId,
                    FileShareStatus: f.FileShareStatus,
                    GatewayARN: f.GatewayARN,
                    LocationARN: f.LocationARN,
                    Role: f.Role,
                    Path: f.Path,
                    DefaultStorageClass: f.DefaultStorageClass,
                    SMBACLEnabled: f.SMBACLEnabled)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe SMB file shares");
        }
    }

    /// <summary>
    /// Update an SMB file share.
    /// </summary>
    public static async Task<string?> UpdateSMBFileShareAsync(
        UpdateSMBFileShareRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateSMBFileShareAsync(request);
            return resp.FileShareARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update SMB file share");
        }
    }

    // ── Tape operations ──────────────────────────────────────────────

    /// <summary>
    /// Create a tape with a barcode.
    /// </summary>
    public static async Task<StorageGatewayCreateTapeResult>
        CreateTapeWithBarcodeAsync(
            CreateTapeWithBarcodeRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTapeWithBarcodeAsync(request);
            return new StorageGatewayCreateTapeResult(
                TapeARN: resp.TapeARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create tape with barcode");
        }
    }

    /// <summary>
    /// Delete a tape.
    /// </summary>
    public static async Task<string?> DeleteTapeAsync(
        string gatewayARN,
        string tapeARN,
        bool? bypassGovernanceRetention = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteTapeRequest
        {
            GatewayARN = gatewayARN,
            TapeARN = tapeARN
        };
        if (bypassGovernanceRetention.HasValue)
            request.BypassGovernanceRetention = bypassGovernanceRetention.Value;

        try
        {
            var resp = await client.DeleteTapeAsync(request);
            return resp.TapeARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete tape '{tapeARN}'");
        }
    }

    /// <summary>
    /// Describe tapes.
    /// </summary>
    public static async Task<List<StorageGatewayTapeInfo>>
        DescribeTapesAsync(
            string gatewayARN,
            List<string>? tapeARNs = null,
            string? marker = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTapesRequest { GatewayARN = gatewayARN };
        if (tapeARNs != null) request.TapeARNs = tapeARNs;
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.DescribeTapesAsync(request);
            return resp.Tapes.Select(t => new StorageGatewayTapeInfo(
                TapeARN: t.TapeARN,
                TapeBarcode: t.TapeBarcode,
                TapeStatus: t.TapeStatus,
                TapeSizeInBytes: t.TapeSizeInBytes,
                PoolId: t.PoolId)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe tapes for gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// List tapes.
    /// </summary>
    public static async Task<List<StorageGatewayTapeInfo>> ListTapesAsync(
        List<string>? tapeARNs = null,
        string? marker = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTapesRequest();
        if (tapeARNs != null) request.TapeARNs = tapeARNs;
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTapesAsync(request);
            return resp.TapeInfos.Select(t => new StorageGatewayTapeInfo(
                TapeARN: t.TapeARN,
                TapeBarcode: t.TapeBarcode,
                TapeStatus: t.TapeStatus,
                TapeSizeInBytes: t.TapeSizeInBytes,
                PoolId: t.PoolId,
                GatewayARN: t.GatewayARN)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tapes");
        }
    }

    // ── Tape Pool operations ─────────────────────────────────────────

    /// <summary>
    /// Create a tape pool.
    /// </summary>
    public static async Task<string?> CreateTapePoolAsync(
        CreateTapePoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTapePoolAsync(request);
            return resp.PoolARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create tape pool");
        }
    }

    /// <summary>
    /// Delete a tape pool.
    /// </summary>
    public static async Task<string?> DeleteTapePoolAsync(
        string poolARN,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteTapePoolAsync(new DeleteTapePoolRequest
            {
                PoolARN = poolARN
            });
            return resp.PoolARN;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete tape pool '{poolARN}'");
        }
    }

    /// <summary>
    /// List tape pools.
    /// </summary>
    public static async Task<List<StorageGatewayTapePoolInfo>>
        ListTapePoolsAsync(
            List<string>? poolARNs = null,
            string? marker = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTapePoolsRequest();
        if (poolARNs != null) request.PoolARNs = poolARNs;
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTapePoolsAsync(request);
            return resp.PoolInfos.Select(p => new StorageGatewayTapePoolInfo(
                PoolARN: p.PoolARN,
                PoolName: p.PoolName,
                StorageClass: p.StorageClass?.Value,
                PoolStatus: p.PoolStatus?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tape pools");
        }
    }

    // ── Cache & Working Storage ──────────────────────────────────────

    /// <summary>
    /// Add cache disks to a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult> AddCacheAsync(
        string gatewayARN,
        List<string> diskIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddCacheAsync(new AddCacheRequest
            {
                GatewayARN = gatewayARN,
                DiskIds = diskIds
            });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add cache to gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Describe cache for a gateway.
    /// </summary>
    public static async Task<StorageGatewayCacheInfo> DescribeCacheAsync(
        string gatewayARN,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCacheAsync(new DescribeCacheRequest
            {
                GatewayARN = gatewayARN
            });
            return new StorageGatewayCacheInfo(
                GatewayARN: resp.GatewayARN,
                DiskIds: resp.DiskIds,
                CacheAllocatedInBytes: resp.CacheAllocatedInBytes,
                CacheUsedPercentage: resp.CacheUsedPercentage,
                CacheDirtyPercentage: resp.CacheDirtyPercentage,
                CacheHitPercentage: resp.CacheHitPercentage,
                CacheMissPercentage: resp.CacheMissPercentage);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe cache for gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Add working storage disks to a gateway.
    /// </summary>
    public static async Task<StorageGatewayActivationResult>
        AddWorkingStorageAsync(
            string gatewayARN,
            List<string> diskIds,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddWorkingStorageAsync(
                new AddWorkingStorageRequest
                {
                    GatewayARN = gatewayARN,
                    DiskIds = diskIds
                });
            return new StorageGatewayActivationResult(
                GatewayARN: resp.GatewayARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add working storage to gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// Describe working storage for a gateway.
    /// </summary>
    public static async Task<StorageGatewayWorkingStorageInfo>
        DescribeWorkingStorageAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeWorkingStorageAsync(
                new DescribeWorkingStorageRequest
                {
                    GatewayARN = gatewayARN
                });
            return new StorageGatewayWorkingStorageInfo(
                GatewayARN: resp.GatewayARN,
                DiskIds: resp.DiskIds,
                WorkingStorageAllocatedInBytes:
                    resp.WorkingStorageAllocatedInBytes,
                WorkingStorageUsedInBytes: resp.WorkingStorageUsedInBytes);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe working storage for gateway '{gatewayARN}'");
        }
    }

    /// <summary>
    /// List local disks for a gateway.
    /// </summary>
    public static async Task<List<StorageGatewayLocalDiskInfo>>
        ListLocalDisksAsync(
            string gatewayARN,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListLocalDisksAsync(
                new ListLocalDisksRequest { GatewayARN = gatewayARN });
            return resp.Disks.Select(d => new StorageGatewayLocalDiskInfo(
                DiskId: d.DiskId,
                DiskPath: d.DiskPath,
                DiskNode: d.DiskNode,
                DiskStatus: d.DiskStatus,
                DiskSizeInBytes: d.DiskSizeInBytes,
                DiskAllocationType: d.DiskAllocationType,
                DiskAllocationResource: d.DiskAllocationResource)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list local disks for gateway '{gatewayARN}'");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Add tags to a Storage Gateway resource.
    /// </summary>
    public static async Task<StorageGatewayTagResult> AddTagsToResourceAsync(
        string resourceARN,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToResourceAsync(new AddTagsToResourceRequest
            {
                ResourceARN = resourceARN,
                Tags = tags
            });
            return new StorageGatewayTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add tags to resource '{resourceARN}'");
        }
    }

    /// <summary>
    /// Remove tags from a Storage Gateway resource.
    /// </summary>
    public static async Task<StorageGatewayTagResult>
        RemoveTagsFromResourceAsync(
            string resourceARN,
            List<string> tagKeys,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromResourceAsync(
                new RemoveTagsFromResourceRequest
                {
                    ResourceARN = resourceARN,
                    TagKeys = tagKeys
                });
            return new StorageGatewayTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove tags from resource '{resourceARN}'");
        }
    }

    /// <summary>
    /// List tags for a Storage Gateway resource.
    /// </summary>
    public static async Task<StorageGatewayListTagsResult>
        ListTagsForResourceAsync(
            string resourceARN,
            string? marker = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceARN = resourceARN
        };
        if (marker != null) request.Marker = marker;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new StorageGatewayListTagsResult(
                Tags: resp.Tags,
                Marker: resp.Marker);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceARN}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="ActivateGatewayAsync"/>.</summary>
    public static StorageGatewayActivationResult ActivateGateway(ActivateGatewayRequest request, RegionEndpoint? region = null)
        => ActivateGatewayAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGatewayAsync"/>.</summary>
    public static StorageGatewayActivationResult DeleteGateway(string gatewayARN, RegionEndpoint? region = null)
        => DeleteGatewayAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeGatewayInformationAsync"/>.</summary>
    public static StorageGatewayInfo DescribeGatewayInformation(string gatewayARN, RegionEndpoint? region = null)
        => DescribeGatewayInformationAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGatewaysAsync"/>.</summary>
    public static List<StorageGatewayInfo> ListGateways(string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => ListGatewaysAsync(marker, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ShutdownGatewayAsync"/>.</summary>
    public static StorageGatewayActivationResult ShutdownGateway(string gatewayARN, RegionEndpoint? region = null)
        => ShutdownGatewayAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartGatewayAsync"/>.</summary>
    public static StorageGatewayActivationResult StartGateway(string gatewayARN, RegionEndpoint? region = null)
        => StartGatewayAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateGatewayInformationAsync"/>.</summary>
    public static StorageGatewayActivationResult UpdateGatewayInformation(UpdateGatewayInformationRequest request, RegionEndpoint? region = null)
        => UpdateGatewayInformationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateGatewaySoftwareNowAsync"/>.</summary>
    public static StorageGatewayActivationResult UpdateGatewaySoftwareNow(string gatewayARN, RegionEndpoint? region = null)
        => UpdateGatewaySoftwareNowAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCachediSCSIVolumeAsync"/>.</summary>
    public static StorageGatewayCreateVolumeResult CreateCachediSCSIVolume(CreateCachediSCSIVolumeRequest request, RegionEndpoint? region = null)
        => CreateCachediSCSIVolumeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateStorediSCSIVolumeAsync"/>.</summary>
    public static StorageGatewayCreateVolumeResult CreateStorediSCSIVolume(CreateStorediSCSIVolumeRequest request, RegionEndpoint? region = null)
        => CreateStorediSCSIVolumeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteVolumeAsync"/>.</summary>
    public static string? DeleteVolume(string volumeARN, RegionEndpoint? region = null)
        => DeleteVolumeAsync(volumeARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStorediSCSIVolumesAsync"/>.</summary>
    public static List<StorageGatewayStorediSCSIVolumeInfo> DescribeStorediSCSIVolumes(List<string> volumeARNs, RegionEndpoint? region = null)
        => DescribeStorediSCSIVolumesAsync(volumeARNs, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCachediSCSIVolumesAsync"/>.</summary>
    public static List<StorageGatewayCachediSCSIVolumeInfo> DescribeCachediSCSIVolumes(List<string> volumeARNs, RegionEndpoint? region = null)
        => DescribeCachediSCSIVolumesAsync(volumeARNs, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListVolumesAsync"/>.</summary>
    public static List<StorageGatewayVolumeInfo> ListVolumes(string? gatewayARN = null, string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => ListVolumesAsync(gatewayARN, marker, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSnapshotAsync"/>.</summary>
    public static StorageGatewaySnapshotResult CreateSnapshot(string volumeARN, string snapshotDescription, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateSnapshotAsync(volumeARN, snapshotDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSnapshotScheduleAsync"/>.</summary>
    public static string? DeleteSnapshotSchedule(string volumeARN, RegionEndpoint? region = null)
        => DeleteSnapshotScheduleAsync(volumeARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSnapshotScheduleAsync"/>.</summary>
    public static StorageGatewaySnapshotScheduleInfo DescribeSnapshotSchedule(string volumeARN, RegionEndpoint? region = null)
        => DescribeSnapshotScheduleAsync(volumeARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateSnapshotScheduleAsync"/>.</summary>
    public static string? UpdateSnapshotSchedule(UpdateSnapshotScheduleRequest request, RegionEndpoint? region = null)
        => UpdateSnapshotScheduleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateNFSFileShareAsync"/>.</summary>
    public static string? CreateNFSFileShare(CreateNFSFileShareRequest request, RegionEndpoint? region = null)
        => CreateNFSFileShareAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFileShareAsync"/>.</summary>
    public static string? DeleteFileShare(string fileShareARN, bool? forceDelete = null, RegionEndpoint? region = null)
        => DeleteFileShareAsync(fileShareARN, forceDelete, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeNFSFileSharesAsync"/>.</summary>
    public static List<StorageGatewayNFSFileShareInfo> DescribeNFSFileShares(List<string> fileShareARNList, RegionEndpoint? region = null)
        => DescribeNFSFileSharesAsync(fileShareARNList, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateNFSFileShareAsync"/>.</summary>
    public static string? UpdateNFSFileShare(UpdateNFSFileShareRequest request, RegionEndpoint? region = null)
        => UpdateNFSFileShareAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSMBFileShareAsync"/>.</summary>
    public static string? CreateSMBFileShare(CreateSMBFileShareRequest request, RegionEndpoint? region = null)
        => CreateSMBFileShareAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSMBFileSharesAsync"/>.</summary>
    public static List<StorageGatewaySMBFileShareInfo> DescribeSMBFileShares(List<string> fileShareARNList, RegionEndpoint? region = null)
        => DescribeSMBFileSharesAsync(fileShareARNList, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateSMBFileShareAsync"/>.</summary>
    public static string? UpdateSMBFileShare(UpdateSMBFileShareRequest request, RegionEndpoint? region = null)
        => UpdateSMBFileShareAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTapeWithBarcodeAsync"/>.</summary>
    public static StorageGatewayCreateTapeResult CreateTapeWithBarcode(CreateTapeWithBarcodeRequest request, RegionEndpoint? region = null)
        => CreateTapeWithBarcodeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTapeAsync"/>.</summary>
    public static string? DeleteTape(string gatewayARN, string tapeARN, bool? bypassGovernanceRetention = null, RegionEndpoint? region = null)
        => DeleteTapeAsync(gatewayARN, tapeARN, bypassGovernanceRetention, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTapesAsync"/>.</summary>
    public static List<StorageGatewayTapeInfo> DescribeTapes(string gatewayARN, List<string>? tapeARNs = null, string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => DescribeTapesAsync(gatewayARN, tapeARNs, marker, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTapesAsync"/>.</summary>
    public static List<StorageGatewayTapeInfo> ListTapes(List<string>? tapeARNs = null, string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => ListTapesAsync(tapeARNs, marker, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTapePoolAsync"/>.</summary>
    public static string? CreateTapePool(CreateTapePoolRequest request, RegionEndpoint? region = null)
        => CreateTapePoolAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTapePoolAsync"/>.</summary>
    public static string? DeleteTapePool(string poolARN, RegionEndpoint? region = null)
        => DeleteTapePoolAsync(poolARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTapePoolsAsync"/>.</summary>
    public static List<StorageGatewayTapePoolInfo> ListTapePools(List<string>? poolARNs = null, string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => ListTapePoolsAsync(poolARNs, marker, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddCacheAsync"/>.</summary>
    public static StorageGatewayActivationResult AddCache(string gatewayARN, List<string> diskIds, RegionEndpoint? region = null)
        => AddCacheAsync(gatewayARN, diskIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCacheAsync"/>.</summary>
    public static StorageGatewayCacheInfo DescribeCache(string gatewayARN, RegionEndpoint? region = null)
        => DescribeCacheAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddWorkingStorageAsync"/>.</summary>
    public static StorageGatewayActivationResult AddWorkingStorage(string gatewayARN, List<string> diskIds, RegionEndpoint? region = null)
        => AddWorkingStorageAsync(gatewayARN, diskIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeWorkingStorageAsync"/>.</summary>
    public static StorageGatewayWorkingStorageInfo DescribeWorkingStorage(string gatewayARN, RegionEndpoint? region = null)
        => DescribeWorkingStorageAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListLocalDisksAsync"/>.</summary>
    public static List<StorageGatewayLocalDiskInfo> ListLocalDisks(string gatewayARN, RegionEndpoint? region = null)
        => ListLocalDisksAsync(gatewayARN, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToResourceAsync"/>.</summary>
    public static StorageGatewayTagResult AddTagsToResource(string resourceARN, List<Tag> tags, RegionEndpoint? region = null)
        => AddTagsToResourceAsync(resourceARN, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromResourceAsync"/>.</summary>
    public static StorageGatewayTagResult RemoveTagsFromResource(string resourceARN, List<string> tagKeys, RegionEndpoint? region = null)
        => RemoveTagsFromResourceAsync(resourceARN, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static StorageGatewayListTagsResult ListTagsForResource(string resourceARN, string? marker = null, int? limit = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceARN, marker, limit, region).GetAwaiter().GetResult();

}
