using Amazon;
using Amazon.FSx;
using Amazon.FSx.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record FsxFileSystemInfo(
    string? FileSystemId = null,
    string? FileSystemType = null,
    string? Lifecycle = null,
    int? StorageCapacity = null,
    string? StorageType = null,
    string? VpcId = null,
    string? DNSName = null,
    string? ResourceARN = null,
    string? OwnerId = null,
    DateTime? CreationTime = null);

public sealed record FsxDataRepositoryTaskInfo(
    string? TaskId = null,
    string? FileSystemId = null,
    string? Lifecycle = null,
    string? Type = null,
    string? ResourceARN = null,
    DateTime? CreationTime = null,
    DateTime? StartTime = null,
    DateTime? EndTime = null);

public sealed record FsxDataRepositoryAssociationInfo(
    string? AssociationId = null,
    string? FileSystemId = null,
    string? FileSystemPath = null,
    string? DataRepositoryPath = null,
    string? Lifecycle = null,
    string? ResourceARN = null);

public sealed record FsxBackupInfo(
    string? BackupId = null,
    string? Lifecycle = null,
    string? Type = null,
    string? FileSystemId = null,
    string? ResourceARN = null,
    DateTime? CreationTime = null);

public sealed record FsxSnapshotInfo(
    string? SnapshotId = null,
    string? Name = null,
    string? Lifecycle = null,
    string? VolumeId = null,
    string? ResourceARN = null,
    DateTime? CreationTime = null);

public sealed record FsxStorageVirtualMachineInfo(
    string? StorageVirtualMachineId = null,
    string? Name = null,
    string? FileSystemId = null,
    string? Lifecycle = null,
    string? ResourceARN = null,
    DateTime? CreationTime = null);

public sealed record FsxVolumeInfo(
    string? VolumeId = null,
    string? Name = null,
    string? VolumeType = null,
    string? Lifecycle = null,
    string? FileSystemId = null,
    string? ResourceARN = null,
    DateTime? CreationTime = null);

public sealed record FsxFileCacheInfo(
    string? FileCacheId = null,
    string? FileCacheType = null,
    string? Lifecycle = null,
    int? StorageCapacity = null,
    string? VpcId = null,
    string? DNSName = null,
    string? ResourceARN = null,
    string? OwnerId = null,
    DateTime? CreationTime = null);

public sealed record FsxDeleteResult(
    string? FileSystemId = null,
    string? Lifecycle = null);

public sealed record FsxTagResult(bool Success = true);

public sealed record FsxListTagsResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

/// <summary>
/// Utility helpers for Amazon FSx.
/// </summary>
public static class FsxService
{
    private static AmazonFSxClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonFSxClient>(region);

    // ── File System operations ───────────────────────────────────────

    /// <summary>
    /// Create a file system.
    /// </summary>
    public static async Task<FsxFileSystemInfo> CreateFileSystemAsync(
        CreateFileSystemRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateFileSystemAsync(request);
            var fs = resp.FileSystem;
            return new FsxFileSystemInfo(
                FileSystemId: fs.FileSystemId,
                FileSystemType: fs.FileSystemType?.Value,
                Lifecycle: fs.Lifecycle?.Value,
                StorageCapacity: fs.StorageCapacity,
                StorageType: fs.StorageType?.Value,
                VpcId: fs.VpcId,
                DNSName: fs.DNSName,
                ResourceARN: fs.ResourceARN,
                OwnerId: fs.OwnerId,
                CreationTime: fs.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create FSx file system");
        }
    }

    /// <summary>
    /// Delete a file system.
    /// </summary>
    public static async Task<FsxDeleteResult> DeleteFileSystemAsync(
        DeleteFileSystemRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteFileSystemAsync(request);
            return new FsxDeleteResult(
                FileSystemId: resp.FileSystemId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete FSx file system");
        }
    }

    /// <summary>
    /// Describe file systems.
    /// </summary>
    public static async Task<List<FsxFileSystemInfo>>
        DescribeFileSystemsAsync(
            List<string>? fileSystemIds = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeFileSystemsRequest();
        if (fileSystemIds != null) request.FileSystemIds = fileSystemIds;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeFileSystemsAsync(request);
            return resp.FileSystems.Select(fs => new FsxFileSystemInfo(
                FileSystemId: fs.FileSystemId,
                FileSystemType: fs.FileSystemType?.Value,
                Lifecycle: fs.Lifecycle?.Value,
                StorageCapacity: fs.StorageCapacity,
                StorageType: fs.StorageType?.Value,
                VpcId: fs.VpcId,
                DNSName: fs.DNSName,
                ResourceARN: fs.ResourceARN,
                OwnerId: fs.OwnerId,
                CreationTime: fs.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe FSx file systems");
        }
    }

    /// <summary>
    /// Update a file system.
    /// </summary>
    public static async Task<FsxFileSystemInfo> UpdateFileSystemAsync(
        UpdateFileSystemRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateFileSystemAsync(request);
            var fs = resp.FileSystem;
            return new FsxFileSystemInfo(
                FileSystemId: fs.FileSystemId,
                FileSystemType: fs.FileSystemType?.Value,
                Lifecycle: fs.Lifecycle?.Value,
                StorageCapacity: fs.StorageCapacity,
                StorageType: fs.StorageType?.Value,
                VpcId: fs.VpcId,
                DNSName: fs.DNSName,
                ResourceARN: fs.ResourceARN,
                OwnerId: fs.OwnerId,
                CreationTime: fs.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update FSx file system");
        }
    }

    // ── Data Repository Task operations ──────────────────────────────

    /// <summary>
    /// Create a data repository task.
    /// </summary>
    public static async Task<FsxDataRepositoryTaskInfo>
        CreateDataRepositoryTaskAsync(
            CreateDataRepositoryTaskRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDataRepositoryTaskAsync(request);
            var t = resp.DataRepositoryTask;
            return new FsxDataRepositoryTaskInfo(
                TaskId: t.TaskId,
                FileSystemId: t.FileSystemId,
                Lifecycle: t.Lifecycle?.Value,
                Type: t.Type?.Value,
                ResourceARN: t.ResourceARN,
                CreationTime: t.CreationTime,
                StartTime: t.StartTime,
                EndTime: t.EndTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create data repository task");
        }
    }

    /// <summary>
    /// Describe data repository tasks.
    /// </summary>
    public static async Task<List<FsxDataRepositoryTaskInfo>>
        DescribeDataRepositoryTasksAsync(
            List<string>? taskIds = null,
            List<DataRepositoryTaskFilter>? filters = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDataRepositoryTasksRequest();
        if (taskIds != null) request.TaskIds = taskIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeDataRepositoryTasksAsync(request);
            return resp.DataRepositoryTasks.Select(t =>
                new FsxDataRepositoryTaskInfo(
                    TaskId: t.TaskId,
                    FileSystemId: t.FileSystemId,
                    Lifecycle: t.Lifecycle?.Value,
                    Type: t.Type?.Value,
                    ResourceARN: t.ResourceARN,
                    CreationTime: t.CreationTime,
                    StartTime: t.StartTime,
                    EndTime: t.EndTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe data repository tasks");
        }
    }

    /// <summary>
    /// Cancel a data repository task.
    /// </summary>
    public static async Task<FsxDataRepositoryTaskInfo>
        CancelDataRepositoryTaskAsync(
            string taskId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelDataRepositoryTaskAsync(
                new CancelDataRepositoryTaskRequest { TaskId = taskId });
            return new FsxDataRepositoryTaskInfo(
                TaskId: resp.TaskId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel data repository task '{taskId}'");
        }
    }

    // ── Data Repository Association operations ───────────────────────

    /// <summary>
    /// Create a data repository association.
    /// </summary>
    public static async Task<FsxDataRepositoryAssociationInfo>
        CreateDataRepositoryAssociationAsync(
            CreateDataRepositoryAssociationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDataRepositoryAssociationAsync(
                request);
            var a = resp.Association;
            return new FsxDataRepositoryAssociationInfo(
                AssociationId: a.AssociationId,
                FileSystemId: a.FileSystemId,
                FileSystemPath: a.FileSystemPath,
                DataRepositoryPath: a.DataRepositoryPath,
                Lifecycle: a.Lifecycle?.Value,
                ResourceARN: a.ResourceARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create data repository association");
        }
    }

    /// <summary>
    /// Delete a data repository association.
    /// </summary>
    public static async Task<FsxDataRepositoryAssociationInfo>
        DeleteDataRepositoryAssociationAsync(
            DeleteDataRepositoryAssociationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDataRepositoryAssociationAsync(
                request);
            return new FsxDataRepositoryAssociationInfo(
                AssociationId: resp.AssociationId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete data repository association");
        }
    }

    /// <summary>
    /// Describe data repository associations.
    /// </summary>
    public static async Task<List<FsxDataRepositoryAssociationInfo>>
        DescribeDataRepositoryAssociationsAsync(
            List<string>? associationIds = null,
            List<Filter>? filters = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDataRepositoryAssociationsRequest();
        if (associationIds != null) request.AssociationIds = associationIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeDataRepositoryAssociationsAsync(
                request);
            return resp.Associations.Select(a =>
                new FsxDataRepositoryAssociationInfo(
                    AssociationId: a.AssociationId,
                    FileSystemId: a.FileSystemId,
                    FileSystemPath: a.FileSystemPath,
                    DataRepositoryPath: a.DataRepositoryPath,
                    Lifecycle: a.Lifecycle?.Value,
                    ResourceARN: a.ResourceARN)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe data repository associations");
        }
    }

    /// <summary>
    /// Update a data repository association.
    /// </summary>
    public static async Task<FsxDataRepositoryAssociationInfo>
        UpdateDataRepositoryAssociationAsync(
            UpdateDataRepositoryAssociationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDataRepositoryAssociationAsync(
                request);
            var a = resp.Association;
            return new FsxDataRepositoryAssociationInfo(
                AssociationId: a.AssociationId,
                FileSystemId: a.FileSystemId,
                FileSystemPath: a.FileSystemPath,
                DataRepositoryPath: a.DataRepositoryPath,
                Lifecycle: a.Lifecycle?.Value,
                ResourceARN: a.ResourceARN);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update data repository association");
        }
    }

    // ── Backup operations ────────────────────────────────────────────

    /// <summary>
    /// Create a backup.
    /// </summary>
    public static async Task<FsxBackupInfo> CreateBackupAsync(
        CreateBackupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateBackupAsync(request);
            var b = resp.Backup;
            return new FsxBackupInfo(
                BackupId: b.BackupId,
                Lifecycle: b.Lifecycle?.Value,
                Type: b.Type?.Value,
                FileSystemId: b.FileSystem?.FileSystemId,
                ResourceARN: b.ResourceARN,
                CreationTime: b.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create FSx backup");
        }
    }

    /// <summary>
    /// Delete a backup.
    /// </summary>
    public static async Task<FsxBackupInfo> DeleteBackupAsync(
        string backupId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteBackupAsync(new DeleteBackupRequest
            {
                BackupId = backupId
            });
            return new FsxBackupInfo(
                BackupId: resp.BackupId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete backup '{backupId}'");
        }
    }

    /// <summary>
    /// Describe backups.
    /// </summary>
    public static async Task<List<FsxBackupInfo>> DescribeBackupsAsync(
        List<string>? backupIds = null,
        List<Filter>? filters = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeBackupsRequest();
        if (backupIds != null) request.BackupIds = backupIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeBackupsAsync(request);
            return resp.Backups.Select(b => new FsxBackupInfo(
                BackupId: b.BackupId,
                Lifecycle: b.Lifecycle?.Value,
                Type: b.Type?.Value,
                FileSystemId: b.FileSystem?.FileSystemId,
                ResourceARN: b.ResourceARN,
                CreationTime: b.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe FSx backups");
        }
    }

    /// <summary>
    /// Copy a backup to another region or account.
    /// </summary>
    public static async Task<FsxBackupInfo> CopyBackupAsync(
        CopyBackupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CopyBackupAsync(request);
            var b = resp.Backup;
            return new FsxBackupInfo(
                BackupId: b.BackupId,
                Lifecycle: b.Lifecycle?.Value,
                Type: b.Type?.Value,
                FileSystemId: b.FileSystem?.FileSystemId,
                ResourceARN: b.ResourceARN,
                CreationTime: b.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to copy FSx backup");
        }
    }

    // ── Snapshot operations ──────────────────────────────────────────

    /// <summary>
    /// Create a snapshot.
    /// </summary>
    public static async Task<FsxSnapshotInfo> CreateSnapshotAsync(
        CreateSnapshotRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            var s = resp.Snapshot;
            return new FsxSnapshotInfo(
                SnapshotId: s.SnapshotId,
                Name: s.Name,
                Lifecycle: s.Lifecycle?.Value,
                VolumeId: s.VolumeId,
                ResourceARN: s.ResourceARN,
                CreationTime: s.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create FSx snapshot");
        }
    }

    /// <summary>
    /// Delete a snapshot.
    /// </summary>
    public static async Task<FsxSnapshotInfo> DeleteSnapshotAsync(
        string snapshotId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSnapshotAsync(
                new DeleteSnapshotRequest { SnapshotId = snapshotId });
            return new FsxSnapshotInfo(
                SnapshotId: resp.SnapshotId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot '{snapshotId}'");
        }
    }

    /// <summary>
    /// Describe snapshots.
    /// </summary>
    public static async Task<List<FsxSnapshotInfo>> DescribeSnapshotsAsync(
        List<string>? snapshotIds = null,
        List<SnapshotFilter>? filters = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSnapshotsRequest();
        if (snapshotIds != null) request.SnapshotIds = snapshotIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeSnapshotsAsync(request);
            return resp.Snapshots.Select(s => new FsxSnapshotInfo(
                SnapshotId: s.SnapshotId,
                Name: s.Name,
                Lifecycle: s.Lifecycle?.Value,
                VolumeId: s.VolumeId,
                ResourceARN: s.ResourceARN,
                CreationTime: s.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe FSx snapshots");
        }
    }

    /// <summary>
    /// Update a snapshot.
    /// </summary>
    public static async Task<FsxSnapshotInfo> UpdateSnapshotAsync(
        UpdateSnapshotRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateSnapshotAsync(request);
            var s = resp.Snapshot;
            return new FsxSnapshotInfo(
                SnapshotId: s.SnapshotId,
                Name: s.Name,
                Lifecycle: s.Lifecycle?.Value,
                VolumeId: s.VolumeId,
                ResourceARN: s.ResourceARN,
                CreationTime: s.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update FSx snapshot");
        }
    }

    // ── Storage Virtual Machine operations ───────────────────────────

    /// <summary>
    /// Create a storage virtual machine.
    /// </summary>
    public static async Task<FsxStorageVirtualMachineInfo>
        CreateStorageVirtualMachineAsync(
            CreateStorageVirtualMachineRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStorageVirtualMachineAsync(request);
            var svm = resp.StorageVirtualMachine;
            return new FsxStorageVirtualMachineInfo(
                StorageVirtualMachineId: svm.StorageVirtualMachineId,
                Name: svm.Name,
                FileSystemId: svm.FileSystemId,
                Lifecycle: svm.Lifecycle?.Value,
                ResourceARN: svm.ResourceARN,
                CreationTime: svm.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create storage virtual machine");
        }
    }

    /// <summary>
    /// Delete a storage virtual machine.
    /// </summary>
    public static async Task<FsxStorageVirtualMachineInfo>
        DeleteStorageVirtualMachineAsync(
            string storageVirtualMachineId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteStorageVirtualMachineAsync(
                new DeleteStorageVirtualMachineRequest
                {
                    StorageVirtualMachineId = storageVirtualMachineId
                });
            return new FsxStorageVirtualMachineInfo(
                StorageVirtualMachineId: resp.StorageVirtualMachineId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete storage virtual machine '{storageVirtualMachineId}'");
        }
    }

    /// <summary>
    /// Describe storage virtual machines.
    /// </summary>
    public static async Task<List<FsxStorageVirtualMachineInfo>>
        DescribeStorageVirtualMachinesAsync(
            List<string>? storageVirtualMachineIds = null,
            List<StorageVirtualMachineFilter>? filters = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStorageVirtualMachinesRequest();
        if (storageVirtualMachineIds != null)
            request.StorageVirtualMachineIds = storageVirtualMachineIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeStorageVirtualMachinesAsync(
                request);
            return resp.StorageVirtualMachines.Select(svm =>
                new FsxStorageVirtualMachineInfo(
                    StorageVirtualMachineId: svm.StorageVirtualMachineId,
                    Name: svm.Name,
                    FileSystemId: svm.FileSystemId,
                    Lifecycle: svm.Lifecycle?.Value,
                    ResourceARN: svm.ResourceARN,
                    CreationTime: svm.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe storage virtual machines");
        }
    }

    /// <summary>
    /// Update a storage virtual machine.
    /// </summary>
    public static async Task<FsxStorageVirtualMachineInfo>
        UpdateStorageVirtualMachineAsync(
            UpdateStorageVirtualMachineRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateStorageVirtualMachineAsync(request);
            var svm = resp.StorageVirtualMachine;
            return new FsxStorageVirtualMachineInfo(
                StorageVirtualMachineId: svm.StorageVirtualMachineId,
                Name: svm.Name,
                FileSystemId: svm.FileSystemId,
                Lifecycle: svm.Lifecycle?.Value,
                ResourceARN: svm.ResourceARN,
                CreationTime: svm.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update storage virtual machine");
        }
    }

    // ── Volume operations ────────────────────────────────────────────

    /// <summary>
    /// Create a volume.
    /// </summary>
    public static async Task<FsxVolumeInfo> CreateVolumeAsync(
        CreateVolumeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVolumeAsync(request);
            var v = resp.Volume;
            return new FsxVolumeInfo(
                VolumeId: v.VolumeId,
                Name: v.Name,
                VolumeType: v.VolumeType?.Value,
                Lifecycle: v.Lifecycle?.Value,
                FileSystemId: v.FileSystemId,
                ResourceARN: v.ResourceARN,
                CreationTime: v.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create FSx volume");
        }
    }

    /// <summary>
    /// Delete a volume.
    /// </summary>
    public static async Task<FsxVolumeInfo> DeleteVolumeAsync(
        DeleteVolumeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteVolumeAsync(request);
            return new FsxVolumeInfo(
                VolumeId: resp.VolumeId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete FSx volume");
        }
    }

    /// <summary>
    /// Describe volumes.
    /// </summary>
    public static async Task<List<FsxVolumeInfo>> DescribeVolumesAsync(
        List<string>? volumeIds = null,
        List<VolumeFilter>? filters = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeVolumesRequest();
        if (volumeIds != null) request.VolumeIds = volumeIds;
        if (filters != null) request.Filters = filters;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeVolumesAsync(request);
            return resp.Volumes.Select(v => new FsxVolumeInfo(
                VolumeId: v.VolumeId,
                Name: v.Name,
                VolumeType: v.VolumeType?.Value,
                Lifecycle: v.Lifecycle?.Value,
                FileSystemId: v.FileSystemId,
                ResourceARN: v.ResourceARN,
                CreationTime: v.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe FSx volumes");
        }
    }

    /// <summary>
    /// Update a volume.
    /// </summary>
    public static async Task<FsxVolumeInfo> UpdateVolumeAsync(
        UpdateVolumeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateVolumeAsync(request);
            var v = resp.Volume;
            return new FsxVolumeInfo(
                VolumeId: v.VolumeId,
                Name: v.Name,
                VolumeType: v.VolumeType?.Value,
                Lifecycle: v.Lifecycle?.Value,
                FileSystemId: v.FileSystemId,
                ResourceARN: v.ResourceARN,
                CreationTime: v.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update FSx volume");
        }
    }

    /// <summary>
    /// Create a volume from a backup.
    /// </summary>
    public static async Task<FsxVolumeInfo> CreateVolumeFromBackupAsync(
        CreateVolumeFromBackupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVolumeFromBackupAsync(request);
            var v = resp.Volume;
            return new FsxVolumeInfo(
                VolumeId: v.VolumeId,
                Name: v.Name,
                VolumeType: v.VolumeType?.Value,
                Lifecycle: v.Lifecycle?.Value,
                FileSystemId: v.FileSystemId,
                ResourceARN: v.ResourceARN,
                CreationTime: v.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create volume from backup");
        }
    }

    // ── File Cache operations ────────────────────────────────────────

    /// <summary>
    /// Create a file cache.
    /// </summary>
    public static async Task<FsxFileCacheInfo> CreateFileCacheAsync(
        CreateFileCacheRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateFileCacheAsync(request);
            var fc = resp.FileCache;
            return new FsxFileCacheInfo(
                FileCacheId: fc.FileCacheId,
                FileCacheType: fc.FileCacheType?.Value,
                Lifecycle: fc.Lifecycle?.Value,
                StorageCapacity: fc.StorageCapacity,
                VpcId: fc.VpcId,
                DNSName: fc.DNSName,
                ResourceARN: fc.ResourceARN,
                OwnerId: fc.OwnerId,
                CreationTime: fc.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create file cache");
        }
    }

    /// <summary>
    /// Delete a file cache.
    /// </summary>
    public static async Task<FsxFileCacheInfo> DeleteFileCacheAsync(
        string fileCacheId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteFileCacheAsync(
                new DeleteFileCacheRequest { FileCacheId = fileCacheId });
            return new FsxFileCacheInfo(
                FileCacheId: resp.FileCacheId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete file cache '{fileCacheId}'");
        }
    }

    /// <summary>
    /// Describe file caches.
    /// </summary>
    public static async Task<List<FsxFileCacheInfo>>
        DescribeFileCachesAsync(
            List<string>? fileCacheIds = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeFileCachesRequest();
        if (fileCacheIds != null) request.FileCacheIds = fileCacheIds;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeFileCachesAsync(request);
            return resp.FileCaches.Select(fc => new FsxFileCacheInfo(
                FileCacheId: fc.FileCacheId,
                FileCacheType: fc.FileCacheType?.Value,
                Lifecycle: fc.Lifecycle?.Value,
                StorageCapacity: fc.StorageCapacity,
                VpcId: fc.VpcId,
                DNSName: fc.DNSName,
                ResourceARN: fc.ResourceARN,
                OwnerId: fc.OwnerId,
                CreationTime: fc.CreationTime)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe file caches");
        }
    }

    /// <summary>
    /// Update a file cache.
    /// </summary>
    public static async Task<FsxFileCacheInfo> UpdateFileCacheAsync(
        UpdateFileCacheRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateFileCacheAsync(request);
            var fc = resp.FileCache;
            return new FsxFileCacheInfo(
                FileCacheId: fc.FileCacheId,
                FileCacheType: fc.FileCacheType?.Value,
                Lifecycle: fc.Lifecycle?.Value,
                StorageCapacity: fc.StorageCapacity,
                VpcId: fc.VpcId,
                DNSName: fc.DNSName,
                ResourceARN: fc.ResourceARN,
                OwnerId: fc.OwnerId,
                CreationTime: fc.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update file cache");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Tag an FSx resource.
    /// </summary>
    public static async Task<FsxTagResult> TagResourceAsync(
        string resourceARN,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceARN = resourceARN,
                Tags = tags
            });
            return new FsxTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceARN}'");
        }
    }

    /// <summary>
    /// Remove tags from an FSx resource.
    /// </summary>
    public static async Task<FsxTagResult> UntagResourceAsync(
        string resourceARN,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceARN = resourceARN,
                TagKeys = tagKeys
            });
            return new FsxTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceARN}'");
        }
    }

    /// <summary>
    /// List tags for an FSx resource.
    /// </summary>
    public static async Task<FsxListTagsResult> ListTagsForResourceAsync(
        string resourceARN,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceARN = resourceARN
        };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new FsxListTagsResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceARN}'");
        }
    }

    // ── Restore & NFS Locks ──────────────────────────────────────────

    /// <summary>
    /// Restore a volume from a snapshot.
    /// </summary>
    public static async Task<FsxVolumeInfo>
        RestoreVolumeFromSnapshotAsync(
            RestoreVolumeFromSnapshotRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreVolumeFromSnapshotAsync(request);
            return new FsxVolumeInfo(
                VolumeId: resp.VolumeId,
                Lifecycle: resp.Lifecycle?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore volume from snapshot");
        }
    }

    /// <summary>
    /// Release NFS V3 locks on a file system.
    /// </summary>
    public static async Task<FsxFileSystemInfo>
        ReleaseFileSystemNfsV3LocksAsync(
            string fileSystemId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ReleaseFileSystemNfsV3LocksAsync(
                new ReleaseFileSystemNfsV3LocksRequest
                {
                    FileSystemId = fileSystemId
                });
            var fs = resp.FileSystem;
            return new FsxFileSystemInfo(
                FileSystemId: fs.FileSystemId,
                FileSystemType: fs.FileSystemType?.Value,
                Lifecycle: fs.Lifecycle?.Value,
                StorageCapacity: fs.StorageCapacity,
                StorageType: fs.StorageType?.Value,
                VpcId: fs.VpcId,
                DNSName: fs.DNSName,
                ResourceARN: fs.ResourceARN,
                OwnerId: fs.OwnerId,
                CreationTime: fs.CreationTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to release NFS V3 locks for file system '{fileSystemId}'");
        }
    }
}
