using Amazon;
using Amazon.Lightsail;
using Amazon.Lightsail.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record LightsailInstanceInfo(
    string? Name = null,
    string? Arn = null,
    string? State = null,
    string? PublicIpAddress = null,
    string? PrivateIpAddress = null,
    string? BlueprintId = null,
    string? BundleId = null,
    DateTime? CreatedAt = null);

public sealed record LightsailSnapshotInfo(
    string? Name = null,
    string? Arn = null,
    string? State = null,
    string? FromInstanceName = null,
    DateTime? CreatedAt = null);

public sealed record LightsailKeyPairInfo(
    string? Name = null,
    string? Arn = null,
    string? Fingerprint = null,
    DateTime? CreatedAt = null);

public sealed record LightsailStaticIpInfo(
    string? Name = null,
    string? Arn = null,
    string? IpAddress = null,
    string? AttachedTo = null,
    bool? IsAttached = null);

public sealed record LightsailDiskInfo(
    string? Name = null,
    string? Arn = null,
    string? State = null,
    int? SizeInGb = null,
    string? AttachedTo = null,
    string? Path = null,
    DateTime? CreatedAt = null);

public sealed record LightsailDiskSnapshotInfo(
    string? Name = null,
    string? Arn = null,
    string? State = null,
    int? SizeInGb = null,
    string? FromDiskName = null,
    DateTime? CreatedAt = null);

public sealed record LightsailLoadBalancerInfo(
    string? Name = null,
    string? Arn = null,
    string? State = null,
    string? DnsName = null,
    int? HealthCheckPort = null,
    DateTime? CreatedAt = null);

public sealed record LightsailDomainInfo(
    string? Name = null,
    string? Arn = null,
    DateTime? CreatedAt = null);

public sealed record LightsailOperationResult(
    string? Id = null,
    string? OperationType = null,
    string? Status = null,
    DateTime? CreatedAt = null);

public sealed record LightsailBundleInfo(
    string? BundleId = null,
    string? Name = null,
    float? Price = null,
    int? RamSizeInGb = null,
    int? DiskSizeInGb = null,
    int? CpuCount = null);

public sealed record LightsailBlueprintInfo(
    string? BlueprintId = null,
    string? Name = null,
    string? Group = null,
    string? Type = null,
    string? Version = null,
    string? Platform = null);

public sealed record LightsailRegionInfo(
    string? Name = null,
    string? DisplayName = null,
    string? Description = null,
    string? ContinentCode = null);

public sealed record LightsailKeyPairCreateResult(
    LightsailKeyPairInfo? KeyPair = null,
    string? PublicKeyBase64 = null,
    string? PrivateKeyBase64 = null,
    LightsailOperationResult? Operation = null);

/// <summary>
/// Utility helpers for Amazon Lightsail.
/// </summary>
public static class LightsailService
{
    private static AmazonLightsailClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLightsailClient>(region);

    // ── Instance operations ──────────────────────────────────────────

    /// <summary>
    /// Create one or more Lightsail instances.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> CreateInstancesAsync(
        CreateInstancesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateInstancesAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Lightsail instances");
        }
    }

    /// <summary>
    /// Delete a Lightsail instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DeleteInstanceAsync(
        string instanceName,
        bool? forceDeleteAddOns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteInstanceRequest { InstanceName = instanceName };
        if (forceDeleteAddOns.HasValue) request.ForceDeleteAddOns = forceDeleteAddOns.Value;

        try
        {
            var resp = await client.DeleteInstanceAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete instance '{instanceName}'");
        }
    }

    /// <summary>
    /// Get details for a specific Lightsail instance.
    /// </summary>
    public static async Task<LightsailInstanceInfo> GetInstanceAsync(
        string instanceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInstanceAsync(new GetInstanceRequest
            {
                InstanceName = instanceName
            });
            var i = resp.Instance;
            return new LightsailInstanceInfo(
                Name: i.Name,
                Arn: i.Arn,
                State: i.State?.Name,
                PublicIpAddress: i.PublicIpAddress,
                PrivateIpAddress: i.PrivateIpAddress,
                BlueprintId: i.BlueprintId,
                BundleId: i.BundleId,
                CreatedAt: i.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get instance '{instanceName}'");
        }
    }

    /// <summary>
    /// Get all Lightsail instances.
    /// </summary>
    public static async Task<List<LightsailInstanceInfo>> GetInstancesAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetInstancesRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetInstancesAsync(request);
            return resp.Instances.Select(i => new LightsailInstanceInfo(
                Name: i.Name,
                Arn: i.Arn,
                State: i.State?.Name,
                PublicIpAddress: i.PublicIpAddress,
                PrivateIpAddress: i.PrivateIpAddress,
                BlueprintId: i.BlueprintId,
                BundleId: i.BundleId,
                CreatedAt: i.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get instances");
        }
    }

    /// <summary>
    /// Reboot a Lightsail instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> RebootInstanceAsync(
        string instanceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RebootInstanceAsync(new RebootInstanceRequest
            {
                InstanceName = instanceName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot instance '{instanceName}'");
        }
    }

    /// <summary>
    /// Start a stopped Lightsail instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> StartInstanceAsync(
        string instanceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartInstanceAsync(new StartInstanceRequest
            {
                InstanceName = instanceName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start instance '{instanceName}'");
        }
    }

    /// <summary>
    /// Stop a running Lightsail instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> StopInstanceAsync(
        string instanceName,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopInstanceRequest { InstanceName = instanceName };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.StopInstanceAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop instance '{instanceName}'");
        }
    }

    // ── Instance Snapshot operations ─────────────────────────────────

    /// <summary>
    /// Create an instance snapshot.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> CreateInstanceSnapshotAsync(
        string instanceName,
        string instanceSnapshotName,
        List<Amazon.Lightsail.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateInstanceSnapshotRequest
        {
            InstanceName = instanceName,
            InstanceSnapshotName = instanceSnapshotName
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateInstanceSnapshotAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create snapshot for instance '{instanceName}'");
        }
    }

    /// <summary>
    /// Delete an instance snapshot.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DeleteInstanceSnapshotAsync(
        string instanceSnapshotName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteInstanceSnapshotAsync(
                new DeleteInstanceSnapshotRequest
                {
                    InstanceSnapshotName = instanceSnapshotName
                });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot '{instanceSnapshotName}'");
        }
    }

    /// <summary>
    /// Get details for a specific instance snapshot.
    /// </summary>
    public static async Task<LightsailSnapshotInfo> GetInstanceSnapshotAsync(
        string instanceSnapshotName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInstanceSnapshotAsync(
                new GetInstanceSnapshotRequest
                {
                    InstanceSnapshotName = instanceSnapshotName
                });
            var s = resp.InstanceSnapshot;
            return new LightsailSnapshotInfo(
                Name: s.Name,
                Arn: s.Arn,
                State: s.State?.Value,
                FromInstanceName: s.FromInstanceName,
                CreatedAt: s.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get snapshot '{instanceSnapshotName}'");
        }
    }

    /// <summary>
    /// Get all instance snapshots.
    /// </summary>
    public static async Task<List<LightsailSnapshotInfo>> GetInstanceSnapshotsAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetInstanceSnapshotsRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetInstanceSnapshotsAsync(request);
            return resp.InstanceSnapshots.Select(s => new LightsailSnapshotInfo(
                Name: s.Name,
                Arn: s.Arn,
                State: s.State?.Value,
                FromInstanceName: s.FromInstanceName,
                CreatedAt: s.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get instance snapshots");
        }
    }

    // ── Key Pair operations ──────────────────────────────────────────

    /// <summary>
    /// Create a key pair.
    /// </summary>
    public static async Task<LightsailKeyPairCreateResult> CreateKeyPairAsync(
        string keyPairName,
        List<Amazon.Lightsail.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateKeyPairRequest { KeyPairName = keyPairName };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateKeyPairAsync(request);
            var kp = resp.KeyPair;
            return new LightsailKeyPairCreateResult(
                KeyPair: new LightsailKeyPairInfo(
                    Name: kp.Name,
                    Arn: kp.Arn,
                    Fingerprint: kp.Fingerprint,
                    CreatedAt: kp.CreatedAt),
                PublicKeyBase64: resp.PublicKeyBase64,
                PrivateKeyBase64: resp.PrivateKeyBase64,
                Operation: resp.Operation != null
                    ? new LightsailOperationResult(
                        Id: resp.Operation.Id,
                        OperationType: resp.Operation.OperationType?.Value,
                        Status: resp.Operation.Status?.Value,
                        CreatedAt: resp.Operation.CreatedAt)
                    : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create key pair '{keyPairName}'");
        }
    }

    /// <summary>
    /// Delete a key pair.
    /// </summary>
    public static async Task<LightsailOperationResult> DeleteKeyPairAsync(
        string keyPairName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteKeyPairAsync(new DeleteKeyPairRequest
            {
                KeyPairName = keyPairName
            });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete key pair '{keyPairName}'");
        }
    }

    /// <summary>
    /// Get details for a specific key pair.
    /// </summary>
    public static async Task<LightsailKeyPairInfo> GetKeyPairAsync(
        string keyPairName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetKeyPairAsync(new GetKeyPairRequest
            {
                KeyPairName = keyPairName
            });
            var kp = resp.KeyPair;
            return new LightsailKeyPairInfo(
                Name: kp.Name,
                Arn: kp.Arn,
                Fingerprint: kp.Fingerprint,
                CreatedAt: kp.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get key pair '{keyPairName}'");
        }
    }

    /// <summary>
    /// Get all key pairs.
    /// </summary>
    public static async Task<List<LightsailKeyPairInfo>> GetKeyPairsAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetKeyPairsRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetKeyPairsAsync(request);
            return resp.KeyPairs.Select(kp => new LightsailKeyPairInfo(
                Name: kp.Name,
                Arn: kp.Arn,
                Fingerprint: kp.Fingerprint,
                CreatedAt: kp.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get key pairs");
        }
    }

    // ── Static IP operations ─────────────────────────────────────────

    /// <summary>
    /// Allocate a static IP address.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> AllocateStaticIpAsync(
        string staticIpName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AllocateStaticIpAsync(new AllocateStaticIpRequest
            {
                StaticIpName = staticIpName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to allocate static IP '{staticIpName}'");
        }
    }

    /// <summary>
    /// Release a static IP address.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> ReleaseStaticIpAsync(
        string staticIpName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ReleaseStaticIpAsync(new ReleaseStaticIpRequest
            {
                StaticIpName = staticIpName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to release static IP '{staticIpName}'");
        }
    }

    /// <summary>
    /// Attach a static IP to an instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> AttachStaticIpAsync(
        string staticIpName,
        string instanceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AttachStaticIpAsync(new AttachStaticIpRequest
            {
                StaticIpName = staticIpName,
                InstanceName = instanceName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach static IP '{staticIpName}' to '{instanceName}'");
        }
    }

    /// <summary>
    /// Detach a static IP from an instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DetachStaticIpAsync(
        string staticIpName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DetachStaticIpAsync(new DetachStaticIpRequest
            {
                StaticIpName = staticIpName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach static IP '{staticIpName}'");
        }
    }

    /// <summary>
    /// Get details for a specific static IP.
    /// </summary>
    public static async Task<LightsailStaticIpInfo> GetStaticIpAsync(
        string staticIpName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetStaticIpAsync(new GetStaticIpRequest
            {
                StaticIpName = staticIpName
            });
            var ip = resp.StaticIp;
            return new LightsailStaticIpInfo(
                Name: ip.Name,
                Arn: ip.Arn,
                IpAddress: ip.IpAddress,
                AttachedTo: ip.AttachedTo,
                IsAttached: ip.IsAttached);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get static IP '{staticIpName}'");
        }
    }

    /// <summary>
    /// Get all static IPs.
    /// </summary>
    public static async Task<List<LightsailStaticIpInfo>> GetStaticIpsAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetStaticIpsRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetStaticIpsAsync(request);
            return resp.StaticIps.Select(ip => new LightsailStaticIpInfo(
                Name: ip.Name,
                Arn: ip.Arn,
                IpAddress: ip.IpAddress,
                AttachedTo: ip.AttachedTo,
                IsAttached: ip.IsAttached)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get static IPs");
        }
    }

    // ── Disk operations ──────────────────────────────────────────────

    /// <summary>
    /// Create a disk.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> CreateDiskAsync(
        CreateDiskRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDiskAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create disk");
        }
    }

    /// <summary>
    /// Delete a disk.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DeleteDiskAsync(
        string diskName,
        bool? forceDeleteAddOns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDiskRequest { DiskName = diskName };
        if (forceDeleteAddOns.HasValue) request.ForceDeleteAddOns = forceDeleteAddOns.Value;

        try
        {
            var resp = await client.DeleteDiskAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete disk '{diskName}'");
        }
    }

    /// <summary>
    /// Get details for a specific disk.
    /// </summary>
    public static async Task<LightsailDiskInfo> GetDiskAsync(
        string diskName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDiskAsync(new GetDiskRequest
            {
                DiskName = diskName
            });
            var d = resp.Disk;
            return new LightsailDiskInfo(
                Name: d.Name,
                Arn: d.Arn,
                State: d.State?.Value,
                SizeInGb: d.SizeInGb,
                AttachedTo: d.AttachedTo,
                Path: d.Path,
                CreatedAt: d.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get disk '{diskName}'");
        }
    }

    /// <summary>
    /// Get all disks.
    /// </summary>
    public static async Task<List<LightsailDiskInfo>> GetDisksAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDisksRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetDisksAsync(request);
            return resp.Disks.Select(d => new LightsailDiskInfo(
                Name: d.Name,
                Arn: d.Arn,
                State: d.State?.Value,
                SizeInGb: d.SizeInGb,
                AttachedTo: d.AttachedTo,
                Path: d.Path,
                CreatedAt: d.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get disks");
        }
    }

    /// <summary>
    /// Attach a disk to an instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> AttachDiskAsync(
        string diskName,
        string instanceName,
        string diskPath,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AttachDiskAsync(new AttachDiskRequest
            {
                DiskName = diskName,
                InstanceName = instanceName,
                DiskPath = diskPath
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach disk '{diskName}' to '{instanceName}'");
        }
    }

    /// <summary>
    /// Detach a disk from an instance.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DetachDiskAsync(
        string diskName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DetachDiskAsync(new DetachDiskRequest
            {
                DiskName = diskName
            });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach disk '{diskName}'");
        }
    }

    /// <summary>
    /// Create a disk snapshot.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> CreateDiskSnapshotAsync(
        string diskSnapshotName,
        string? diskName = null,
        string? instanceName = null,
        List<Amazon.Lightsail.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDiskSnapshotRequest
        {
            DiskSnapshotName = diskSnapshotName
        };
        if (diskName != null) request.DiskName = diskName;
        if (instanceName != null) request.InstanceName = instanceName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDiskSnapshotAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create disk snapshot '{diskSnapshotName}'");
        }
    }

    /// <summary>
    /// Delete a disk snapshot.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DeleteDiskSnapshotAsync(
        string diskSnapshotName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDiskSnapshotAsync(
                new DeleteDiskSnapshotRequest
                {
                    DiskSnapshotName = diskSnapshotName
                });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete disk snapshot '{diskSnapshotName}'");
        }
    }

    // ── Load Balancer operations ─────────────────────────────────────

    /// <summary>
    /// Create a load balancer.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> CreateLoadBalancerAsync(
        CreateLoadBalancerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateLoadBalancerAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create load balancer");
        }
    }

    /// <summary>
    /// Delete a load balancer.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> DeleteLoadBalancerAsync(
        string loadBalancerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteLoadBalancerAsync(
                new DeleteLoadBalancerRequest
                {
                    LoadBalancerName = loadBalancerName
                });
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete load balancer '{loadBalancerName}'");
        }
    }

    /// <summary>
    /// Get details for a specific load balancer.
    /// </summary>
    public static async Task<LightsailLoadBalancerInfo> GetLoadBalancerAsync(
        string loadBalancerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetLoadBalancerAsync(new GetLoadBalancerRequest
            {
                LoadBalancerName = loadBalancerName
            });
            var lb = resp.LoadBalancer;
            return new LightsailLoadBalancerInfo(
                Name: lb.Name,
                Arn: lb.Arn,
                State: lb.State?.Value,
                DnsName: lb.DnsName,
                HealthCheckPort: null,
                CreatedAt: lb.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get load balancer '{loadBalancerName}'");
        }
    }

    /// <summary>
    /// Get all load balancers.
    /// </summary>
    public static async Task<List<LightsailLoadBalancerInfo>> GetLoadBalancersAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLoadBalancersRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetLoadBalancersAsync(request);
            return resp.LoadBalancers.Select(lb => new LightsailLoadBalancerInfo(
                Name: lb.Name,
                Arn: lb.Arn,
                State: lb.State?.Value,
                DnsName: lb.DnsName,
                HealthCheckPort: null,
                CreatedAt: lb.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get load balancers");
        }
    }

    // ── Domain operations ────────────────────────────────────────────

    /// <summary>
    /// Create a domain resource.
    /// </summary>
    public static async Task<LightsailOperationResult> CreateDomainAsync(
        string domainName,
        List<Amazon.Lightsail.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDomainRequest { DomainName = domainName };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDomainAsync(request);
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create domain '{domainName}'");
        }
    }

    /// <summary>
    /// Delete a domain resource.
    /// </summary>
    public static async Task<LightsailOperationResult> DeleteDomainAsync(
        string domainName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDomainAsync(new DeleteDomainRequest
            {
                DomainName = domainName
            });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete domain '{domainName}'");
        }
    }

    /// <summary>
    /// Get details for a specific domain.
    /// </summary>
    public static async Task<LightsailDomainInfo> GetDomainAsync(
        string domainName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDomainAsync(new GetDomainRequest
            {
                DomainName = domainName
            });
            var d = resp.Domain;
            return new LightsailDomainInfo(
                Name: d.Name,
                Arn: d.Arn,
                CreatedAt: d.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get domain '{domainName}'");
        }
    }

    /// <summary>
    /// Get all domains.
    /// </summary>
    public static async Task<List<LightsailDomainInfo>> GetDomainsAsync(
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDomainsRequest();
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetDomainsAsync(request);
            return resp.Domains.Select(d => new LightsailDomainInfo(
                Name: d.Name,
                Arn: d.Arn,
                CreatedAt: d.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get domains");
        }
    }

    /// <summary>
    /// Create a domain entry (DNS record).
    /// </summary>
    public static async Task<LightsailOperationResult> CreateDomainEntryAsync(
        string domainName,
        DomainEntry domainEntry,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDomainEntryAsync(new CreateDomainEntryRequest
            {
                DomainName = domainName,
                DomainEntry = domainEntry
            });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create domain entry for '{domainName}'");
        }
    }

    /// <summary>
    /// Delete a domain entry (DNS record).
    /// </summary>
    public static async Task<LightsailOperationResult> DeleteDomainEntryAsync(
        string domainName,
        DomainEntry domainEntry,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDomainEntryAsync(new DeleteDomainEntryRequest
            {
                DomainName = domainName,
                DomainEntry = domainEntry
            });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete domain entry for '{domainName}'");
        }
    }

    // ── Firewall operations ──────────────────────────────────────────

    /// <summary>
    /// Open public ports on an instance.
    /// </summary>
    public static async Task<LightsailOperationResult> OpenInstancePublicPortsAsync(
        string instanceName,
        PortInfo portInfo,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.OpenInstancePublicPortsAsync(
                new OpenInstancePublicPortsRequest
                {
                    InstanceName = instanceName,
                    PortInfo = portInfo
                });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to open public ports on '{instanceName}'");
        }
    }

    /// <summary>
    /// Close public ports on an instance.
    /// </summary>
    public static async Task<LightsailOperationResult> CloseInstancePublicPortsAsync(
        string instanceName,
        PortInfo portInfo,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CloseInstancePublicPortsAsync(
                new CloseInstancePublicPortsRequest
                {
                    InstanceName = instanceName,
                    PortInfo = portInfo
                });
            var o = resp.Operation;
            return new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to close public ports on '{instanceName}'");
        }
    }

    // ── Catalog operations ───────────────────────────────────────────

    /// <summary>
    /// Get all available bundles.
    /// </summary>
    public static async Task<List<LightsailBundleInfo>> GetBundlesAsync(
        bool? includeInactive = null,
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBundlesRequest();
        if (includeInactive.HasValue) request.IncludeInactive = includeInactive.Value;
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetBundlesAsync(request);
            return resp.Bundles.Select(b => new LightsailBundleInfo(
                BundleId: b.BundleId,
                Name: b.Name,
                Price: b.Price,
                RamSizeInGb: (int?)b.RamSizeInGb,
                DiskSizeInGb: b.DiskSizeInGb,
                CpuCount: b.CpuCount)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get bundles");
        }
    }

    /// <summary>
    /// Get all available blueprints.
    /// </summary>
    public static async Task<List<LightsailBlueprintInfo>> GetBlueprintsAsync(
        bool? includeInactive = null,
        string? pageToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBlueprintsRequest();
        if (includeInactive.HasValue) request.IncludeInactive = includeInactive.Value;
        if (pageToken != null) request.PageToken = pageToken;

        try
        {
            var resp = await client.GetBlueprintsAsync(request);
            return resp.Blueprints.Select(b => new LightsailBlueprintInfo(
                BlueprintId: b.BlueprintId,
                Name: b.Name,
                Group: b.Group,
                Type: b.Type?.Value,
                Version: b.Version,
                Platform: b.Platform?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get blueprints");
        }
    }

    /// <summary>
    /// Get all available AWS regions for Lightsail.
    /// </summary>
    public static async Task<List<LightsailRegionInfo>> GetRegionsAsync(
        bool? includeAvailabilityZones = null,
        bool? includeRelationalDatabaseAvailabilityZones = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRegionsRequest();
        if (includeAvailabilityZones.HasValue)
            request.IncludeAvailabilityZones = includeAvailabilityZones.Value;
        if (includeRelationalDatabaseAvailabilityZones.HasValue)
            request.IncludeRelationalDatabaseAvailabilityZones =
                includeRelationalDatabaseAvailabilityZones.Value;

        try
        {
            var resp = await client.GetRegionsAsync(request);
            return resp.Regions.Select(r => new LightsailRegionInfo(
                Name: r.Name?.Value,
                DisplayName: r.DisplayName,
                Description: r.Description,
                ContinentCode: r.ContinentCode)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get regions");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Tag a Lightsail resource.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> TagResourceAsync(
        string resourceName,
        List<Amazon.Lightsail.Model.Tag> tags,
        string? resourceArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TagResourceRequest
        {
            ResourceName = resourceName,
            Tags = tags
        };
        if (resourceArn != null) request.ResourceArn = resourceArn;

        try
        {
            var resp = await client.TagResourceAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceName}'");
        }
    }

    /// <summary>
    /// Remove tags from a Lightsail resource.
    /// </summary>
    public static async Task<List<LightsailOperationResult>> UntagResourceAsync(
        string resourceName,
        List<string> tagKeys,
        string? resourceArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UntagResourceRequest
        {
            ResourceName = resourceName,
            TagKeys = tagKeys
        };
        if (resourceArn != null) request.ResourceArn = resourceArn;

        try
        {
            var resp = await client.UntagResourceAsync(request);
            return resp.Operations.Select(o => new LightsailOperationResult(
                Id: o.Id,
                OperationType: o.OperationType?.Value,
                Status: o.Status?.Value,
                CreatedAt: o.CreatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceName}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateInstancesAsync"/>.</summary>
    public static List<LightsailOperationResult> CreateInstances(CreateInstancesRequest request, RegionEndpoint? region = null)
        => CreateInstancesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteInstanceAsync"/>.</summary>
    public static List<LightsailOperationResult> DeleteInstance(string instanceName, bool? forceDeleteAddOns = null, RegionEndpoint? region = null)
        => DeleteInstanceAsync(instanceName, forceDeleteAddOns, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetInstanceAsync"/>.</summary>
    public static LightsailInstanceInfo GetInstance(string instanceName, RegionEndpoint? region = null)
        => GetInstanceAsync(instanceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetInstancesAsync"/>.</summary>
    public static List<LightsailInstanceInfo> GetInstances(string? pageToken = null, RegionEndpoint? region = null)
        => GetInstancesAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RebootInstanceAsync"/>.</summary>
    public static List<LightsailOperationResult> RebootInstance(string instanceName, RegionEndpoint? region = null)
        => RebootInstanceAsync(instanceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartInstanceAsync"/>.</summary>
    public static List<LightsailOperationResult> StartInstance(string instanceName, RegionEndpoint? region = null)
        => StartInstanceAsync(instanceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopInstanceAsync"/>.</summary>
    public static List<LightsailOperationResult> StopInstance(string instanceName, bool? force = null, RegionEndpoint? region = null)
        => StopInstanceAsync(instanceName, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateInstanceSnapshotAsync"/>.</summary>
    public static List<LightsailOperationResult> CreateInstanceSnapshot(string instanceName, string instanceSnapshotName, List<Amazon.Lightsail.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateInstanceSnapshotAsync(instanceName, instanceSnapshotName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteInstanceSnapshotAsync"/>.</summary>
    public static List<LightsailOperationResult> DeleteInstanceSnapshot(string instanceSnapshotName, RegionEndpoint? region = null)
        => DeleteInstanceSnapshotAsync(instanceSnapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetInstanceSnapshotAsync"/>.</summary>
    public static LightsailSnapshotInfo GetInstanceSnapshot(string instanceSnapshotName, RegionEndpoint? region = null)
        => GetInstanceSnapshotAsync(instanceSnapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetInstanceSnapshotsAsync"/>.</summary>
    public static List<LightsailSnapshotInfo> GetInstanceSnapshots(string? pageToken = null, RegionEndpoint? region = null)
        => GetInstanceSnapshotsAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateKeyPairAsync"/>.</summary>
    public static LightsailKeyPairCreateResult CreateKeyPair(string keyPairName, List<Amazon.Lightsail.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateKeyPairAsync(keyPairName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteKeyPairAsync"/>.</summary>
    public static LightsailOperationResult DeleteKeyPair(string keyPairName, RegionEndpoint? region = null)
        => DeleteKeyPairAsync(keyPairName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetKeyPairAsync"/>.</summary>
    public static LightsailKeyPairInfo GetKeyPair(string keyPairName, RegionEndpoint? region = null)
        => GetKeyPairAsync(keyPairName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetKeyPairsAsync"/>.</summary>
    public static List<LightsailKeyPairInfo> GetKeyPairs(string? pageToken = null, RegionEndpoint? region = null)
        => GetKeyPairsAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AllocateStaticIpAsync"/>.</summary>
    public static List<LightsailOperationResult> AllocateStaticIp(string staticIpName, RegionEndpoint? region = null)
        => AllocateStaticIpAsync(staticIpName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReleaseStaticIpAsync"/>.</summary>
    public static List<LightsailOperationResult> ReleaseStaticIp(string staticIpName, RegionEndpoint? region = null)
        => ReleaseStaticIpAsync(staticIpName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachStaticIpAsync"/>.</summary>
    public static List<LightsailOperationResult> AttachStaticIp(string staticIpName, string instanceName, RegionEndpoint? region = null)
        => AttachStaticIpAsync(staticIpName, instanceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachStaticIpAsync"/>.</summary>
    public static List<LightsailOperationResult> DetachStaticIp(string staticIpName, RegionEndpoint? region = null)
        => DetachStaticIpAsync(staticIpName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetStaticIpAsync"/>.</summary>
    public static LightsailStaticIpInfo GetStaticIp(string staticIpName, RegionEndpoint? region = null)
        => GetStaticIpAsync(staticIpName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetStaticIpsAsync"/>.</summary>
    public static List<LightsailStaticIpInfo> GetStaticIps(string? pageToken = null, RegionEndpoint? region = null)
        => GetStaticIpsAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDiskAsync"/>.</summary>
    public static List<LightsailOperationResult> CreateDisk(CreateDiskRequest request, RegionEndpoint? region = null)
        => CreateDiskAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDiskAsync"/>.</summary>
    public static List<LightsailOperationResult> DeleteDisk(string diskName, bool? forceDeleteAddOns = null, RegionEndpoint? region = null)
        => DeleteDiskAsync(diskName, forceDeleteAddOns, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDiskAsync"/>.</summary>
    public static LightsailDiskInfo GetDisk(string diskName, RegionEndpoint? region = null)
        => GetDiskAsync(diskName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDisksAsync"/>.</summary>
    public static List<LightsailDiskInfo> GetDisks(string? pageToken = null, RegionEndpoint? region = null)
        => GetDisksAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachDiskAsync"/>.</summary>
    public static List<LightsailOperationResult> AttachDisk(string diskName, string instanceName, string diskPath, RegionEndpoint? region = null)
        => AttachDiskAsync(diskName, instanceName, diskPath, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachDiskAsync"/>.</summary>
    public static List<LightsailOperationResult> DetachDisk(string diskName, RegionEndpoint? region = null)
        => DetachDiskAsync(diskName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDiskSnapshotAsync"/>.</summary>
    public static List<LightsailOperationResult> CreateDiskSnapshot(string diskSnapshotName, string? diskName = null, string? instanceName = null, List<Amazon.Lightsail.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDiskSnapshotAsync(diskSnapshotName, diskName, instanceName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDiskSnapshotAsync"/>.</summary>
    public static List<LightsailOperationResult> DeleteDiskSnapshot(string diskSnapshotName, RegionEndpoint? region = null)
        => DeleteDiskSnapshotAsync(diskSnapshotName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateLoadBalancerAsync"/>.</summary>
    public static List<LightsailOperationResult> CreateLoadBalancer(CreateLoadBalancerRequest request, RegionEndpoint? region = null)
        => CreateLoadBalancerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteLoadBalancerAsync"/>.</summary>
    public static List<LightsailOperationResult> DeleteLoadBalancer(string loadBalancerName, RegionEndpoint? region = null)
        => DeleteLoadBalancerAsync(loadBalancerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLoadBalancerAsync"/>.</summary>
    public static LightsailLoadBalancerInfo GetLoadBalancer(string loadBalancerName, RegionEndpoint? region = null)
        => GetLoadBalancerAsync(loadBalancerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLoadBalancersAsync"/>.</summary>
    public static List<LightsailLoadBalancerInfo> GetLoadBalancers(string? pageToken = null, RegionEndpoint? region = null)
        => GetLoadBalancersAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDomainAsync"/>.</summary>
    public static LightsailOperationResult CreateDomain(string domainName, List<Amazon.Lightsail.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDomainAsync(domainName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDomainAsync"/>.</summary>
    public static LightsailOperationResult DeleteDomain(string domainName, RegionEndpoint? region = null)
        => DeleteDomainAsync(domainName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDomainAsync"/>.</summary>
    public static LightsailDomainInfo GetDomain(string domainName, RegionEndpoint? region = null)
        => GetDomainAsync(domainName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDomainsAsync"/>.</summary>
    public static List<LightsailDomainInfo> GetDomains(string? pageToken = null, RegionEndpoint? region = null)
        => GetDomainsAsync(pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDomainEntryAsync"/>.</summary>
    public static LightsailOperationResult CreateDomainEntry(string domainName, DomainEntry domainEntry, RegionEndpoint? region = null)
        => CreateDomainEntryAsync(domainName, domainEntry, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDomainEntryAsync"/>.</summary>
    public static LightsailOperationResult DeleteDomainEntry(string domainName, DomainEntry domainEntry, RegionEndpoint? region = null)
        => DeleteDomainEntryAsync(domainName, domainEntry, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="OpenInstancePublicPortsAsync"/>.</summary>
    public static LightsailOperationResult OpenInstancePublicPorts(string instanceName, PortInfo portInfo, RegionEndpoint? region = null)
        => OpenInstancePublicPortsAsync(instanceName, portInfo, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CloseInstancePublicPortsAsync"/>.</summary>
    public static LightsailOperationResult CloseInstancePublicPorts(string instanceName, PortInfo portInfo, RegionEndpoint? region = null)
        => CloseInstancePublicPortsAsync(instanceName, portInfo, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBundlesAsync"/>.</summary>
    public static List<LightsailBundleInfo> GetBundles(bool? includeInactive = null, string? pageToken = null, RegionEndpoint? region = null)
        => GetBundlesAsync(includeInactive, pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBlueprintsAsync"/>.</summary>
    public static List<LightsailBlueprintInfo> GetBlueprints(bool? includeInactive = null, string? pageToken = null, RegionEndpoint? region = null)
        => GetBlueprintsAsync(includeInactive, pageToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRegionsAsync"/>.</summary>
    public static List<LightsailRegionInfo> GetRegions(bool? includeAvailabilityZones = null, bool? includeRelationalDatabaseAvailabilityZones = null, RegionEndpoint? region = null)
        => GetRegionsAsync(includeAvailabilityZones, includeRelationalDatabaseAvailabilityZones, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static List<LightsailOperationResult> TagResource(string resourceName, List<Amazon.Lightsail.Model.Tag> tags, string? resourceArn = null, RegionEndpoint? region = null)
        => TagResourceAsync(resourceName, tags, resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static List<LightsailOperationResult> UntagResource(string resourceName, List<string> tagKeys, string? resourceArn = null, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceName, tagKeys, resourceArn, region).GetAwaiter().GetResult();

}
