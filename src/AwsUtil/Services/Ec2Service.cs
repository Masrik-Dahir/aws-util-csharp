using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record Ec2InstanceResult(
    string? InstanceId = null, string? State = null,
    string? InstanceType = null, string? PublicIpAddress = null,
    string? PrivateIpAddress = null, string? VpcId = null,
    string? SubnetId = null, string? ImageId = null,
    string? KeyName = null, string? LaunchTime = null,
    List<Dictionary<string, string>>? Tags = null);

public sealed record RunInstancesResult(List<Ec2InstanceResult>? Instances = null);

public sealed record InstanceStateChangeResult(
    string? InstanceId = null, string? PreviousState = null,
    string? CurrentState = null);

public sealed record SecurityGroupResult(
    string? GroupId = null, string? GroupName = null,
    string? Description = null, string? VpcId = null);

public sealed record CreateSecurityGroupResult(string? GroupId = null);

public sealed record VpcResult(
    string? VpcId = null, string? CidrBlock = null,
    string? State = null, bool? IsDefault = null);

public sealed record CreateVpcResult(string? VpcId = null, string? CidrBlock = null);

public sealed record SubnetResult(
    string? SubnetId = null, string? VpcId = null,
    string? CidrBlock = null, string? AvailabilityZone = null,
    string? State = null);

public sealed record CreateSubnetResult(
    string? SubnetId = null, string? VpcId = null,
    string? CidrBlock = null, string? AvailabilityZone = null);

public sealed record AllocateAddressResult(
    string? AllocationId = null, string? PublicIp = null);

public sealed record AssociateAddressResult(string? AssociationId = null);

public sealed record ImageResult(
    string? ImageId = null, string? Name = null,
    string? State = null, string? OwnerId = null,
    string? Description = null);

public sealed record CreateImageResult(string? ImageId = null);

public sealed record VolumeResult(
    string? VolumeId = null, string? State = null,
    int? Size = null, string? VolumeType = null,
    string? AvailabilityZone = null);

public sealed record CreateVolumeResult(
    string? VolumeId = null, int? Size = null,
    string? VolumeType = null, string? AvailabilityZone = null);

public sealed record AttachVolumeResult(
    string? VolumeId = null, string? InstanceId = null,
    string? Device = null, string? State = null);

public sealed record DetachVolumeResult(
    string? VolumeId = null, string? InstanceId = null,
    string? Device = null, string? State = null);

public sealed record SnapshotResult(
    string? SnapshotId = null, string? VolumeId = null,
    string? State = null, int? VolumeSize = null,
    string? StartTime = null);

public sealed record KeyPairResult(
    string? KeyPairId = null, string? KeyName = null,
    string? KeyFingerprint = null);

public sealed record CreateKeyPairResult(
    string? KeyPairId = null, string? KeyName = null,
    string? KeyFingerprint = null, string? KeyMaterial = null);

public sealed record InternetGatewayResult(
    string? InternetGatewayId = null,
    List<string>? AttachedVpcIds = null);

public sealed record CreateInternetGatewayResult(string? InternetGatewayId = null);

public sealed record RouteTableResult(
    string? RouteTableId = null, string? VpcId = null);

public sealed record CreateRouteTableResult(
    string? RouteTableId = null, string? VpcId = null);

public sealed record NatGatewayResult(
    string? NatGatewayId = null, string? VpcId = null,
    string? SubnetId = null, string? State = null);

public sealed record CreateNatGatewayResult(
    string? NatGatewayId = null, string? SubnetId = null);

public sealed record NetworkInterfaceResult(
    string? NetworkInterfaceId = null, string? SubnetId = null,
    string? VpcId = null, string? PrivateIpAddress = null,
    string? Status = null);

public sealed record InstanceTypeResult(
    string? InstanceType = null, int? VCpuCount = null,
    long? MemoryMiB = null);

public sealed record VpcPeeringConnectionResult(
    string? VpcPeeringConnectionId = null, string? Status = null);

public sealed record TransitGatewayVpcAttachmentResult(
    string? TransitGatewayAttachmentId = null, string? State = null);

public sealed record VpcEndpointResult(
    string? VpcEndpointId = null, string? State = null);

public sealed record DescribeNetworkAclsResult(
    List<NetworkAcl>? NetworkAcls = null);

/// <summary>
/// Utility helpers for Amazon EC2.
/// </summary>
public static class Ec2Service
{
    private static AmazonEC2Client GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEC2Client>(region);

    // ── Instances ────────────────────────────────────────────────────

    /// <summary>
    /// Describe EC2 instances, optionally filtered by instance IDs.
    /// </summary>
    public static async Task<List<Ec2InstanceResult>> DescribeInstancesAsync(
        List<string>? instanceIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeInstancesRequest();
        if (instanceIds != null) request.InstanceIds = instanceIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var results = new List<Ec2InstanceResult>();
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeInstancesAsync(request);
                foreach (var reservation in resp.Reservations)
                foreach (var inst in reservation.Instances)
                {
                    results.Add(new Ec2InstanceResult(
                        InstanceId: inst.InstanceId,
                        State: inst.State?.Name?.Value,
                        InstanceType: inst.InstanceType?.Value,
                        PublicIpAddress: inst.PublicIpAddress,
                        PrivateIpAddress: inst.PrivateIpAddress,
                        VpcId: inst.VpcId,
                        SubnetId: inst.SubnetId,
                        ImageId: inst.ImageId,
                        KeyName: inst.KeyName,
                        LaunchTime: inst.LaunchTime.ToString(),
                        Tags: inst.Tags?.Select(t => new Dictionary<string, string>
                        {
                            ["Key"] = t.Key, ["Value"] = t.Value
                        }).ToList()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe instances");
        }
    }

    /// <summary>
    /// Launch new EC2 instances.
    /// </summary>
    public static async Task<RunInstancesResult> RunInstancesAsync(
        string imageId,
        string instanceType,
        int minCount = 1,
        int maxCount = 1,
        string? keyName = null,
        List<string>? securityGroupIds = null,
        string? subnetId = null,
        string? userData = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RunInstancesRequest
        {
            ImageId = imageId,
            InstanceType = instanceType,
            MinCount = minCount,
            MaxCount = maxCount
        };
        if (keyName != null) request.KeyName = keyName;
        if (securityGroupIds != null) request.SecurityGroupIds = securityGroupIds;
        if (subnetId != null) request.SubnetId = subnetId;
        if (userData != null) request.UserData = userData;

        try
        {
            var resp = await client.RunInstancesAsync(request);
            var instances = resp.Reservation.Instances.Select(inst =>
                new Ec2InstanceResult(
                    InstanceId: inst.InstanceId,
                    State: inst.State?.Name?.Value,
                    InstanceType: inst.InstanceType?.Value,
                    PublicIpAddress: inst.PublicIpAddress,
                    PrivateIpAddress: inst.PrivateIpAddress,
                    VpcId: inst.VpcId,
                    SubnetId: inst.SubnetId,
                    ImageId: inst.ImageId,
                    KeyName: inst.KeyName,
                    LaunchTime: inst.LaunchTime.ToString())).ToList();
            return new RunInstancesResult(Instances: instances);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to run instances");
        }
    }

    /// <summary>
    /// Start stopped EC2 instances.
    /// </summary>
    public static async Task<List<InstanceStateChangeResult>> StartInstancesAsync(
        List<string> instanceIds, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartInstancesAsync(
                new StartInstancesRequest { InstanceIds = instanceIds });
            return resp.StartingInstances.Select(s => new InstanceStateChangeResult(
                InstanceId: s.InstanceId,
                PreviousState: s.PreviousState?.Name?.Value,
                CurrentState: s.CurrentState?.Name?.Value)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start instances");
        }
    }

    /// <summary>
    /// Stop running EC2 instances.
    /// </summary>
    public static async Task<List<InstanceStateChangeResult>> StopInstancesAsync(
        List<string> instanceIds,
        bool force = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopInstancesAsync(
                new StopInstancesRequest { InstanceIds = instanceIds, Force = force });
            return resp.StoppingInstances.Select(s => new InstanceStateChangeResult(
                InstanceId: s.InstanceId,
                PreviousState: s.PreviousState?.Name?.Value,
                CurrentState: s.CurrentState?.Name?.Value)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to stop instances");
        }
    }

    /// <summary>
    /// Terminate EC2 instances.
    /// </summary>
    public static async Task<List<InstanceStateChangeResult>> TerminateInstancesAsync(
        List<string> instanceIds, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TerminateInstancesAsync(
                new TerminateInstancesRequest { InstanceIds = instanceIds });
            return resp.TerminatingInstances.Select(s => new InstanceStateChangeResult(
                InstanceId: s.InstanceId,
                PreviousState: s.PreviousState?.Name?.Value,
                CurrentState: s.CurrentState?.Name?.Value)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to terminate instances");
        }
    }

    /// <summary>
    /// Reboot EC2 instances.
    /// </summary>
    public static async Task RebootInstancesAsync(
        List<string> instanceIds, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RebootInstancesAsync(
                new RebootInstancesRequest { InstanceIds = instanceIds });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to reboot instances");
        }
    }

    // ── Security Groups ──────────────────────────────────────────────

    /// <summary>
    /// Describe security groups.
    /// </summary>
    public static async Task<List<SecurityGroupResult>> DescribeSecurityGroupsAsync(
        List<string>? groupIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSecurityGroupsRequest();
        if (groupIds != null) request.GroupIds = groupIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeSecurityGroupsAsync(request);
            return resp.SecurityGroups.Select(sg => new SecurityGroupResult(
                GroupId: sg.GroupId,
                GroupName: sg.GroupName,
                Description: sg.Description,
                VpcId: sg.VpcId)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe security groups");
        }
    }

    /// <summary>
    /// Create a security group.
    /// </summary>
    public static async Task<CreateSecurityGroupResult> CreateSecurityGroupAsync(
        string groupName,
        string description,
        string? vpcId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSecurityGroupRequest
        {
            GroupName = groupName,
            Description = description
        };
        if (vpcId != null) request.VpcId = vpcId;

        try
        {
            var resp = await client.CreateSecurityGroupAsync(request);
            return new CreateSecurityGroupResult(GroupId: resp.GroupId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create security group '{groupName}'");
        }
    }

    /// <summary>
    /// Delete a security group.
    /// </summary>
    public static async Task DeleteSecurityGroupAsync(
        string groupId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSecurityGroupAsync(
                new DeleteSecurityGroupRequest { GroupId = groupId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete security group '{groupId}'");
        }
    }

    /// <summary>
    /// Authorize inbound traffic for a security group.
    /// </summary>
    public static async Task AuthorizeSecurityGroupIngressAsync(
        string groupId,
        List<IpPermission> ipPermissions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AuthorizeSecurityGroupIngressAsync(
                new AuthorizeSecurityGroupIngressRequest
                {
                    GroupId = groupId,
                    IpPermissions = ipPermissions
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to authorize ingress for security group '{groupId}'");
        }
    }

    /// <summary>
    /// Revoke inbound traffic for a security group.
    /// </summary>
    public static async Task RevokeSecurityGroupIngressAsync(
        string groupId,
        List<IpPermission> ipPermissions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RevokeSecurityGroupIngressAsync(
                new RevokeSecurityGroupIngressRequest
                {
                    GroupId = groupId,
                    IpPermissions = ipPermissions
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to revoke ingress for security group '{groupId}'");
        }
    }

    // ── VPCs ─────────────────────────────────────────────────────────

    /// <summary>
    /// Describe VPCs.
    /// </summary>
    public static async Task<List<VpcResult>> DescribeVpcsAsync(
        List<string>? vpcIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeVpcsRequest();
        if (vpcIds != null) request.VpcIds = vpcIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeVpcsAsync(request);
            return resp.Vpcs.Select(v => new VpcResult(
                VpcId: v.VpcId,
                CidrBlock: v.CidrBlock,
                State: v.State?.Value,
                IsDefault: v.IsDefault)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe VPCs");
        }
    }

    /// <summary>
    /// Create a VPC.
    /// </summary>
    public static async Task<CreateVpcResult> CreateVpcAsync(
        string cidrBlock,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVpcAsync(
                new CreateVpcRequest { CidrBlock = cidrBlock });
            return new CreateVpcResult(
                VpcId: resp.Vpc.VpcId,
                CidrBlock: resp.Vpc.CidrBlock);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create VPC");
        }
    }

    /// <summary>
    /// Delete a VPC.
    /// </summary>
    public static async Task DeleteVpcAsync(
        string vpcId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteVpcAsync(new DeleteVpcRequest { VpcId = vpcId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete VPC '{vpcId}'");
        }
    }

    // ── Subnets ──────────────────────────────────────────────────────

    /// <summary>
    /// Describe subnets.
    /// </summary>
    public static async Task<List<SubnetResult>> DescribeSubnetsAsync(
        List<string>? subnetIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSubnetsRequest();
        if (subnetIds != null) request.SubnetIds = subnetIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeSubnetsAsync(request);
            return resp.Subnets.Select(s => new SubnetResult(
                SubnetId: s.SubnetId,
                VpcId: s.VpcId,
                CidrBlock: s.CidrBlock,
                AvailabilityZone: s.AvailabilityZone,
                State: s.State?.Value)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe subnets");
        }
    }

    /// <summary>
    /// Create a subnet.
    /// </summary>
    public static async Task<CreateSubnetResult> CreateSubnetAsync(
        string vpcId,
        string cidrBlock,
        string? availabilityZone = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSubnetRequest
        {
            VpcId = vpcId,
            CidrBlock = cidrBlock
        };
        if (availabilityZone != null) request.AvailabilityZone = availabilityZone;

        try
        {
            var resp = await client.CreateSubnetAsync(request);
            return new CreateSubnetResult(
                SubnetId: resp.Subnet.SubnetId,
                VpcId: resp.Subnet.VpcId,
                CidrBlock: resp.Subnet.CidrBlock,
                AvailabilityZone: resp.Subnet.AvailabilityZone);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create subnet");
        }
    }

    /// <summary>
    /// Delete a subnet.
    /// </summary>
    public static async Task DeleteSubnetAsync(
        string subnetId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSubnetAsync(
                new DeleteSubnetRequest { SubnetId = subnetId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete subnet '{subnetId}'");
        }
    }

    // ── Elastic IPs ──────────────────────────────────────────────────

    /// <summary>
    /// Allocate an Elastic IP address.
    /// </summary>
    public static async Task<AllocateAddressResult> AllocateAddressAsync(
        string domain = "vpc",
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AllocateAddressAsync(
                new AllocateAddressRequest { Domain = domain });
            return new AllocateAddressResult(
                AllocationId: resp.AllocationId,
                PublicIp: resp.PublicIp);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to allocate address");
        }
    }

    /// <summary>
    /// Release an Elastic IP address.
    /// </summary>
    public static async Task ReleaseAddressAsync(
        string allocationId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ReleaseAddressAsync(
                new ReleaseAddressRequest { AllocationId = allocationId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to release address '{allocationId}'");
        }
    }

    /// <summary>
    /// Associate an Elastic IP address with an instance or network interface.
    /// </summary>
    public static async Task<AssociateAddressResult> AssociateAddressAsync(
        string allocationId,
        string? instanceId = null,
        string? networkInterfaceId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AssociateAddressRequest { AllocationId = allocationId };
        if (instanceId != null) request.InstanceId = instanceId;
        if (networkInterfaceId != null) request.NetworkInterfaceId = networkInterfaceId;

        try
        {
            var resp = await client.AssociateAddressAsync(request);
            return new AssociateAddressResult(AssociationId: resp.AssociationId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to associate address");
        }
    }

    /// <summary>
    /// Disassociate an Elastic IP address.
    /// </summary>
    public static async Task DisassociateAddressAsync(
        string associationId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateAddressAsync(
                new DisassociateAddressRequest { AssociationId = associationId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate address '{associationId}'");
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────

    /// <summary>
    /// Create tags on EC2 resources.
    /// </summary>
    public static async Task CreateTagsAsync(
        List<string> resourceIds,
        List<Amazon.EC2.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateTagsAsync(new CreateTagsRequest
            {
                Resources = resourceIds,
                Tags = tags
            });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create tags");
        }
    }

    /// <summary>
    /// Delete tags from EC2 resources.
    /// </summary>
    public static async Task DeleteTagsAsync(
        List<string> resourceIds,
        List<Amazon.EC2.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTagsAsync(new DeleteTagsRequest
            {
                Resources = resourceIds,
                Tags = tags
            });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete tags");
        }
    }

    // ── Images (AMIs) ────────────────────────────────────────────────

    /// <summary>
    /// Describe AMI images.
    /// </summary>
    public static async Task<List<ImageResult>> DescribeImagesAsync(
        List<string>? imageIds = null,
        List<string>? owners = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeImagesRequest();
        if (imageIds != null) request.ImageIds = imageIds;
        if (owners != null) request.Owners = owners;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeImagesAsync(request);
            return resp.Images.Select(i => new ImageResult(
                ImageId: i.ImageId,
                Name: i.Name,
                State: i.State?.Value,
                OwnerId: i.OwnerId,
                Description: i.Description)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe images");
        }
    }

    /// <summary>
    /// Create an AMI from an instance.
    /// </summary>
    public static async Task<CreateImageResult> CreateImageAsync(
        string instanceId,
        string name,
        string? description = null,
        bool noReboot = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateImageRequest
        {
            InstanceId = instanceId,
            Name = name,
            NoReboot = noReboot
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.CreateImageAsync(request);
            return new CreateImageResult(ImageId: resp.ImageId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create image from '{instanceId}'");
        }
    }

    // ── Volumes ──────────────────────────────────────────────────────

    /// <summary>
    /// Describe EBS volumes.
    /// </summary>
    public static async Task<List<VolumeResult>> DescribeVolumesAsync(
        List<string>? volumeIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeVolumesRequest();
        if (volumeIds != null) request.VolumeIds = volumeIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeVolumesAsync(request);
            return resp.Volumes.Select(v => new VolumeResult(
                VolumeId: v.VolumeId,
                State: v.State?.Value,
                Size: v.Size,
                VolumeType: v.VolumeType?.Value,
                AvailabilityZone: v.AvailabilityZone)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe volumes");
        }
    }

    /// <summary>
    /// Create an EBS volume.
    /// </summary>
    public static async Task<CreateVolumeResult> CreateVolumeAsync(
        string availabilityZone,
        int? size = null,
        string? volumeType = null,
        string? snapshotId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateVolumeRequest
        {
            AvailabilityZone = availabilityZone
        };
        if (size.HasValue) request.Size = size.Value;
        if (volumeType != null) request.VolumeType = volumeType;
        if (snapshotId != null) request.SnapshotId = snapshotId;

        try
        {
            var resp = await client.CreateVolumeAsync(request);
            var vol = resp.Volume;
            return new CreateVolumeResult(
                VolumeId: vol.VolumeId,
                Size: vol.Size ?? 0,
                VolumeType: vol.VolumeType?.Value,
                AvailabilityZone: vol.AvailabilityZone);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create volume");
        }
    }

    /// <summary>
    /// Delete an EBS volume.
    /// </summary>
    public static async Task DeleteVolumeAsync(
        string volumeId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteVolumeAsync(
                new DeleteVolumeRequest { VolumeId = volumeId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete volume '{volumeId}'");
        }
    }

    /// <summary>
    /// Attach an EBS volume to an instance.
    /// </summary>
    public static async Task<AttachVolumeResult> AttachVolumeAsync(
        string volumeId,
        string instanceId,
        string device,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AttachVolumeAsync(new AttachVolumeRequest
            {
                VolumeId = volumeId,
                InstanceId = instanceId,
                Device = device
            });
            var att = resp.Attachment;
            return new AttachVolumeResult(
                VolumeId: att.VolumeId,
                InstanceId: att.InstanceId,
                Device: att.Device,
                State: att.State?.Value);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach volume '{volumeId}' to '{instanceId}'");
        }
    }

    /// <summary>
    /// Detach an EBS volume from an instance.
    /// </summary>
    public static async Task<DetachVolumeResult> DetachVolumeAsync(
        string volumeId,
        string? instanceId = null,
        bool force = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetachVolumeRequest
        {
            VolumeId = volumeId,
            Force = force
        };
        if (instanceId != null) request.InstanceId = instanceId;

        try
        {
            var resp = await client.DetachVolumeAsync(request);
            var att = resp.Attachment;
            return new DetachVolumeResult(
                VolumeId: att.VolumeId,
                InstanceId: att.InstanceId,
                Device: att.Device,
                State: att.State?.Value);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach volume '{volumeId}'");
        }
    }

    // ── Snapshots ────────────────────────────────────────────────────

    /// <summary>
    /// Create an EBS snapshot.
    /// </summary>
    public static async Task<SnapshotResult> CreateSnapshotAsync(
        string volumeId,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSnapshotRequest { VolumeId = volumeId };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.CreateSnapshotAsync(request);
            var snap = resp.Snapshot;
            return new SnapshotResult(
                SnapshotId: snap.SnapshotId,
                VolumeId: snap.VolumeId,
                State: snap.State?.Value,
                VolumeSize: snap.VolumeSize ?? 0,
                StartTime: snap.StartTime.ToString());
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create snapshot for volume '{volumeId}'");
        }
    }

    /// <summary>
    /// Delete an EBS snapshot.
    /// </summary>
    public static async Task DeleteSnapshotAsync(
        string snapshotId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSnapshotAsync(
                new DeleteSnapshotRequest { SnapshotId = snapshotId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot '{snapshotId}'");
        }
    }

    /// <summary>
    /// Describe EBS snapshots.
    /// </summary>
    public static async Task<List<SnapshotResult>> DescribeSnapshotsAsync(
        List<string>? snapshotIds = null,
        List<string>? ownerIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSnapshotsRequest();
        if (snapshotIds != null) request.SnapshotIds = snapshotIds;
        if (ownerIds != null) request.OwnerIds = ownerIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeSnapshotsAsync(request);
            return resp.Snapshots.Select(s => new SnapshotResult(
                SnapshotId: s.SnapshotId,
                VolumeId: s.VolumeId,
                State: s.State?.Value,
                VolumeSize: s.VolumeSize,
                StartTime: s.StartTime.ToString())).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe snapshots");
        }
    }

    // ── Key Pairs ────────────────────────────────────────────────────

    /// <summary>
    /// Describe key pairs.
    /// </summary>
    public static async Task<List<KeyPairResult>> DescribeKeyPairsAsync(
        List<string>? keyNames = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeKeyPairsRequest();
        if (keyNames != null) request.KeyNames = keyNames;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeKeyPairsAsync(request);
            return resp.KeyPairs.Select(kp => new KeyPairResult(
                KeyPairId: kp.KeyPairId,
                KeyName: kp.KeyName,
                KeyFingerprint: kp.KeyFingerprint)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe key pairs");
        }
    }

    /// <summary>
    /// Create a key pair.
    /// </summary>
    public static async Task<CreateKeyPairResult> CreateKeyPairAsync(
        string keyName,
        string? keyType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateKeyPairRequest { KeyName = keyName };
        if (keyType != null) request.KeyType = keyType;

        try
        {
            var resp = await client.CreateKeyPairAsync(request);
            var kp = resp.KeyPair;
            return new CreateKeyPairResult(
                KeyPairId: kp.KeyPairId,
                KeyName: kp.KeyName,
                KeyFingerprint: kp.KeyFingerprint,
                KeyMaterial: kp.KeyMaterial);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create key pair '{keyName}'");
        }
    }

    /// <summary>
    /// Delete a key pair.
    /// </summary>
    public static async Task DeleteKeyPairAsync(
        string keyName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteKeyPairAsync(
                new DeleteKeyPairRequest { KeyName = keyName });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete key pair '{keyName}'");
        }
    }

    // ── Internet Gateways ────────────────────────────────────────────

    /// <summary>
    /// Describe internet gateways.
    /// </summary>
    public static async Task<List<InternetGatewayResult>> DescribeInternetGatewaysAsync(
        List<string>? internetGatewayIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeInternetGatewaysRequest();
        if (internetGatewayIds != null) request.InternetGatewayIds = internetGatewayIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeInternetGatewaysAsync(request);
            return resp.InternetGateways.Select(igw => new InternetGatewayResult(
                InternetGatewayId: igw.InternetGatewayId,
                AttachedVpcIds: igw.Attachments?
                    .Select(a => a.VpcId).ToList())).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe internet gateways");
        }
    }

    /// <summary>
    /// Create an internet gateway.
    /// </summary>
    public static async Task<CreateInternetGatewayResult> CreateInternetGatewayAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateInternetGatewayAsync(
                new CreateInternetGatewayRequest());
            return new CreateInternetGatewayResult(
                InternetGatewayId: resp.InternetGateway.InternetGatewayId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create internet gateway");
        }
    }

    /// <summary>
    /// Delete an internet gateway.
    /// </summary>
    public static async Task DeleteInternetGatewayAsync(
        string internetGatewayId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteInternetGatewayAsync(
                new DeleteInternetGatewayRequest
                {
                    InternetGatewayId = internetGatewayId
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete internet gateway '{internetGatewayId}'");
        }
    }

    /// <summary>
    /// Attach an internet gateway to a VPC.
    /// </summary>
    public static async Task AttachInternetGatewayAsync(
        string internetGatewayId,
        string vpcId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachInternetGatewayAsync(
                new AttachInternetGatewayRequest
                {
                    InternetGatewayId = internetGatewayId,
                    VpcId = vpcId
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach internet gateway '{internetGatewayId}' to VPC '{vpcId}'");
        }
    }

    /// <summary>
    /// Detach an internet gateway from a VPC.
    /// </summary>
    public static async Task DetachInternetGatewayAsync(
        string internetGatewayId,
        string vpcId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachInternetGatewayAsync(
                new DetachInternetGatewayRequest
                {
                    InternetGatewayId = internetGatewayId,
                    VpcId = vpcId
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach internet gateway '{internetGatewayId}' from VPC '{vpcId}'");
        }
    }

    // ── Route Tables ─────────────────────────────────────────────────

    /// <summary>
    /// Describe route tables.
    /// </summary>
    public static async Task<List<RouteTableResult>> DescribeRouteTablesAsync(
        List<string>? routeTableIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeRouteTablesRequest();
        if (routeTableIds != null) request.RouteTableIds = routeTableIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeRouteTablesAsync(request);
            return resp.RouteTables.Select(rt => new RouteTableResult(
                RouteTableId: rt.RouteTableId,
                VpcId: rt.VpcId)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe route tables");
        }
    }

    /// <summary>
    /// Create a route table.
    /// </summary>
    public static async Task<CreateRouteTableResult> CreateRouteTableAsync(
        string vpcId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRouteTableAsync(
                new CreateRouteTableRequest { VpcId = vpcId });
            return new CreateRouteTableResult(
                RouteTableId: resp.RouteTable.RouteTableId,
                VpcId: resp.RouteTable.VpcId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create route table");
        }
    }

    /// <summary>
    /// Delete a route table.
    /// </summary>
    public static async Task DeleteRouteTableAsync(
        string routeTableId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRouteTableAsync(
                new DeleteRouteTableRequest { RouteTableId = routeTableId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete route table '{routeTableId}'");
        }
    }

    /// <summary>
    /// Create a route in a route table.
    /// </summary>
    public static async Task CreateRouteAsync(
        string routeTableId,
        string destinationCidrBlock,
        string? gatewayId = null,
        string? natGatewayId = null,
        string? instanceId = null,
        string? networkInterfaceId = null,
        string? vpcPeeringConnectionId = null,
        string? transitGatewayId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateRouteRequest
        {
            RouteTableId = routeTableId,
            DestinationCidrBlock = destinationCidrBlock
        };
        if (gatewayId != null) request.GatewayId = gatewayId;
        if (natGatewayId != null) request.NatGatewayId = natGatewayId;
        if (instanceId != null) request.InstanceId = instanceId;
        if (networkInterfaceId != null) request.NetworkInterfaceId = networkInterfaceId;
        if (vpcPeeringConnectionId != null) request.VpcPeeringConnectionId = vpcPeeringConnectionId;
        if (transitGatewayId != null) request.TransitGatewayId = transitGatewayId;

        try
        {
            await client.CreateRouteAsync(request);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create route");
        }
    }

    /// <summary>
    /// Delete a route from a route table.
    /// </summary>
    public static async Task DeleteRouteAsync(
        string routeTableId,
        string destinationCidrBlock,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRouteAsync(new DeleteRouteRequest
            {
                RouteTableId = routeTableId,
                DestinationCidrBlock = destinationCidrBlock
            });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete route");
        }
    }

    // ── NAT Gateways ─────────────────────────────────────────────────

    /// <summary>
    /// Describe NAT gateways.
    /// </summary>
    public static async Task<List<NatGatewayResult>> DescribeNatGatewaysAsync(
        List<string>? natGatewayIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeNatGatewaysRequest();
        if (natGatewayIds != null) request.NatGatewayIds = natGatewayIds;
        if (filters != null) request.Filter = filters;

        try
        {
            var results = new List<NatGatewayResult>();
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeNatGatewaysAsync(request);
                results.AddRange(resp.NatGateways.Select(ng => new NatGatewayResult(
                    NatGatewayId: ng.NatGatewayId,
                    VpcId: ng.VpcId,
                    SubnetId: ng.SubnetId,
                    State: ng.State?.Value)));
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe NAT gateways");
        }
    }

    /// <summary>
    /// Create a NAT gateway.
    /// </summary>
    public static async Task<CreateNatGatewayResult> CreateNatGatewayAsync(
        string subnetId,
        string allocationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateNatGatewayAsync(new CreateNatGatewayRequest
            {
                SubnetId = subnetId,
                AllocationId = allocationId
            });
            return new CreateNatGatewayResult(
                NatGatewayId: resp.NatGateway.NatGatewayId,
                SubnetId: resp.NatGateway.SubnetId);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create NAT gateway");
        }
    }

    /// <summary>
    /// Delete a NAT gateway.
    /// </summary>
    public static async Task DeleteNatGatewayAsync(
        string natGatewayId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteNatGatewayAsync(
                new DeleteNatGatewayRequest { NatGatewayId = natGatewayId });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete NAT gateway '{natGatewayId}'");
        }
    }

    // ── Network Interfaces ───────────────────────────────────────────

    /// <summary>
    /// Describe network interfaces.
    /// </summary>
    public static async Task<List<NetworkInterfaceResult>> DescribeNetworkInterfacesAsync(
        List<string>? networkInterfaceIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeNetworkInterfacesRequest();
        if (networkInterfaceIds != null) request.NetworkInterfaceIds = networkInterfaceIds;
        if (filters != null) request.Filters = filters;

        try
        {
            var resp = await client.DescribeNetworkInterfacesAsync(request);
            return resp.NetworkInterfaces.Select(ni => new NetworkInterfaceResult(
                NetworkInterfaceId: ni.NetworkInterfaceId,
                SubnetId: ni.SubnetId,
                VpcId: ni.VpcId,
                PrivateIpAddress: ni.PrivateIpAddress,
                Status: ni.Status?.Value)).ToList();
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe network interfaces");
        }
    }

    // ── Instance Attribute & Types ───────────────────────────────────

    /// <summary>
    /// Modify an instance attribute.
    /// </summary>
    public static async Task ModifyInstanceAttributeAsync(
        string instanceId,
        string? instanceType = null,
        bool? sourceDestCheck = null,
        List<string>? groups = null,
        string? userData = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ModifyInstanceAttributeRequest
        {
            InstanceId = instanceId
        };
        if (instanceType != null) request.InstanceType = instanceType;
        if (sourceDestCheck.HasValue)
            request.SourceDestCheck = sourceDestCheck.Value;
        if (groups != null) request.Groups = groups;
        if (userData != null) request.UserData = userData;

        try
        {
            await client.ModifyInstanceAttributeAsync(request);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify instance attribute for '{instanceId}'");
        }
    }

    /// <summary>
    /// Describe available instance types.
    /// </summary>
    public static async Task<List<InstanceTypeResult>> DescribeInstanceTypesAsync(
        List<string>? instanceTypes = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeInstanceTypesRequest();
        if (instanceTypes != null)
            request.InstanceTypes = instanceTypes;
        if (filters != null) request.Filters = filters;

        try
        {
            var results = new List<InstanceTypeResult>();
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeInstanceTypesAsync(request);
                results.AddRange(resp.InstanceTypes.Select(it => new InstanceTypeResult(
                    InstanceType: it.InstanceType?.Value,
                    VCpuCount: it.VCpuInfo?.DefaultVCpus,
                    MemoryMiB: it.MemoryInfo?.SizeInMiB)));
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe instance types");
        }
    }

    // ── VPC Peering ─────────────────────────────────────────────────

    /// <summary>
    /// Create a VPC peering connection.
    /// </summary>
    public static async Task<VpcPeeringConnectionResult> CreateVpcPeeringConnectionAsync(
        string vpcId,
        string peerVpcId,
        string? peerOwnerId = null,
        string? peerRegion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateVpcPeeringConnectionRequest
        {
            VpcId = vpcId,
            PeerVpcId = peerVpcId
        };
        if (peerOwnerId != null) request.PeerOwnerId = peerOwnerId;
        if (peerRegion != null) request.PeerRegion = peerRegion;

        try
        {
            var resp = await client.CreateVpcPeeringConnectionAsync(request);
            return new VpcPeeringConnectionResult(
                VpcPeeringConnectionId: resp.VpcPeeringConnection?.VpcPeeringConnectionId,
                Status: resp.VpcPeeringConnection?.Status?.Code?.Value);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create VPC peering connection");
        }
    }

    /// <summary>
    /// Accept a VPC peering connection.
    /// </summary>
    public static async Task AcceptVpcPeeringConnectionAsync(
        string vpcPeeringConnectionId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AcceptVpcPeeringConnectionAsync(
                new AcceptVpcPeeringConnectionRequest
                {
                    VpcPeeringConnectionId = vpcPeeringConnectionId
                });
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to accept VPC peering connection '{vpcPeeringConnectionId}'");
        }
    }

    /// <summary>
    /// Replace an existing route in a route table.
    /// </summary>
    public static async Task ReplaceRouteAsync(
        string routeTableId,
        string destinationCidrBlock,
        string? gatewayId = null,
        string? natGatewayId = null,
        string? instanceId = null,
        string? networkInterfaceId = null,
        string? vpcPeeringConnectionId = null,
        string? transitGatewayId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ReplaceRouteRequest
        {
            RouteTableId = routeTableId,
            DestinationCidrBlock = destinationCidrBlock
        };
        if (gatewayId != null) request.GatewayId = gatewayId;
        if (natGatewayId != null) request.NatGatewayId = natGatewayId;
        if (instanceId != null) request.InstanceId = instanceId;
        if (networkInterfaceId != null) request.NetworkInterfaceId = networkInterfaceId;
        if (vpcPeeringConnectionId != null) request.VpcPeeringConnectionId = vpcPeeringConnectionId;
        if (transitGatewayId != null) request.TransitGatewayId = transitGatewayId;

        try
        {
            await client.ReplaceRouteAsync(request);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to replace route");
        }
    }

    /// <summary>
    /// Create a Transit Gateway VPC attachment.
    /// </summary>
    public static async Task<TransitGatewayVpcAttachmentResult> CreateTransitGatewayVpcAttachmentAsync(
        string transitGatewayId,
        string vpcId,
        List<string> subnetIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTransitGatewayVpcAttachmentAsync(
                new CreateTransitGatewayVpcAttachmentRequest
                {
                    TransitGatewayId = transitGatewayId,
                    VpcId = vpcId,
                    SubnetIds = subnetIds
                });
            return new TransitGatewayVpcAttachmentResult(
                TransitGatewayAttachmentId: resp.TransitGatewayVpcAttachment?.TransitGatewayAttachmentId,
                State: resp.TransitGatewayVpcAttachment?.State?.Value);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create Transit Gateway VPC attachment");
        }
    }

    /// <summary>
    /// Create a VPC endpoint.
    /// </summary>
    public static async Task<VpcEndpointResult> CreateVpcEndpointAsync(
        string vpcId,
        string serviceName,
        string? vpcEndpointType = null,
        List<string>? subnetIds = null,
        List<string>? securityGroupIds = null,
        List<string>? routeTableIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateVpcEndpointRequest
        {
            VpcId = vpcId,
            ServiceName = serviceName
        };
        if (vpcEndpointType != null) request.VpcEndpointType = vpcEndpointType;
        if (subnetIds != null) request.SubnetIds = subnetIds;
        if (securityGroupIds != null) request.SecurityGroupIds = securityGroupIds;
        if (routeTableIds != null) request.RouteTableIds = routeTableIds;

        try
        {
            var resp = await client.CreateVpcEndpointAsync(request);
            return new VpcEndpointResult(
                VpcEndpointId: resp.VpcEndpoint?.VpcEndpointId,
                State: resp.VpcEndpoint?.State?.Value);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create VPC endpoint");
        }
    }

    /// <summary>
    /// Describe network ACLs.
    /// </summary>
    public static async Task<DescribeNetworkAclsResult> DescribeNetworkAclsAsync(
        List<string>? networkAclIds = null,
        List<string>? vpcIds = null,
        List<Filter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeNetworkAclsRequest();
        if (networkAclIds != null) request.NetworkAclIds = networkAclIds;
        if (filters != null) request.Filters = filters;
        if (vpcIds != null)
        {
            var vpcFilter = new Filter { Name = "vpc-id", Values = vpcIds };
            request.Filters ??= new List<Filter>();
            request.Filters.Add(vpcFilter);
        }

        try
        {
            var resp = await client.DescribeNetworkAclsAsync(request);
            return new DescribeNetworkAclsResult(NetworkAcls: resp.NetworkAcls);
        }
        catch (AmazonEC2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe network ACLs");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="DescribeInstancesAsync"/>.</summary>
    public static List<Ec2InstanceResult> DescribeInstances(List<string>? instanceIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeInstancesAsync(instanceIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RunInstancesAsync"/>.</summary>
    public static RunInstancesResult RunInstances(string imageId, string instanceType, int minCount = 1, int maxCount = 1, string? keyName = null, List<string>? securityGroupIds = null, string? subnetId = null, string? userData = null, RegionEndpoint? region = null)
        => RunInstancesAsync(imageId, instanceType, minCount, maxCount, keyName, securityGroupIds, subnetId, userData, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartInstancesAsync"/>.</summary>
    public static List<InstanceStateChangeResult> StartInstances(List<string> instanceIds, RegionEndpoint? region = null)
        => StartInstancesAsync(instanceIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopInstancesAsync"/>.</summary>
    public static List<InstanceStateChangeResult> StopInstances(List<string> instanceIds, bool force = false, RegionEndpoint? region = null)
        => StopInstancesAsync(instanceIds, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TerminateInstancesAsync"/>.</summary>
    public static List<InstanceStateChangeResult> TerminateInstances(List<string> instanceIds, RegionEndpoint? region = null)
        => TerminateInstancesAsync(instanceIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RebootInstancesAsync"/>.</summary>
    public static void RebootInstances(List<string> instanceIds, RegionEndpoint? region = null)
        => RebootInstancesAsync(instanceIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSecurityGroupsAsync"/>.</summary>
    public static List<SecurityGroupResult> DescribeSecurityGroups(List<string>? groupIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeSecurityGroupsAsync(groupIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSecurityGroupAsync"/>.</summary>
    public static CreateSecurityGroupResult CreateSecurityGroup(string groupName, string description, string? vpcId = null, RegionEndpoint? region = null)
        => CreateSecurityGroupAsync(groupName, description, vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSecurityGroupAsync"/>.</summary>
    public static void DeleteSecurityGroup(string groupId, RegionEndpoint? region = null)
        => DeleteSecurityGroupAsync(groupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AuthorizeSecurityGroupIngressAsync"/>.</summary>
    public static void AuthorizeSecurityGroupIngress(string groupId, List<IpPermission> ipPermissions, RegionEndpoint? region = null)
        => AuthorizeSecurityGroupIngressAsync(groupId, ipPermissions, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RevokeSecurityGroupIngressAsync"/>.</summary>
    public static void RevokeSecurityGroupIngress(string groupId, List<IpPermission> ipPermissions, RegionEndpoint? region = null)
        => RevokeSecurityGroupIngressAsync(groupId, ipPermissions, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeVpcsAsync"/>.</summary>
    public static List<VpcResult> DescribeVpcs(List<string>? vpcIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeVpcsAsync(vpcIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVpcAsync"/>.</summary>
    public static CreateVpcResult CreateVpc(string cidrBlock, RegionEndpoint? region = null)
        => CreateVpcAsync(cidrBlock, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteVpcAsync"/>.</summary>
    public static void DeleteVpc(string vpcId, RegionEndpoint? region = null)
        => DeleteVpcAsync(vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSubnetsAsync"/>.</summary>
    public static List<SubnetResult> DescribeSubnets(List<string>? subnetIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeSubnetsAsync(subnetIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSubnetAsync"/>.</summary>
    public static CreateSubnetResult CreateSubnet(string vpcId, string cidrBlock, string? availabilityZone = null, RegionEndpoint? region = null)
        => CreateSubnetAsync(vpcId, cidrBlock, availabilityZone, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSubnetAsync"/>.</summary>
    public static void DeleteSubnet(string subnetId, RegionEndpoint? region = null)
        => DeleteSubnetAsync(subnetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AllocateAddressAsync"/>.</summary>
    public static AllocateAddressResult AllocateAddress(string domain = "vpc", RegionEndpoint? region = null)
        => AllocateAddressAsync(domain, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReleaseAddressAsync"/>.</summary>
    public static void ReleaseAddress(string allocationId, RegionEndpoint? region = null)
        => ReleaseAddressAsync(allocationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateAddressAsync"/>.</summary>
    public static AssociateAddressResult AssociateAddress(string allocationId, string? instanceId = null, string? networkInterfaceId = null, RegionEndpoint? region = null)
        => AssociateAddressAsync(allocationId, instanceId, networkInterfaceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateAddressAsync"/>.</summary>
    public static void DisassociateAddress(string associationId, RegionEndpoint? region = null)
        => DisassociateAddressAsync(associationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTagsAsync"/>.</summary>
    public static void CreateTags(List<string> resourceIds, List<Amazon.EC2.Model.Tag> tags, RegionEndpoint? region = null)
        => CreateTagsAsync(resourceIds, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTagsAsync"/>.</summary>
    public static void DeleteTags(List<string> resourceIds, List<Amazon.EC2.Model.Tag> tags, RegionEndpoint? region = null)
        => DeleteTagsAsync(resourceIds, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeImagesAsync"/>.</summary>
    public static List<ImageResult> DescribeImages(List<string>? imageIds = null, List<string>? owners = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeImagesAsync(imageIds, owners, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateImageAsync"/>.</summary>
    public static CreateImageResult CreateImage(string instanceId, string name, string? description = null, bool noReboot = false, RegionEndpoint? region = null)
        => CreateImageAsync(instanceId, name, description, noReboot, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeVolumesAsync"/>.</summary>
    public static List<VolumeResult> DescribeVolumes(List<string>? volumeIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeVolumesAsync(volumeIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVolumeAsync"/>.</summary>
    public static CreateVolumeResult CreateVolume(string availabilityZone, int? size = null, string? volumeType = null, string? snapshotId = null, RegionEndpoint? region = null)
        => CreateVolumeAsync(availabilityZone, size, volumeType, snapshotId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteVolumeAsync"/>.</summary>
    public static void DeleteVolume(string volumeId, RegionEndpoint? region = null)
        => DeleteVolumeAsync(volumeId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachVolumeAsync"/>.</summary>
    public static AttachVolumeResult AttachVolume(string volumeId, string instanceId, string device, RegionEndpoint? region = null)
        => AttachVolumeAsync(volumeId, instanceId, device, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachVolumeAsync"/>.</summary>
    public static DetachVolumeResult DetachVolume(string volumeId, string? instanceId = null, bool force = false, RegionEndpoint? region = null)
        => DetachVolumeAsync(volumeId, instanceId, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSnapshotAsync"/>.</summary>
    public static SnapshotResult CreateSnapshot(string volumeId, string? description = null, RegionEndpoint? region = null)
        => CreateSnapshotAsync(volumeId, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSnapshotAsync"/>.</summary>
    public static void DeleteSnapshot(string snapshotId, RegionEndpoint? region = null)
        => DeleteSnapshotAsync(snapshotId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSnapshotsAsync"/>.</summary>
    public static List<SnapshotResult> DescribeSnapshots(List<string>? snapshotIds = null, List<string>? ownerIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeSnapshotsAsync(snapshotIds, ownerIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeKeyPairsAsync"/>.</summary>
    public static List<KeyPairResult> DescribeKeyPairs(List<string>? keyNames = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeKeyPairsAsync(keyNames, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateKeyPairAsync"/>.</summary>
    public static CreateKeyPairResult CreateKeyPair(string keyName, string? keyType = null, RegionEndpoint? region = null)
        => CreateKeyPairAsync(keyName, keyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteKeyPairAsync"/>.</summary>
    public static void DeleteKeyPair(string keyName, RegionEndpoint? region = null)
        => DeleteKeyPairAsync(keyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeInternetGatewaysAsync"/>.</summary>
    public static List<InternetGatewayResult> DescribeInternetGateways(List<string>? internetGatewayIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeInternetGatewaysAsync(internetGatewayIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateInternetGatewayAsync"/>.</summary>
    public static CreateInternetGatewayResult CreateInternetGateway(RegionEndpoint? region = null)
        => CreateInternetGatewayAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteInternetGatewayAsync"/>.</summary>
    public static void DeleteInternetGateway(string internetGatewayId, RegionEndpoint? region = null)
        => DeleteInternetGatewayAsync(internetGatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachInternetGatewayAsync"/>.</summary>
    public static void AttachInternetGateway(string internetGatewayId, string vpcId, RegionEndpoint? region = null)
        => AttachInternetGatewayAsync(internetGatewayId, vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachInternetGatewayAsync"/>.</summary>
    public static void DetachInternetGateway(string internetGatewayId, string vpcId, RegionEndpoint? region = null)
        => DetachInternetGatewayAsync(internetGatewayId, vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeRouteTablesAsync"/>.</summary>
    public static List<RouteTableResult> DescribeRouteTables(List<string>? routeTableIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeRouteTablesAsync(routeTableIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRouteTableAsync"/>.</summary>
    public static CreateRouteTableResult CreateRouteTable(string vpcId, RegionEndpoint? region = null)
        => CreateRouteTableAsync(vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRouteTableAsync"/>.</summary>
    public static void DeleteRouteTable(string routeTableId, RegionEndpoint? region = null)
        => DeleteRouteTableAsync(routeTableId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRouteAsync"/>.</summary>
    public static void CreateRoute(string routeTableId, string destinationCidrBlock, string? gatewayId = null, string? natGatewayId = null, string? instanceId = null, string? networkInterfaceId = null, string? vpcPeeringConnectionId = null, string? transitGatewayId = null, RegionEndpoint? region = null)
        => CreateRouteAsync(routeTableId, destinationCidrBlock, gatewayId, natGatewayId, instanceId, networkInterfaceId, vpcPeeringConnectionId, transitGatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRouteAsync"/>.</summary>
    public static void DeleteRoute(string routeTableId, string destinationCidrBlock, RegionEndpoint? region = null)
        => DeleteRouteAsync(routeTableId, destinationCidrBlock, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeNatGatewaysAsync"/>.</summary>
    public static List<NatGatewayResult> DescribeNatGateways(List<string>? natGatewayIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeNatGatewaysAsync(natGatewayIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateNatGatewayAsync"/>.</summary>
    public static CreateNatGatewayResult CreateNatGateway(string subnetId, string allocationId, RegionEndpoint? region = null)
        => CreateNatGatewayAsync(subnetId, allocationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteNatGatewayAsync"/>.</summary>
    public static void DeleteNatGateway(string natGatewayId, RegionEndpoint? region = null)
        => DeleteNatGatewayAsync(natGatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeNetworkInterfacesAsync"/>.</summary>
    public static List<NetworkInterfaceResult> DescribeNetworkInterfaces(List<string>? networkInterfaceIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeNetworkInterfacesAsync(networkInterfaceIds, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyInstanceAttributeAsync"/>.</summary>
    public static void ModifyInstanceAttribute(string instanceId, string? instanceType = null, bool? sourceDestCheck = null, List<string>? groups = null, string? userData = null, RegionEndpoint? region = null)
        => ModifyInstanceAttributeAsync(instanceId, instanceType, sourceDestCheck, groups, userData, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeInstanceTypesAsync"/>.</summary>
    public static List<InstanceTypeResult> DescribeInstanceTypes(List<string>? instanceTypes = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeInstanceTypesAsync(instanceTypes, filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVpcPeeringConnectionAsync"/>.</summary>
    public static VpcPeeringConnectionResult CreateVpcPeeringConnection(string vpcId, string peerVpcId, string? peerOwnerId = null, string? peerRegion = null, RegionEndpoint? region = null)
        => CreateVpcPeeringConnectionAsync(vpcId, peerVpcId, peerOwnerId, peerRegion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AcceptVpcPeeringConnectionAsync"/>.</summary>
    public static void AcceptVpcPeeringConnection(string vpcPeeringConnectionId, RegionEndpoint? region = null)
        => AcceptVpcPeeringConnectionAsync(vpcPeeringConnectionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReplaceRouteAsync"/>.</summary>
    public static void ReplaceRoute(string routeTableId, string destinationCidrBlock, string? gatewayId = null, string? natGatewayId = null, string? instanceId = null, string? networkInterfaceId = null, string? vpcPeeringConnectionId = null, string? transitGatewayId = null, RegionEndpoint? region = null)
        => ReplaceRouteAsync(routeTableId, destinationCidrBlock, gatewayId, natGatewayId, instanceId, networkInterfaceId, vpcPeeringConnectionId, transitGatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTransitGatewayVpcAttachmentAsync"/>.</summary>
    public static TransitGatewayVpcAttachmentResult CreateTransitGatewayVpcAttachment(string transitGatewayId, string vpcId, List<string> subnetIds, RegionEndpoint? region = null)
        => CreateTransitGatewayVpcAttachmentAsync(transitGatewayId, vpcId, subnetIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVpcEndpointAsync"/>.</summary>
    public static VpcEndpointResult CreateVpcEndpoint(string vpcId, string serviceName, string? vpcEndpointType = null, List<string>? subnetIds = null, List<string>? securityGroupIds = null, List<string>? routeTableIds = null, RegionEndpoint? region = null)
        => CreateVpcEndpointAsync(vpcId, serviceName, vpcEndpointType, subnetIds, securityGroupIds, routeTableIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeNetworkAclsAsync"/>.</summary>
    public static DescribeNetworkAclsResult DescribeNetworkAcls(List<string>? networkAclIds = null, List<string>? vpcIds = null, List<Filter>? filters = null, RegionEndpoint? region = null)
        => DescribeNetworkAclsAsync(networkAclIds, vpcIds, filters, region).GetAwaiter().GetResult();

}
