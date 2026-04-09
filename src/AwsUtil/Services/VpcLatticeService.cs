using Amazon;
using Amazon.VPCLattice;
using Amazon.VPCLattice.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record VlCreateServiceResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? DnsName = null, string? Status = null);

public sealed record VlDeleteServiceResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Status = null);

public sealed record VlGetServiceResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? DnsName = null, string? Status = null,
    string? CustomDomainName = null, string? CertificateArn = null);

public sealed record VlListServicesResult(
    List<ServiceSummary>? Items = null, string? NextToken = null);

public sealed record VlUpdateServiceResult(
    string? Id = null, string? Arn = null, string? Name = null);

public sealed record VlCreateServiceNetworkResult(
    string? Id = null, string? Arn = null, string? Name = null);

public sealed record VlDeleteServiceNetworkResult(bool Success = true);

public sealed record VlGetServiceNetworkResult(
    string? Id = null, string? Arn = null, string? Name = null,
    long? NumberOfAssociatedServices = null,
    long? NumberOfAssociatedVPCs = null);

public sealed record VlListServiceNetworksResult(
    List<ServiceNetworkSummary>? Items = null, string? NextToken = null);

public sealed record VlUpdateServiceNetworkResult(
    string? Id = null, string? Arn = null, string? Name = null);

public sealed record VlCreateServiceNetworkServiceAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null,
    string? DnsEntry = null);

public sealed record VlDeleteServiceNetworkServiceAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null);

public sealed record VlGetServiceNetworkServiceAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null,
    string? ServiceId = null, string? ServiceNetworkId = null);

public sealed record VlListServiceNetworkServiceAssociationsResult(
    List<ServiceNetworkServiceAssociationSummary>? Items = null,
    string? NextToken = null);

public sealed record VlCreateServiceNetworkVpcAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null);

public sealed record VlDeleteServiceNetworkVpcAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null);

public sealed record VlGetServiceNetworkVpcAssociationResult(
    string? Id = null, string? Arn = null, string? Status = null,
    string? VpcId = null, string? ServiceNetworkId = null,
    List<string>? SecurityGroupIds = null);

public sealed record VlListServiceNetworkVpcAssociationsResult(
    List<ServiceNetworkVpcAssociationSummary>? Items = null,
    string? NextToken = null);

public sealed record VlUpdateServiceNetworkVpcAssociationResult(
    string? Id = null, string? Arn = null,
    List<string>? SecurityGroupIds = null);

public sealed record VlCreateTargetGroupResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Type = null, string? Status = null);

public sealed record VlDeleteTargetGroupResult(
    string? Id = null, string? Arn = null, string? Status = null);

public sealed record VlGetTargetGroupResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Type = null, string? Status = null,
    TargetGroupConfig? Config = null);

public sealed record VlListTargetGroupsResult(
    List<TargetGroupSummary>? Items = null, string? NextToken = null);

public sealed record VlUpdateTargetGroupResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Type = null, string? Status = null);

public sealed record VlRegisterTargetsResult(
    List<Target>? Successful = null,
    List<TargetFailure>? Unsuccessful = null);

public sealed record VlDeregisterTargetsResult(
    List<Target>? Successful = null,
    List<TargetFailure>? Unsuccessful = null);

public sealed record VlListTargetsResult(
    List<TargetSummary>? Items = null, string? NextToken = null);

public sealed record VlCreateListenerResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Protocol = null, int? Port = null);

public sealed record VlDeleteListenerResult(bool Success = true);

public sealed record VlGetListenerResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Protocol = null, int? Port = null,
    string? ServiceId = null, string? ServiceArn = null,
    RuleAction? DefaultAction = null);

public sealed record VlListListenersResult(
    List<ListenerSummary>? Items = null, string? NextToken = null);

public sealed record VlUpdateListenerResult(
    string? Id = null, string? Arn = null, string? Name = null,
    string? Protocol = null, int? Port = null,
    RuleAction? DefaultAction = null);

public sealed record VlCreateRuleResult(
    string? Id = null, string? Arn = null, string? Name = null,
    int? Priority = null);

public sealed record VlDeleteRuleResult(bool Success = true);

public sealed record VlGetRuleResult(
    string? Id = null, string? Arn = null, string? Name = null,
    int? Priority = null, bool? IsDefault = null,
    RuleMatch? Match = null, RuleAction? Action = null);

public sealed record VlListRulesResult(
    List<RuleSummary>? Items = null, string? NextToken = null);

public sealed record VlUpdateRuleResult(
    string? Id = null, string? Arn = null, string? Name = null,
    int? Priority = null);

public sealed record VlCreateAccessLogSubscriptionResult(
    string? Id = null, string? Arn = null,
    string? DestinationArn = null);

public sealed record VlDeleteAccessLogSubscriptionResult(bool Success = true);

public sealed record VlGetAccessLogSubscriptionResult(
    string? Id = null, string? Arn = null,
    string? ResourceId = null, string? DestinationArn = null);

public sealed record VlListAccessLogSubscriptionsResult(
    List<AccessLogSubscriptionSummary>? Items = null,
    string? NextToken = null);

public sealed record VlUpdateAccessLogSubscriptionResult(
    string? Id = null, string? Arn = null,
    string? DestinationArn = null);

public sealed record VlGetAuthPolicyResult(
    string? Policy = null, string? State = null);

public sealed record VlPutAuthPolicyResult(
    string? Policy = null, string? State = null);

public sealed record VlDeleteAuthPolicyResult(bool Success = true);

public sealed record VlGetResourcePolicyResult(string? Policy = null);

public sealed record VlPutResourcePolicyResult(bool Success = true);

public sealed record VlDeleteResourcePolicyResult(bool Success = true);

public sealed record VlTagResourceResult(bool Success = true);
public sealed record VlUntagResourceResult(bool Success = true);

public sealed record VlListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon VPC Lattice.
/// </summary>
public static class VpcLatticeService
{
    private static AmazonVPCLatticeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonVPCLatticeClient>(region);

    // ── Services ────────────────────────────────────────────────────

    /// <summary>Create a VPC Lattice service.</summary>
    public static async Task<VlCreateServiceResult> CreateServiceAsync(
        CreateServiceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServiceAsync(request);
            return new VlCreateServiceResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                DnsName: resp.DnsEntry?.DomainName,
                Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC Lattice service");
        }
    }

    /// <summary>Delete a VPC Lattice service.</summary>
    public static async Task<VlDeleteServiceResult> DeleteServiceAsync(
        string serviceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteServiceAsync(
                new DeleteServiceRequest
                {
                    ServiceIdentifier = serviceIdentifier
                });
            return new VlDeleteServiceResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete VPC Lattice service '{serviceIdentifier}'");
        }
    }

    /// <summary>Get a VPC Lattice service.</summary>
    public static async Task<VlGetServiceResult> GetServiceAsync(
        string serviceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetServiceAsync(
                new GetServiceRequest
                {
                    ServiceIdentifier = serviceIdentifier
                });
            return new VlGetServiceResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                DnsName: resp.DnsEntry?.DomainName,
                Status: resp.Status?.Value,
                CustomDomainName: resp.CustomDomainName,
                CertificateArn: resp.CertificateArn);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get VPC Lattice service '{serviceIdentifier}'");
        }
    }

    /// <summary>List VPC Lattice services.</summary>
    public static async Task<VlListServicesResult> ListServicesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServicesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListServicesAsync(request);
            return new VlListServicesResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC Lattice services");
        }
    }

    /// <summary>Update a VPC Lattice service.</summary>
    public static async Task<VlUpdateServiceResult> UpdateServiceAsync(
        UpdateServiceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServiceAsync(request);
            return new VlUpdateServiceResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC Lattice service");
        }
    }

    // ── Service Networks ────────────────────────────────────────────

    /// <summary>Create a VPC Lattice service network.</summary>
    public static async Task<VlCreateServiceNetworkResult>
        CreateServiceNetworkAsync(
            CreateServiceNetworkRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServiceNetworkAsync(request);
            return new VlCreateServiceNetworkResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC Lattice service network");
        }
    }

    /// <summary>Delete a VPC Lattice service network.</summary>
    public static async Task<VlDeleteServiceNetworkResult>
        DeleteServiceNetworkAsync(
            string serviceNetworkIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteServiceNetworkAsync(
                new DeleteServiceNetworkRequest
                {
                    ServiceNetworkIdentifier = serviceNetworkIdentifier
                });
            return new VlDeleteServiceNetworkResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete service network '{serviceNetworkIdentifier}'");
        }
    }

    /// <summary>Get a VPC Lattice service network.</summary>
    public static async Task<VlGetServiceNetworkResult> GetServiceNetworkAsync(
        string serviceNetworkIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetServiceNetworkAsync(
                new GetServiceNetworkRequest
                {
                    ServiceNetworkIdentifier = serviceNetworkIdentifier
                });
            return new VlGetServiceNetworkResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                NumberOfAssociatedServices: resp.NumberOfAssociatedServices,
                NumberOfAssociatedVPCs: resp.NumberOfAssociatedVPCs);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get service network '{serviceNetworkIdentifier}'");
        }
    }

    /// <summary>List VPC Lattice service networks.</summary>
    public static async Task<VlListServiceNetworksResult>
        ListServiceNetworksAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServiceNetworksRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListServiceNetworksAsync(request);
            return new VlListServiceNetworksResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC Lattice service networks");
        }
    }

    /// <summary>Update a VPC Lattice service network.</summary>
    public static async Task<VlUpdateServiceNetworkResult>
        UpdateServiceNetworkAsync(
            UpdateServiceNetworkRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServiceNetworkAsync(request);
            return new VlUpdateServiceNetworkResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC Lattice service network");
        }
    }

    // ── Service Network / Service Associations ──────────────────────

    /// <summary>Associate a service with a service network.</summary>
    public static async Task<VlCreateServiceNetworkServiceAssociationResult>
        CreateServiceNetworkServiceAssociationAsync(
            CreateServiceNetworkServiceAssociationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .CreateServiceNetworkServiceAssociationAsync(request);
            return new VlCreateServiceNetworkServiceAssociationResult(
                Id: resp.Id, Arn: resp.Arn,
                Status: resp.Status?.Value,
                DnsEntry: resp.DnsEntry?.DomainName);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create service network service association");
        }
    }

    /// <summary>Delete a service network service association.</summary>
    public static async Task<VlDeleteServiceNetworkServiceAssociationResult>
        DeleteServiceNetworkServiceAssociationAsync(
            string serviceNetworkServiceAssociationIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .DeleteServiceNetworkServiceAssociationAsync(
                    new DeleteServiceNetworkServiceAssociationRequest
                    {
                        ServiceNetworkServiceAssociationIdentifier =
                            serviceNetworkServiceAssociationIdentifier
                    });
            return new VlDeleteServiceNetworkServiceAssociationResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete service network service association");
        }
    }

    /// <summary>Get a service network service association.</summary>
    public static async Task<VlGetServiceNetworkServiceAssociationResult>
        GetServiceNetworkServiceAssociationAsync(
            string serviceNetworkServiceAssociationIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .GetServiceNetworkServiceAssociationAsync(
                    new GetServiceNetworkServiceAssociationRequest
                    {
                        ServiceNetworkServiceAssociationIdentifier =
                            serviceNetworkServiceAssociationIdentifier
                    });
            return new VlGetServiceNetworkServiceAssociationResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value,
                ServiceId: resp.ServiceId,
                ServiceNetworkId: resp.ServiceNetworkId);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get service network service association");
        }
    }

    /// <summary>List service network service associations.</summary>
    public static async Task<VlListServiceNetworkServiceAssociationsResult>
        ListServiceNetworkServiceAssociationsAsync(
            string? serviceNetworkIdentifier = null,
            string? serviceIdentifier = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServiceNetworkServiceAssociationsRequest();
        if (serviceNetworkIdentifier != null)
            request.ServiceNetworkIdentifier = serviceNetworkIdentifier;
        if (serviceIdentifier != null)
            request.ServiceIdentifier = serviceIdentifier;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client
                .ListServiceNetworkServiceAssociationsAsync(request);
            return new VlListServiceNetworkServiceAssociationsResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list service network service associations");
        }
    }

    // ── Service Network / VPC Associations ──────────────────────────

    /// <summary>Associate a VPC with a service network.</summary>
    public static async Task<VlCreateServiceNetworkVpcAssociationResult>
        CreateServiceNetworkVpcAssociationAsync(
            CreateServiceNetworkVpcAssociationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .CreateServiceNetworkVpcAssociationAsync(request);
            return new VlCreateServiceNetworkVpcAssociationResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create service network VPC association");
        }
    }

    /// <summary>Delete a service network VPC association.</summary>
    public static async Task<VlDeleteServiceNetworkVpcAssociationResult>
        DeleteServiceNetworkVpcAssociationAsync(
            string serviceNetworkVpcAssociationIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .DeleteServiceNetworkVpcAssociationAsync(
                    new DeleteServiceNetworkVpcAssociationRequest
                    {
                        ServiceNetworkVpcAssociationIdentifier =
                            serviceNetworkVpcAssociationIdentifier
                    });
            return new VlDeleteServiceNetworkVpcAssociationResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete service network VPC association");
        }
    }

    /// <summary>Get a service network VPC association.</summary>
    public static async Task<VlGetServiceNetworkVpcAssociationResult>
        GetServiceNetworkVpcAssociationAsync(
            string serviceNetworkVpcAssociationIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetServiceNetworkVpcAssociationAsync(
                new GetServiceNetworkVpcAssociationRequest
                {
                    ServiceNetworkVpcAssociationIdentifier =
                        serviceNetworkVpcAssociationIdentifier
                });
            return new VlGetServiceNetworkVpcAssociationResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value,
                VpcId: resp.VpcId,
                ServiceNetworkId: resp.ServiceNetworkId,
                SecurityGroupIds: resp.SecurityGroupIds);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get service network VPC association");
        }
    }

    /// <summary>List service network VPC associations.</summary>
    public static async Task<VlListServiceNetworkVpcAssociationsResult>
        ListServiceNetworkVpcAssociationsAsync(
            string? serviceNetworkIdentifier = null,
            string? vpcIdentifier = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServiceNetworkVpcAssociationsRequest();
        if (serviceNetworkIdentifier != null)
            request.ServiceNetworkIdentifier = serviceNetworkIdentifier;
        if (vpcIdentifier != null) request.VpcIdentifier = vpcIdentifier;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client
                .ListServiceNetworkVpcAssociationsAsync(request);
            return new VlListServiceNetworkVpcAssociationsResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list service network VPC associations");
        }
    }

    /// <summary>Update a service network VPC association.</summary>
    public static async Task<VlUpdateServiceNetworkVpcAssociationResult>
        UpdateServiceNetworkVpcAssociationAsync(
            string serviceNetworkVpcAssociationIdentifier,
            List<string> securityGroupIds,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .UpdateServiceNetworkVpcAssociationAsync(
                    new UpdateServiceNetworkVpcAssociationRequest
                    {
                        ServiceNetworkVpcAssociationIdentifier =
                            serviceNetworkVpcAssociationIdentifier,
                        SecurityGroupIds = securityGroupIds
                    });
            return new VlUpdateServiceNetworkVpcAssociationResult(
                Id: resp.Id, Arn: resp.Arn,
                SecurityGroupIds: resp.SecurityGroupIds);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update service network VPC association");
        }
    }

    // ── Target Groups ───────────────────────────────────────────────

    /// <summary>Create a target group.</summary>
    public static async Task<VlCreateTargetGroupResult> CreateTargetGroupAsync(
        CreateTargetGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTargetGroupAsync(request);
            return new VlCreateTargetGroupResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Type: resp.Type?.Value, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC Lattice target group");
        }
    }

    /// <summary>Delete a target group.</summary>
    public static async Task<VlDeleteTargetGroupResult> DeleteTargetGroupAsync(
        string targetGroupIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteTargetGroupAsync(
                new DeleteTargetGroupRequest
                {
                    TargetGroupIdentifier = targetGroupIdentifier
                });
            return new VlDeleteTargetGroupResult(
                Id: resp.Id, Arn: resp.Arn, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete target group '{targetGroupIdentifier}'");
        }
    }

    /// <summary>Get a target group.</summary>
    public static async Task<VlGetTargetGroupResult> GetTargetGroupAsync(
        string targetGroupIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTargetGroupAsync(
                new GetTargetGroupRequest
                {
                    TargetGroupIdentifier = targetGroupIdentifier
                });
            return new VlGetTargetGroupResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Type: resp.Type?.Value, Status: resp.Status?.Value,
                Config: resp.Config);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get target group '{targetGroupIdentifier}'");
        }
    }

    /// <summary>List target groups.</summary>
    public static async Task<VlListTargetGroupsResult> ListTargetGroupsAsync(
        string? targetGroupType = null,
        string? vpcIdentifier = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTargetGroupsRequest();
        if (targetGroupType != null) request.TargetGroupType = targetGroupType;
        if (vpcIdentifier != null) request.VpcIdentifier = vpcIdentifier;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTargetGroupsAsync(request);
            return new VlListTargetGroupsResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC Lattice target groups");
        }
    }

    /// <summary>Update a target group.</summary>
    public static async Task<VlUpdateTargetGroupResult> UpdateTargetGroupAsync(
        UpdateTargetGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateTargetGroupAsync(request);
            return new VlUpdateTargetGroupResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Type: resp.Type?.Value, Status: resp.Status?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC Lattice target group");
        }
    }

    /// <summary>Register targets with a target group.</summary>
    public static async Task<VlRegisterTargetsResult> RegisterTargetsAsync(
        string targetGroupIdentifier,
        List<Target> targets,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RegisterTargetsAsync(
                new RegisterTargetsRequest
                {
                    TargetGroupIdentifier = targetGroupIdentifier,
                    Targets = targets
                });
            return new VlRegisterTargetsResult(
                Successful: resp.Successful,
                Unsuccessful: resp.Unsuccessful);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to register targets in '{targetGroupIdentifier}'");
        }
    }

    /// <summary>Deregister targets from a target group.</summary>
    public static async Task<VlDeregisterTargetsResult> DeregisterTargetsAsync(
        string targetGroupIdentifier,
        List<Target> targets,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeregisterTargetsAsync(
                new DeregisterTargetsRequest
                {
                    TargetGroupIdentifier = targetGroupIdentifier,
                    Targets = targets
                });
            return new VlDeregisterTargetsResult(
                Successful: resp.Successful,
                Unsuccessful: resp.Unsuccessful);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister targets from '{targetGroupIdentifier}'");
        }
    }

    /// <summary>List targets in a target group.</summary>
    public static async Task<VlListTargetsResult> ListTargetsAsync(
        string targetGroupIdentifier,
        List<Target>? targets = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTargetsRequest
        {
            TargetGroupIdentifier = targetGroupIdentifier
        };
        if (targets != null) request.Targets = targets;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTargetsAsync(request);
            return new VlListTargetsResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list targets in '{targetGroupIdentifier}'");
        }
    }

    // ── Listeners ───────────────────────────────────────────────────

    /// <summary>Create a listener.</summary>
    public static async Task<VlCreateListenerResult> CreateListenerAsync(
        CreateListenerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateListenerAsync(request);
            return new VlCreateListenerResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Protocol: resp.Protocol?.Value, Port: resp.Port);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC Lattice listener");
        }
    }

    /// <summary>Delete a listener.</summary>
    public static async Task<VlDeleteListenerResult> DeleteListenerAsync(
        string serviceIdentifier,
        string listenerIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteListenerAsync(new DeleteListenerRequest
            {
                ServiceIdentifier = serviceIdentifier,
                ListenerIdentifier = listenerIdentifier
            });
            return new VlDeleteListenerResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete listener '{listenerIdentifier}'");
        }
    }

    /// <summary>Get a listener.</summary>
    public static async Task<VlGetListenerResult> GetListenerAsync(
        string serviceIdentifier,
        string listenerIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetListenerAsync(new GetListenerRequest
            {
                ServiceIdentifier = serviceIdentifier,
                ListenerIdentifier = listenerIdentifier
            });
            return new VlGetListenerResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Protocol: resp.Protocol?.Value, Port: resp.Port,
                ServiceId: resp.ServiceId, ServiceArn: resp.ServiceArn,
                DefaultAction: resp.DefaultAction);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get listener '{listenerIdentifier}'");
        }
    }

    /// <summary>List listeners for a service.</summary>
    public static async Task<VlListListenersResult> ListListenersAsync(
        string serviceIdentifier,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListListenersRequest
        {
            ServiceIdentifier = serviceIdentifier
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListListenersAsync(request);
            return new VlListListenersResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC Lattice listeners");
        }
    }

    /// <summary>Update a listener.</summary>
    public static async Task<VlUpdateListenerResult> UpdateListenerAsync(
        UpdateListenerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateListenerAsync(request);
            return new VlUpdateListenerResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Protocol: resp.Protocol?.Value, Port: resp.Port,
                DefaultAction: resp.DefaultAction);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC Lattice listener");
        }
    }

    // ── Rules ───────────────────────────────────────────────────────

    /// <summary>Create a listener rule.</summary>
    public static async Task<VlCreateRuleResult> CreateRuleAsync(
        CreateRuleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRuleAsync(request);
            return new VlCreateRuleResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Priority: resp.Priority);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC Lattice rule");
        }
    }

    /// <summary>Delete a listener rule.</summary>
    public static async Task<VlDeleteRuleResult> DeleteRuleAsync(
        string serviceIdentifier,
        string listenerIdentifier,
        string ruleIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRuleAsync(new DeleteRuleRequest
            {
                ServiceIdentifier = serviceIdentifier,
                ListenerIdentifier = listenerIdentifier,
                RuleIdentifier = ruleIdentifier
            });
            return new VlDeleteRuleResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete rule '{ruleIdentifier}'");
        }
    }

    /// <summary>Get a listener rule.</summary>
    public static async Task<VlGetRuleResult> GetRuleAsync(
        string serviceIdentifier,
        string listenerIdentifier,
        string ruleIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRuleAsync(new GetRuleRequest
            {
                ServiceIdentifier = serviceIdentifier,
                ListenerIdentifier = listenerIdentifier,
                RuleIdentifier = ruleIdentifier
            });
            return new VlGetRuleResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Priority: resp.Priority, IsDefault: resp.IsDefault,
                Match: resp.Match, Action: resp.Action);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get rule '{ruleIdentifier}'");
        }
    }

    /// <summary>List listener rules.</summary>
    public static async Task<VlListRulesResult> ListRulesAsync(
        string serviceIdentifier,
        string listenerIdentifier,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRulesRequest
        {
            ServiceIdentifier = serviceIdentifier,
            ListenerIdentifier = listenerIdentifier
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListRulesAsync(request);
            return new VlListRulesResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC Lattice rules");
        }
    }

    /// <summary>Update a listener rule.</summary>
    public static async Task<VlUpdateRuleResult> UpdateRuleAsync(
        UpdateRuleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRuleAsync(request);
            return new VlUpdateRuleResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Priority: resp.Priority);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC Lattice rule");
        }
    }

    // ── Access Log Subscriptions ────────────────────────────────────

    /// <summary>Create an access log subscription.</summary>
    public static async Task<VlCreateAccessLogSubscriptionResult>
        CreateAccessLogSubscriptionAsync(
            CreateAccessLogSubscriptionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .CreateAccessLogSubscriptionAsync(request);
            return new VlCreateAccessLogSubscriptionResult(
                Id: resp.Id, Arn: resp.Arn,
                DestinationArn: resp.DestinationArn);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create access log subscription");
        }
    }

    /// <summary>Delete an access log subscription.</summary>
    public static async Task<VlDeleteAccessLogSubscriptionResult>
        DeleteAccessLogSubscriptionAsync(
            string accessLogSubscriptionIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAccessLogSubscriptionAsync(
                new DeleteAccessLogSubscriptionRequest
                {
                    AccessLogSubscriptionIdentifier =
                        accessLogSubscriptionIdentifier
                });
            return new VlDeleteAccessLogSubscriptionResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete access log subscription");
        }
    }

    /// <summary>Get an access log subscription.</summary>
    public static async Task<VlGetAccessLogSubscriptionResult>
        GetAccessLogSubscriptionAsync(
            string accessLogSubscriptionIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccessLogSubscriptionAsync(
                new GetAccessLogSubscriptionRequest
                {
                    AccessLogSubscriptionIdentifier =
                        accessLogSubscriptionIdentifier
                });
            return new VlGetAccessLogSubscriptionResult(
                Id: resp.Id, Arn: resp.Arn,
                ResourceId: resp.ResourceId,
                DestinationArn: resp.DestinationArn);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get access log subscription");
        }
    }

    /// <summary>List access log subscriptions.</summary>
    public static async Task<VlListAccessLogSubscriptionsResult>
        ListAccessLogSubscriptionsAsync(
            string resourceIdentifier,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAccessLogSubscriptionsRequest
        {
            ResourceIdentifier = resourceIdentifier
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client
                .ListAccessLogSubscriptionsAsync(request);
            return new VlListAccessLogSubscriptionsResult(
                Items: resp.Items, NextToken: resp.NextToken);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list access log subscriptions");
        }
    }

    /// <summary>Update an access log subscription.</summary>
    public static async Task<VlUpdateAccessLogSubscriptionResult>
        UpdateAccessLogSubscriptionAsync(
            string accessLogSubscriptionIdentifier,
            string destinationArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAccessLogSubscriptionAsync(
                new UpdateAccessLogSubscriptionRequest
                {
                    AccessLogSubscriptionIdentifier =
                        accessLogSubscriptionIdentifier,
                    DestinationArn = destinationArn
                });
            return new VlUpdateAccessLogSubscriptionResult(
                Id: resp.Id, Arn: resp.Arn,
                DestinationArn: resp.DestinationArn);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update access log subscription");
        }
    }

    // ── Auth Policy ─────────────────────────────────────────────────

    /// <summary>Get the auth policy for a resource.</summary>
    public static async Task<VlGetAuthPolicyResult> GetAuthPolicyAsync(
        string resourceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAuthPolicyAsync(
                new GetAuthPolicyRequest
                {
                    ResourceIdentifier = resourceIdentifier
                });
            return new VlGetAuthPolicyResult(
                Policy: resp.Policy, State: resp.State?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get auth policy for '{resourceIdentifier}'");
        }
    }

    /// <summary>Put an auth policy on a resource.</summary>
    public static async Task<VlPutAuthPolicyResult> PutAuthPolicyAsync(
        string resourceIdentifier,
        string policy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutAuthPolicyAsync(
                new PutAuthPolicyRequest
                {
                    ResourceIdentifier = resourceIdentifier,
                    Policy = policy
                });
            return new VlPutAuthPolicyResult(
                Policy: resp.Policy, State: resp.State?.Value);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put auth policy for '{resourceIdentifier}'");
        }
    }

    /// <summary>Delete the auth policy for a resource.</summary>
    public static async Task<VlDeleteAuthPolicyResult> DeleteAuthPolicyAsync(
        string resourceIdentifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAuthPolicyAsync(
                new DeleteAuthPolicyRequest
                {
                    ResourceIdentifier = resourceIdentifier
                });
            return new VlDeleteAuthPolicyResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete auth policy for '{resourceIdentifier}'");
        }
    }

    // ── Resource Policy ─────────────────────────────────────────────

    /// <summary>Get the resource policy for a resource.</summary>
    public static async Task<VlGetResourcePolicyResult> GetResourcePolicyAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetResourcePolicyAsync(
                new GetResourcePolicyRequest { ResourceArn = resourceArn });
            return new VlGetResourcePolicyResult(Policy: resp.Policy);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get resource policy for '{resourceArn}'");
        }
    }

    /// <summary>Put a resource policy on a resource.</summary>
    public static async Task<VlPutResourcePolicyResult> PutResourcePolicyAsync(
        string resourceArn,
        string policy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutResourcePolicyAsync(
                new PutResourcePolicyRequest
                {
                    ResourceArn = resourceArn,
                    Policy = policy
                });
            return new VlPutResourcePolicyResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put resource policy for '{resourceArn}'");
        }
    }

    /// <summary>Delete the resource policy for a resource.</summary>
    public static async Task<VlDeleteResourcePolicyResult>
        DeleteResourcePolicyAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteResourcePolicyAsync(
                new DeleteResourcePolicyRequest
                {
                    ResourceArn = resourceArn
                });
            return new VlDeleteResourcePolicyResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete resource policy for '{resourceArn}'");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────

    /// <summary>Tag a VPC Lattice resource.</summary>
    public static async Task<VlTagResourceResult> TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
            return new VlTagResourceResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag VPC Lattice resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a VPC Lattice resource.</summary>
    public static async Task<VlUntagResourceResult> UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = resourceArn,
                TagKeys = tagKeys
            });
            return new VlUntagResourceResult();
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag VPC Lattice resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a VPC Lattice resource.</summary>
    public static async Task<VlListTagsForResourceResult>
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
            return new VlListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonVPCLatticeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for VPC Lattice resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateServiceAsync"/>.</summary>
    public static VlCreateServiceResult CreateService(CreateServiceRequest request, RegionEndpoint? region = null)
        => CreateServiceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceAsync"/>.</summary>
    public static VlDeleteServiceResult DeleteService(string serviceIdentifier, RegionEndpoint? region = null)
        => DeleteServiceAsync(serviceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetServiceAsync"/>.</summary>
    public static VlGetServiceResult GetService(string serviceIdentifier, RegionEndpoint? region = null)
        => GetServiceAsync(serviceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServicesAsync"/>.</summary>
    public static VlListServicesResult ListServices(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListServicesAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServiceAsync"/>.</summary>
    public static VlUpdateServiceResult UpdateService(UpdateServiceRequest request, RegionEndpoint? region = null)
        => UpdateServiceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateServiceNetworkAsync"/>.</summary>
    public static VlCreateServiceNetworkResult CreateServiceNetwork(CreateServiceNetworkRequest request, RegionEndpoint? region = null)
        => CreateServiceNetworkAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceNetworkAsync"/>.</summary>
    public static VlDeleteServiceNetworkResult DeleteServiceNetwork(string serviceNetworkIdentifier, RegionEndpoint? region = null)
        => DeleteServiceNetworkAsync(serviceNetworkIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetServiceNetworkAsync"/>.</summary>
    public static VlGetServiceNetworkResult GetServiceNetwork(string serviceNetworkIdentifier, RegionEndpoint? region = null)
        => GetServiceNetworkAsync(serviceNetworkIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServiceNetworksAsync"/>.</summary>
    public static VlListServiceNetworksResult ListServiceNetworks(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListServiceNetworksAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServiceNetworkAsync"/>.</summary>
    public static VlUpdateServiceNetworkResult UpdateServiceNetwork(UpdateServiceNetworkRequest request, RegionEndpoint? region = null)
        => UpdateServiceNetworkAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateServiceNetworkServiceAssociationAsync"/>.</summary>
    public static VlCreateServiceNetworkServiceAssociationResult CreateServiceNetworkServiceAssociation(CreateServiceNetworkServiceAssociationRequest request, RegionEndpoint? region = null)
        => CreateServiceNetworkServiceAssociationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceNetworkServiceAssociationAsync"/>.</summary>
    public static VlDeleteServiceNetworkServiceAssociationResult DeleteServiceNetworkServiceAssociation(string serviceNetworkServiceAssociationIdentifier, RegionEndpoint? region = null)
        => DeleteServiceNetworkServiceAssociationAsync(serviceNetworkServiceAssociationIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetServiceNetworkServiceAssociationAsync"/>.</summary>
    public static VlGetServiceNetworkServiceAssociationResult GetServiceNetworkServiceAssociation(string serviceNetworkServiceAssociationIdentifier, RegionEndpoint? region = null)
        => GetServiceNetworkServiceAssociationAsync(serviceNetworkServiceAssociationIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServiceNetworkServiceAssociationsAsync"/>.</summary>
    public static VlListServiceNetworkServiceAssociationsResult ListServiceNetworkServiceAssociations(string? serviceNetworkIdentifier = null, string? serviceIdentifier = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListServiceNetworkServiceAssociationsAsync(serviceNetworkIdentifier, serviceIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateServiceNetworkVpcAssociationAsync"/>.</summary>
    public static VlCreateServiceNetworkVpcAssociationResult CreateServiceNetworkVpcAssociation(CreateServiceNetworkVpcAssociationRequest request, RegionEndpoint? region = null)
        => CreateServiceNetworkVpcAssociationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceNetworkVpcAssociationAsync"/>.</summary>
    public static VlDeleteServiceNetworkVpcAssociationResult DeleteServiceNetworkVpcAssociation(string serviceNetworkVpcAssociationIdentifier, RegionEndpoint? region = null)
        => DeleteServiceNetworkVpcAssociationAsync(serviceNetworkVpcAssociationIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetServiceNetworkVpcAssociationAsync"/>.</summary>
    public static VlGetServiceNetworkVpcAssociationResult GetServiceNetworkVpcAssociation(string serviceNetworkVpcAssociationIdentifier, RegionEndpoint? region = null)
        => GetServiceNetworkVpcAssociationAsync(serviceNetworkVpcAssociationIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServiceNetworkVpcAssociationsAsync"/>.</summary>
    public static VlListServiceNetworkVpcAssociationsResult ListServiceNetworkVpcAssociations(string? serviceNetworkIdentifier = null, string? vpcIdentifier = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListServiceNetworkVpcAssociationsAsync(serviceNetworkIdentifier, vpcIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServiceNetworkVpcAssociationAsync"/>.</summary>
    public static VlUpdateServiceNetworkVpcAssociationResult UpdateServiceNetworkVpcAssociation(string serviceNetworkVpcAssociationIdentifier, List<string> securityGroupIds, RegionEndpoint? region = null)
        => UpdateServiceNetworkVpcAssociationAsync(serviceNetworkVpcAssociationIdentifier, securityGroupIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTargetGroupAsync"/>.</summary>
    public static VlCreateTargetGroupResult CreateTargetGroup(CreateTargetGroupRequest request, RegionEndpoint? region = null)
        => CreateTargetGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTargetGroupAsync"/>.</summary>
    public static VlDeleteTargetGroupResult DeleteTargetGroup(string targetGroupIdentifier, RegionEndpoint? region = null)
        => DeleteTargetGroupAsync(targetGroupIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTargetGroupAsync"/>.</summary>
    public static VlGetTargetGroupResult GetTargetGroup(string targetGroupIdentifier, RegionEndpoint? region = null)
        => GetTargetGroupAsync(targetGroupIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTargetGroupsAsync"/>.</summary>
    public static VlListTargetGroupsResult ListTargetGroups(string? targetGroupType = null, string? vpcIdentifier = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTargetGroupsAsync(targetGroupType, vpcIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateTargetGroupAsync"/>.</summary>
    public static VlUpdateTargetGroupResult UpdateTargetGroup(UpdateTargetGroupRequest request, RegionEndpoint? region = null)
        => UpdateTargetGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterTargetsAsync"/>.</summary>
    public static VlRegisterTargetsResult RegisterTargets(string targetGroupIdentifier, List<Target> targets, RegionEndpoint? region = null)
        => RegisterTargetsAsync(targetGroupIdentifier, targets, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterTargetsAsync"/>.</summary>
    public static VlDeregisterTargetsResult DeregisterTargets(string targetGroupIdentifier, List<Target> targets, RegionEndpoint? region = null)
        => DeregisterTargetsAsync(targetGroupIdentifier, targets, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTargetsAsync"/>.</summary>
    public static VlListTargetsResult ListTargets(string targetGroupIdentifier, List<Target>? targets = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTargetsAsync(targetGroupIdentifier, targets, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateListenerAsync"/>.</summary>
    public static VlCreateListenerResult CreateListener(CreateListenerRequest request, RegionEndpoint? region = null)
        => CreateListenerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteListenerAsync"/>.</summary>
    public static VlDeleteListenerResult DeleteListener(string serviceIdentifier, string listenerIdentifier, RegionEndpoint? region = null)
        => DeleteListenerAsync(serviceIdentifier, listenerIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetListenerAsync"/>.</summary>
    public static VlGetListenerResult GetListener(string serviceIdentifier, string listenerIdentifier, RegionEndpoint? region = null)
        => GetListenerAsync(serviceIdentifier, listenerIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListListenersAsync"/>.</summary>
    public static VlListListenersResult ListListeners(string serviceIdentifier, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListListenersAsync(serviceIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateListenerAsync"/>.</summary>
    public static VlUpdateListenerResult UpdateListener(UpdateListenerRequest request, RegionEndpoint? region = null)
        => UpdateListenerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRuleAsync"/>.</summary>
    public static VlCreateRuleResult CreateRule(CreateRuleRequest request, RegionEndpoint? region = null)
        => CreateRuleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRuleAsync"/>.</summary>
    public static VlDeleteRuleResult DeleteRule(string serviceIdentifier, string listenerIdentifier, string ruleIdentifier, RegionEndpoint? region = null)
        => DeleteRuleAsync(serviceIdentifier, listenerIdentifier, ruleIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRuleAsync"/>.</summary>
    public static VlGetRuleResult GetRule(string serviceIdentifier, string listenerIdentifier, string ruleIdentifier, RegionEndpoint? region = null)
        => GetRuleAsync(serviceIdentifier, listenerIdentifier, ruleIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRulesAsync"/>.</summary>
    public static VlListRulesResult ListRules(string serviceIdentifier, string listenerIdentifier, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListRulesAsync(serviceIdentifier, listenerIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRuleAsync"/>.</summary>
    public static VlUpdateRuleResult UpdateRule(UpdateRuleRequest request, RegionEndpoint? region = null)
        => UpdateRuleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAccessLogSubscriptionAsync"/>.</summary>
    public static VlCreateAccessLogSubscriptionResult CreateAccessLogSubscription(CreateAccessLogSubscriptionRequest request, RegionEndpoint? region = null)
        => CreateAccessLogSubscriptionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAccessLogSubscriptionAsync"/>.</summary>
    public static VlDeleteAccessLogSubscriptionResult DeleteAccessLogSubscription(string accessLogSubscriptionIdentifier, RegionEndpoint? region = null)
        => DeleteAccessLogSubscriptionAsync(accessLogSubscriptionIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAccessLogSubscriptionAsync"/>.</summary>
    public static VlGetAccessLogSubscriptionResult GetAccessLogSubscription(string accessLogSubscriptionIdentifier, RegionEndpoint? region = null)
        => GetAccessLogSubscriptionAsync(accessLogSubscriptionIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccessLogSubscriptionsAsync"/>.</summary>
    public static VlListAccessLogSubscriptionsResult ListAccessLogSubscriptions(string resourceIdentifier, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListAccessLogSubscriptionsAsync(resourceIdentifier, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAccessLogSubscriptionAsync"/>.</summary>
    public static VlUpdateAccessLogSubscriptionResult UpdateAccessLogSubscription(string accessLogSubscriptionIdentifier, string destinationArn, RegionEndpoint? region = null)
        => UpdateAccessLogSubscriptionAsync(accessLogSubscriptionIdentifier, destinationArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAuthPolicyAsync"/>.</summary>
    public static VlGetAuthPolicyResult GetAuthPolicy(string resourceIdentifier, RegionEndpoint? region = null)
        => GetAuthPolicyAsync(resourceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutAuthPolicyAsync"/>.</summary>
    public static VlPutAuthPolicyResult PutAuthPolicy(string resourceIdentifier, string policy, RegionEndpoint? region = null)
        => PutAuthPolicyAsync(resourceIdentifier, policy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAuthPolicyAsync"/>.</summary>
    public static VlDeleteAuthPolicyResult DeleteAuthPolicy(string resourceIdentifier, RegionEndpoint? region = null)
        => DeleteAuthPolicyAsync(resourceIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetResourcePolicyAsync"/>.</summary>
    public static VlGetResourcePolicyResult GetResourcePolicy(string resourceArn, RegionEndpoint? region = null)
        => GetResourcePolicyAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutResourcePolicyAsync"/>.</summary>
    public static VlPutResourcePolicyResult PutResourcePolicy(string resourceArn, string policy, RegionEndpoint? region = null)
        => PutResourcePolicyAsync(resourceArn, policy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteResourcePolicyAsync"/>.</summary>
    public static VlDeleteResourcePolicyResult DeleteResourcePolicy(string resourceArn, RegionEndpoint? region = null)
        => DeleteResourcePolicyAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static VlTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static VlUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static VlListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
