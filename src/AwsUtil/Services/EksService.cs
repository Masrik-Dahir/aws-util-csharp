using Amazon;
using Amazon.EKS;
using Amazon.EKS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for EKS operations.
/// </summary>
public sealed record EksCreateClusterResult(
    string? Name = null, string? Arn = null, string? Status = null,
    string? Version = null, string? Endpoint = null);

public sealed record EksDeleteClusterResult(
    string? Name = null, string? Arn = null, string? Status = null);

public sealed record EksDescribeClusterResult(Cluster? Cluster = null);

public sealed record EksListClustersResult(
    List<string>? Clusters = null, string? NextToken = null);

public sealed record EksUpdateClusterVersionResult(Update? Update = null);
public sealed record EksUpdateClusterConfigResult(Update? Update = null);

public sealed record EksCreateNodegroupResult(
    string? NodegroupName = null, string? NodegroupArn = null,
    string? ClusterName = null, string? Status = null);

public sealed record EksDeleteNodegroupResult(
    string? NodegroupName = null, string? NodegroupArn = null,
    string? Status = null);

public sealed record EksDescribeNodegroupResult(Nodegroup? Nodegroup = null);

public sealed record EksListNodegroupsResult(
    List<string>? Nodegroups = null, string? NextToken = null);

public sealed record EksUpdateNodegroupVersionResult(Update? Update = null);
public sealed record EksUpdateNodegroupConfigResult(Update? Update = null);

public sealed record EksCreateFargateProfileResult(
    string? FargateProfileName = null, string? FargateProfileArn = null,
    string? ClusterName = null, string? Status = null);

public sealed record EksDeleteFargateProfileResult(
    string? FargateProfileName = null, string? FargateProfileArn = null,
    string? Status = null);

public sealed record EksDescribeFargateProfileResult(
    FargateProfile? FargateProfile = null);

public sealed record EksListFargateProfilesResult(
    List<string>? FargateProfileNames = null, string? NextToken = null);

public sealed record EksCreateAddonResult(
    string? AddonName = null, string? AddonArn = null,
    string? ClusterName = null, string? Status = null);

public sealed record EksDeleteAddonResult(
    string? AddonName = null, string? AddonArn = null,
    string? Status = null);

public sealed record EksDescribeAddonResult(Addon? Addon = null);

public sealed record EksListAddonsResult(
    List<string>? Addons = null, string? NextToken = null);

public sealed record EksUpdateAddonResult(Update? Update = null);

public sealed record EksTagResourceResult(bool Success = true);
public sealed record EksUntagResourceResult(bool Success = true);

public sealed record EksListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

public sealed record EksDescribeAddonVersionsResult(
    List<AddonInfo>? Addons = null, string? NextToken = null);

public sealed record EksAssociateIdentityProviderConfigResult(
    Update? Update = null);

public sealed record EksDisassociateIdentityProviderConfigResult(
    Update? Update = null);

public sealed record EksListIdentityProviderConfigsResult(
    List<IdentityProviderConfig>? IdentityProviderConfigs = null,
    string? NextToken = null);

public sealed record EksRegisterClusterResult(
    string? Name = null, string? Arn = null, string? Status = null);

public sealed record EksDeregisterClusterResult(
    string? Name = null, string? Arn = null, string? Status = null);

/// <summary>
/// Utility helpers for Amazon Elastic Kubernetes Service (EKS).
/// </summary>
public static class EksService
{
    private static AmazonEKSClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEKSClient>(region);

    /// <summary>
    /// Create a new EKS cluster.
    /// </summary>
    public static async Task<EksCreateClusterResult> CreateClusterAsync(
        CreateClusterRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterAsync(request);
            return new EksCreateClusterResult(
                Name: resp.Cluster.Name,
                Arn: resp.Cluster.Arn,
                Status: resp.Cluster.Status?.Value,
                Version: resp.Cluster.Version,
                Endpoint: resp.Cluster.Endpoint);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create EKS cluster");
        }
    }

    /// <summary>
    /// Delete an EKS cluster.
    /// </summary>
    public static async Task<EksDeleteClusterResult> DeleteClusterAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteClusterAsync(
                new DeleteClusterRequest { Name = name });
            return new EksDeleteClusterResult(
                Name: resp.Cluster.Name,
                Arn: resp.Cluster.Arn,
                Status: resp.Cluster.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EKS cluster '{name}'");
        }
    }

    /// <summary>
    /// Describe an EKS cluster.
    /// </summary>
    public static async Task<EksDescribeClusterResult> DescribeClusterAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClusterAsync(
                new DescribeClusterRequest { Name = name });
            return new EksDescribeClusterResult(Cluster: resp.Cluster);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EKS cluster '{name}'");
        }
    }

    /// <summary>
    /// List EKS clusters.
    /// </summary>
    public static async Task<EksListClustersResult> ListClustersAsync(
        string? nextToken = null, int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListClustersRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListClustersAsync(request);
            return new EksListClustersResult(
                Clusters: resp.Clusters,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list EKS clusters");
        }
    }

    /// <summary>
    /// Update the Kubernetes version for an EKS cluster.
    /// </summary>
    public static async Task<EksUpdateClusterVersionResult>
        UpdateClusterVersionAsync(
            string name, string version,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateClusterVersionAsync(
                new UpdateClusterVersionRequest
                {
                    Name = name,
                    Version = version
                });
            return new EksUpdateClusterVersionResult(Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update EKS cluster version for '{name}'");
        }
    }

    /// <summary>
    /// Update the configuration for an EKS cluster.
    /// </summary>
    public static async Task<EksUpdateClusterConfigResult>
        UpdateClusterConfigAsync(
            UpdateClusterConfigRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateClusterConfigAsync(request);
            return new EksUpdateClusterConfigResult(Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update EKS cluster config");
        }
    }

    /// <summary>
    /// Create a managed node group.
    /// </summary>
    public static async Task<EksCreateNodegroupResult> CreateNodegroupAsync(
        CreateNodegroupRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateNodegroupAsync(request);
            return new EksCreateNodegroupResult(
                NodegroupName: resp.Nodegroup.NodegroupName,
                NodegroupArn: resp.Nodegroup.NodegroupArn,
                ClusterName: resp.Nodegroup.ClusterName,
                Status: resp.Nodegroup.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create EKS nodegroup");
        }
    }

    /// <summary>
    /// Delete a managed node group.
    /// </summary>
    public static async Task<EksDeleteNodegroupResult> DeleteNodegroupAsync(
        string clusterName, string nodegroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteNodegroupAsync(
                new DeleteNodegroupRequest
                {
                    ClusterName = clusterName,
                    NodegroupName = nodegroupName
                });
            return new EksDeleteNodegroupResult(
                NodegroupName: resp.Nodegroup.NodegroupName,
                NodegroupArn: resp.Nodegroup.NodegroupArn,
                Status: resp.Nodegroup.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EKS nodegroup '{nodegroupName}'");
        }
    }

    /// <summary>
    /// Describe a managed node group.
    /// </summary>
    public static async Task<EksDescribeNodegroupResult> DescribeNodegroupAsync(
        string clusterName, string nodegroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeNodegroupAsync(
                new DescribeNodegroupRequest
                {
                    ClusterName = clusterName,
                    NodegroupName = nodegroupName
                });
            return new EksDescribeNodegroupResult(Nodegroup: resp.Nodegroup);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EKS nodegroup '{nodegroupName}'");
        }
    }

    /// <summary>
    /// List managed node groups in a cluster.
    /// </summary>
    public static async Task<EksListNodegroupsResult> ListNodegroupsAsync(
        string clusterName, string? nextToken = null,
        int? maxResults = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListNodegroupsRequest
        {
            ClusterName = clusterName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListNodegroupsAsync(request);
            return new EksListNodegroupsResult(
                Nodegroups: resp.Nodegroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list EKS nodegroups for '{clusterName}'");
        }
    }

    /// <summary>
    /// Update the version of a managed node group.
    /// </summary>
    public static async Task<EksUpdateNodegroupVersionResult>
        UpdateNodegroupVersionAsync(
            UpdateNodegroupVersionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateNodegroupVersionAsync(request);
            return new EksUpdateNodegroupVersionResult(Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update EKS nodegroup version");
        }
    }

    /// <summary>
    /// Update the configuration of a managed node group.
    /// </summary>
    public static async Task<EksUpdateNodegroupConfigResult>
        UpdateNodegroupConfigAsync(
            UpdateNodegroupConfigRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateNodegroupConfigAsync(request);
            return new EksUpdateNodegroupConfigResult(Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update EKS nodegroup config");
        }
    }

    /// <summary>
    /// Create a Fargate profile.
    /// </summary>
    public static async Task<EksCreateFargateProfileResult>
        CreateFargateProfileAsync(
            CreateFargateProfileRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateFargateProfileAsync(request);
            return new EksCreateFargateProfileResult(
                FargateProfileName: resp.FargateProfile.FargateProfileName,
                FargateProfileArn: resp.FargateProfile.FargateProfileArn,
                ClusterName: resp.FargateProfile.ClusterName,
                Status: resp.FargateProfile.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create EKS Fargate profile");
        }
    }

    /// <summary>
    /// Delete a Fargate profile.
    /// </summary>
    public static async Task<EksDeleteFargateProfileResult>
        DeleteFargateProfileAsync(
            string clusterName, string fargateProfileName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteFargateProfileAsync(
                new DeleteFargateProfileRequest
                {
                    ClusterName = clusterName,
                    FargateProfileName = fargateProfileName
                });
            return new EksDeleteFargateProfileResult(
                FargateProfileName: resp.FargateProfile.FargateProfileName,
                FargateProfileArn: resp.FargateProfile.FargateProfileArn,
                Status: resp.FargateProfile.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EKS Fargate profile '{fargateProfileName}'");
        }
    }

    /// <summary>
    /// Describe a Fargate profile.
    /// </summary>
    public static async Task<EksDescribeFargateProfileResult>
        DescribeFargateProfileAsync(
            string clusterName, string fargateProfileName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeFargateProfileAsync(
                new DescribeFargateProfileRequest
                {
                    ClusterName = clusterName,
                    FargateProfileName = fargateProfileName
                });
            return new EksDescribeFargateProfileResult(
                FargateProfile: resp.FargateProfile);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EKS Fargate profile '{fargateProfileName}'");
        }
    }

    /// <summary>
    /// List Fargate profiles in a cluster.
    /// </summary>
    public static async Task<EksListFargateProfilesResult>
        ListFargateProfilesAsync(
            string clusterName, string? nextToken = null,
            int? maxResults = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFargateProfilesRequest
        {
            ClusterName = clusterName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListFargateProfilesAsync(request);
            return new EksListFargateProfilesResult(
                FargateProfileNames: resp.FargateProfileNames,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list EKS Fargate profiles for '{clusterName}'");
        }
    }

    /// <summary>
    /// Create an EKS add-on.
    /// </summary>
    public static async Task<EksCreateAddonResult> CreateAddonAsync(
        CreateAddonRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAddonAsync(request);
            return new EksCreateAddonResult(
                AddonName: resp.Addon.AddonName,
                AddonArn: resp.Addon.AddonArn,
                ClusterName: resp.Addon.ClusterName,
                Status: resp.Addon.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create EKS addon");
        }
    }

    /// <summary>
    /// Delete an EKS add-on.
    /// </summary>
    public static async Task<EksDeleteAddonResult> DeleteAddonAsync(
        string clusterName, string addonName, bool? preserve = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAddonRequest
        {
            ClusterName = clusterName,
            AddonName = addonName
        };
        if (preserve.HasValue) request.Preserve = preserve.Value;

        try
        {
            var resp = await client.DeleteAddonAsync(request);
            return new EksDeleteAddonResult(
                AddonName: resp.Addon.AddonName,
                AddonArn: resp.Addon.AddonArn,
                Status: resp.Addon.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EKS addon '{addonName}'");
        }
    }

    /// <summary>
    /// Describe an EKS add-on.
    /// </summary>
    public static async Task<EksDescribeAddonResult> DescribeAddonAsync(
        string clusterName, string addonName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAddonAsync(
                new DescribeAddonRequest
                {
                    ClusterName = clusterName,
                    AddonName = addonName
                });
            return new EksDescribeAddonResult(Addon: resp.Addon);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EKS addon '{addonName}'");
        }
    }

    /// <summary>
    /// List EKS add-ons for a cluster.
    /// </summary>
    public static async Task<EksListAddonsResult> ListAddonsAsync(
        string clusterName, string? nextToken = null,
        int? maxResults = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAddonsRequest
        {
            ClusterName = clusterName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListAddonsAsync(request);
            return new EksListAddonsResult(
                Addons: resp.Addons,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list EKS addons for '{clusterName}'");
        }
    }

    /// <summary>
    /// Update an EKS add-on.
    /// </summary>
    public static async Task<EksUpdateAddonResult> UpdateAddonAsync(
        UpdateAddonRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAddonAsync(request);
            return new EksUpdateAddonResult(Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update EKS addon");
        }
    }

    /// <summary>
    /// Tag an EKS resource.
    /// </summary>
    public static async Task<EksTagResourceResult> TagResourceAsync(
        string resourceArn, Dictionary<string, string> tags,
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
            return new EksTagResourceResult();
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag EKS resource");
        }
    }

    /// <summary>
    /// Remove tags from an EKS resource.
    /// </summary>
    public static async Task<EksUntagResourceResult> UntagResourceAsync(
        string resourceArn, List<string> tagKeys,
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
            return new EksUntagResourceResult();
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag EKS resource");
        }
    }

    /// <summary>
    /// List tags for an EKS resource.
    /// </summary>
    public static async Task<EksListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new EksListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for EKS resource");
        }
    }

    /// <summary>
    /// Describe available add-on versions.
    /// </summary>
    public static async Task<EksDescribeAddonVersionsResult>
        DescribeAddonVersionsAsync(
            DescribeAddonVersionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAddonVersionsAsync(request);
            return new EksDescribeAddonVersionsResult(
                Addons: resp.Addons,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe EKS addon versions");
        }
    }

    /// <summary>
    /// Associate an identity provider configuration with a cluster.
    /// </summary>
    public static async Task<EksAssociateIdentityProviderConfigResult>
        AssociateIdentityProviderConfigAsync(
            AssociateIdentityProviderConfigRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AssociateIdentityProviderConfigAsync(request);
            return new EksAssociateIdentityProviderConfigResult(
                Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to associate identity provider config");
        }
    }

    /// <summary>
    /// Disassociate an identity provider configuration from a cluster.
    /// </summary>
    public static async Task<EksDisassociateIdentityProviderConfigResult>
        DisassociateIdentityProviderConfigAsync(
            DisassociateIdentityProviderConfigRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisassociateIdentityProviderConfigAsync(
                request);
            return new EksDisassociateIdentityProviderConfigResult(
                Update: resp.Update);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disassociate identity provider config");
        }
    }

    /// <summary>
    /// List identity provider configurations for a cluster.
    /// </summary>
    public static async Task<EksListIdentityProviderConfigsResult>
        ListIdentityProviderConfigsAsync(
            string clusterName, string? nextToken = null,
            int? maxResults = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListIdentityProviderConfigsRequest
        {
            ClusterName = clusterName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListIdentityProviderConfigsAsync(request);
            return new EksListIdentityProviderConfigsResult(
                IdentityProviderConfigs: resp.IdentityProviderConfigs,
                NextToken: resp.NextToken);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list identity provider configs");
        }
    }

    /// <summary>
    /// Register an external Kubernetes cluster with EKS.
    /// </summary>
    public static async Task<EksRegisterClusterResult> RegisterClusterAsync(
        RegisterClusterRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RegisterClusterAsync(request);
            return new EksRegisterClusterResult(
                Name: resp.Cluster.Name,
                Arn: resp.Cluster.Arn,
                Status: resp.Cluster.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to register cluster");
        }
    }

    /// <summary>
    /// Deregister an external Kubernetes cluster from EKS.
    /// </summary>
    public static async Task<EksDeregisterClusterResult> DeregisterClusterAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeregisterClusterAsync(
                new DeregisterClusterRequest { Name = name });
            return new EksDeregisterClusterResult(
                Name: resp.Cluster.Name,
                Arn: resp.Cluster.Arn,
                Status: resp.Cluster.Status?.Value);
        }
        catch (AmazonEKSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister cluster '{name}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateClusterAsync"/>.</summary>
    public static EksCreateClusterResult CreateCluster(CreateClusterRequest request, RegionEndpoint? region = null)
        => CreateClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteClusterAsync"/>.</summary>
    public static EksDeleteClusterResult DeleteCluster(string name, RegionEndpoint? region = null)
        => DeleteClusterAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeClusterAsync"/>.</summary>
    public static EksDescribeClusterResult DescribeCluster(string name, RegionEndpoint? region = null)
        => DescribeClusterAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListClustersAsync"/>.</summary>
    public static EksListClustersResult ListClusters(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListClustersAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateClusterVersionAsync"/>.</summary>
    public static EksUpdateClusterVersionResult UpdateClusterVersion(string name, string version, RegionEndpoint? region = null)
        => UpdateClusterVersionAsync(name, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateClusterConfigAsync"/>.</summary>
    public static EksUpdateClusterConfigResult UpdateClusterConfig(UpdateClusterConfigRequest request, RegionEndpoint? region = null)
        => UpdateClusterConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateNodegroupAsync"/>.</summary>
    public static EksCreateNodegroupResult CreateNodegroup(CreateNodegroupRequest request, RegionEndpoint? region = null)
        => CreateNodegroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteNodegroupAsync"/>.</summary>
    public static EksDeleteNodegroupResult DeleteNodegroup(string clusterName, string nodegroupName, RegionEndpoint? region = null)
        => DeleteNodegroupAsync(clusterName, nodegroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeNodegroupAsync"/>.</summary>
    public static EksDescribeNodegroupResult DescribeNodegroup(string clusterName, string nodegroupName, RegionEndpoint? region = null)
        => DescribeNodegroupAsync(clusterName, nodegroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListNodegroupsAsync"/>.</summary>
    public static EksListNodegroupsResult ListNodegroups(string clusterName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListNodegroupsAsync(clusterName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateNodegroupVersionAsync"/>.</summary>
    public static EksUpdateNodegroupVersionResult UpdateNodegroupVersion(UpdateNodegroupVersionRequest request, RegionEndpoint? region = null)
        => UpdateNodegroupVersionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateNodegroupConfigAsync"/>.</summary>
    public static EksUpdateNodegroupConfigResult UpdateNodegroupConfig(UpdateNodegroupConfigRequest request, RegionEndpoint? region = null)
        => UpdateNodegroupConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateFargateProfileAsync"/>.</summary>
    public static EksCreateFargateProfileResult CreateFargateProfile(CreateFargateProfileRequest request, RegionEndpoint? region = null)
        => CreateFargateProfileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFargateProfileAsync"/>.</summary>
    public static EksDeleteFargateProfileResult DeleteFargateProfile(string clusterName, string fargateProfileName, RegionEndpoint? region = null)
        => DeleteFargateProfileAsync(clusterName, fargateProfileName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeFargateProfileAsync"/>.</summary>
    public static EksDescribeFargateProfileResult DescribeFargateProfile(string clusterName, string fargateProfileName, RegionEndpoint? region = null)
        => DescribeFargateProfileAsync(clusterName, fargateProfileName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFargateProfilesAsync"/>.</summary>
    public static EksListFargateProfilesResult ListFargateProfiles(string clusterName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListFargateProfilesAsync(clusterName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAddonAsync"/>.</summary>
    public static EksCreateAddonResult CreateAddon(CreateAddonRequest request, RegionEndpoint? region = null)
        => CreateAddonAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAddonAsync"/>.</summary>
    public static EksDeleteAddonResult DeleteAddon(string clusterName, string addonName, bool? preserve = null, RegionEndpoint? region = null)
        => DeleteAddonAsync(clusterName, addonName, preserve, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAddonAsync"/>.</summary>
    public static EksDescribeAddonResult DescribeAddon(string clusterName, string addonName, RegionEndpoint? region = null)
        => DescribeAddonAsync(clusterName, addonName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAddonsAsync"/>.</summary>
    public static EksListAddonsResult ListAddons(string clusterName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListAddonsAsync(clusterName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAddonAsync"/>.</summary>
    public static EksUpdateAddonResult UpdateAddon(UpdateAddonRequest request, RegionEndpoint? region = null)
        => UpdateAddonAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static EksTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static EksUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static EksListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAddonVersionsAsync"/>.</summary>
    public static EksDescribeAddonVersionsResult DescribeAddonVersions(DescribeAddonVersionsRequest request, RegionEndpoint? region = null)
        => DescribeAddonVersionsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateIdentityProviderConfigAsync"/>.</summary>
    public static EksAssociateIdentityProviderConfigResult AssociateIdentityProviderConfig(AssociateIdentityProviderConfigRequest request, RegionEndpoint? region = null)
        => AssociateIdentityProviderConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateIdentityProviderConfigAsync"/>.</summary>
    public static EksDisassociateIdentityProviderConfigResult DisassociateIdentityProviderConfig(DisassociateIdentityProviderConfigRequest request, RegionEndpoint? region = null)
        => DisassociateIdentityProviderConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListIdentityProviderConfigsAsync"/>.</summary>
    public static EksListIdentityProviderConfigsResult ListIdentityProviderConfigs(string clusterName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListIdentityProviderConfigsAsync(clusterName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterClusterAsync"/>.</summary>
    public static EksRegisterClusterResult RegisterCluster(RegisterClusterRequest request, RegionEndpoint? region = null)
        => RegisterClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterClusterAsync"/>.</summary>
    public static EksDeregisterClusterResult DeregisterCluster(string name, RegionEndpoint? region = null)
        => DeregisterClusterAsync(name, region).GetAwaiter().GetResult();

}
