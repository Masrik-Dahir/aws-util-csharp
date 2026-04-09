using Amazon;
using Amazon.IoTSiteWise;
using Amazon.IoTSiteWise.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateSiteWiseAssetResult(
    string? AssetId = null,
    string? AssetArn = null,
    AssetStatus? AssetStatus = null);

public sealed record DescribeSiteWiseAssetResult(
    string? AssetId = null,
    string? AssetArn = null,
    string? AssetName = null,
    string? AssetModelId = null,
    List<AssetProperty>? AssetProperties = null,
    List<AssetHierarchy>? AssetHierarchies = null,
    AssetStatus? AssetStatus = null);

public sealed record ListSiteWiseAssetsResult(
    List<AssetSummary>? AssetSummaries = null,
    string? NextToken = null);

public sealed record UpdateSiteWiseAssetResult(
    AssetStatus? AssetStatus = null);

public sealed record CreateAssetModelResult(
    string? AssetModelId = null,
    string? AssetModelArn = null,
    AssetModelStatus? AssetModelStatus = null);

public sealed record DescribeAssetModelResult(
    string? AssetModelId = null,
    string? AssetModelArn = null,
    string? AssetModelName = null,
    string? AssetModelDescription = null,
    List<AssetModelProperty>? AssetModelProperties = null,
    List<AssetModelHierarchy>? AssetModelHierarchies = null,
    AssetModelStatus? AssetModelStatus = null);

public sealed record ListAssetModelsResult(
    List<AssetModelSummary>? AssetModelSummaries = null,
    string? NextToken = null);

public sealed record UpdateAssetModelResult(
    AssetModelStatus? AssetModelStatus = null);

public sealed record ListAssociatedAssetsResult(
    List<AssociatedAssetsSummary>? AssetSummaries = null,
    string? NextToken = null);

public sealed record CreateSiteWisePortalResult(
    string? PortalId = null,
    string? PortalArn = null,
    string? PortalStartUrl = null,
    PortalStatus? PortalStatus = null);

public sealed record DescribeSiteWisePortalResult(
    string? PortalId = null,
    string? PortalArn = null,
    string? PortalName = null,
    string? PortalDescription = null,
    string? PortalStartUrl = null,
    string? PortalContactEmail = null,
    PortalStatus? PortalStatus = null);

public sealed record ListSiteWisePortalsResult(
    List<PortalSummary>? PortalSummaries = null,
    string? NextToken = null);

public sealed record UpdateSiteWisePortalResult(
    PortalStatus? PortalStatus = null);

public sealed record CreateSiteWiseProjectResult(
    string? ProjectId = null,
    string? ProjectArn = null);

public sealed record DescribeSiteWiseProjectResult(
    string? ProjectId = null,
    string? ProjectArn = null,
    string? ProjectName = null,
    string? ProjectDescription = null,
    string? PortalId = null);

public sealed record ListSiteWiseProjectsResult(
    List<ProjectSummary>? ProjectSummaries = null,
    string? NextToken = null);

public sealed record CreateSiteWiseDashboardResult(
    string? DashboardId = null,
    string? DashboardArn = null);

public sealed record DescribeSiteWiseDashboardResult(
    string? DashboardId = null,
    string? DashboardArn = null,
    string? DashboardName = null,
    string? DashboardDescription = null,
    string? DashboardDefinition = null,
    string? ProjectId = null);

public sealed record ListSiteWiseDashboardsResult(
    List<DashboardSummary>? DashboardSummaries = null,
    string? NextToken = null);

public sealed record CreateSiteWiseAccessPolicyResult(
    string? AccessPolicyId = null,
    string? AccessPolicyArn = null);

public sealed record DescribeSiteWiseAccessPolicyResult(
    string? AccessPolicyId = null,
    string? AccessPolicyArn = null,
    Identity? AccessPolicyIdentity = null,
    Resource? AccessPolicyResource = null,
    Permission? AccessPolicyPermission = null);

public sealed record ListSiteWiseAccessPoliciesResult(
    List<AccessPolicySummary>? AccessPolicySummaries = null,
    string? NextToken = null);

public sealed record GetAssetPropertyValueResult(
    AssetPropertyValue? PropertyValue = null);

public sealed record GetAssetPropertyValueHistoryResult(
    List<AssetPropertyValue>? AssetPropertyValueHistory = null,
    string? NextToken = null);

public sealed record GetAssetPropertyAggregatesResult(
    List<AggregatedValue>? AggregatedValues = null,
    string? NextToken = null);

public sealed record BatchPutAssetPropertyValueResult(
    List<BatchPutAssetPropertyErrorEntry>? ErrorEntries = null);

public sealed record CreateSiteWiseGatewayResult(
    string? GatewayId = null,
    string? GatewayArn = null);

public sealed record DescribeSiteWiseGatewayResult(
    string? GatewayId = null,
    string? GatewayArn = null,
    string? GatewayName = null,
    GatewayPlatform? GatewayPlatform = null,
    List<GatewayCapabilitySummary>? GatewayCapabilitySummaries = null);

public sealed record ListSiteWiseGatewaysResult(
    List<GatewaySummary>? GatewaySummaries = null,
    string? NextToken = null);

public sealed record SiteWiseListTagsResult(
    Dictionary<string, string>? Tags = null);

/// <summary>
/// Utility helpers for AWS IoT SiteWise.
/// </summary>
public static class IoTSiteWiseService
{
    private static AmazonIoTSiteWiseClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIoTSiteWiseClient>(region);

    // ── Asset operations ────────────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise asset.
    /// </summary>
    public static async Task<CreateSiteWiseAssetResult> CreateAssetAsync(
        string assetName,
        string assetModelId,
        string? assetDescription = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAssetRequest
        {
            AssetName = assetName,
            AssetModelId = assetModelId
        };
        if (assetDescription != null) request.AssetDescription = assetDescription;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAssetAsync(request);
            return new CreateSiteWiseAssetResult(
                AssetId: resp.AssetId,
                AssetArn: resp.AssetArn,
                AssetStatus: resp.AssetStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create asset '{assetName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise asset.
    /// </summary>
    public static async Task DeleteAssetAsync(
        string assetId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAssetAsync(
                new DeleteAssetRequest { AssetId = assetId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete asset '{assetId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise asset.
    /// </summary>
    public static async Task<DescribeSiteWiseAssetResult> DescribeAssetAsync(
        string assetId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAssetAsync(
                new DescribeAssetRequest { AssetId = assetId });
            return new DescribeSiteWiseAssetResult(
                AssetId: resp.AssetId,
                AssetArn: resp.AssetArn,
                AssetName: resp.AssetName,
                AssetModelId: resp.AssetModelId,
                AssetProperties: resp.AssetProperties,
                AssetHierarchies: resp.AssetHierarchies,
                AssetStatus: resp.AssetStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe asset '{assetId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise assets with optional filtering.
    /// </summary>
    public static async Task<ListSiteWiseAssetsResult> ListAssetsAsync(
        string? assetModelId = null,
        ListAssetsFilter? filter = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAssetsRequest();
        if (assetModelId != null) request.AssetModelId = assetModelId;
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAssetsAsync(request);
            return new ListSiteWiseAssetsResult(
                AssetSummaries: resp.AssetSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list assets");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise asset.
    /// </summary>
    public static async Task<UpdateSiteWiseAssetResult> UpdateAssetAsync(
        string assetId,
        string assetName,
        string? assetDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAssetRequest
        {
            AssetId = assetId,
            AssetName = assetName
        };
        if (assetDescription != null) request.AssetDescription = assetDescription;

        try
        {
            var resp = await client.UpdateAssetAsync(request);
            return new UpdateSiteWiseAssetResult(AssetStatus: resp.AssetStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update asset '{assetId}'");
        }
    }

    // ── Asset Model operations ──────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise asset model.
    /// </summary>
    public static async Task<CreateAssetModelResult> CreateAssetModelAsync(
        string assetModelName,
        string? assetModelDescription = null,
        List<AssetModelPropertyDefinition>? assetModelProperties = null,
        List<AssetModelHierarchyDefinition>? assetModelHierarchies = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAssetModelRequest { AssetModelName = assetModelName };
        if (assetModelDescription != null)
            request.AssetModelDescription = assetModelDescription;
        if (assetModelProperties != null)
            request.AssetModelProperties = assetModelProperties;
        if (assetModelHierarchies != null)
            request.AssetModelHierarchies = assetModelHierarchies;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAssetModelAsync(request);
            return new CreateAssetModelResult(
                AssetModelId: resp.AssetModelId,
                AssetModelArn: resp.AssetModelArn,
                AssetModelStatus: resp.AssetModelStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create asset model '{assetModelName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise asset model.
    /// </summary>
    public static async Task DeleteAssetModelAsync(
        string assetModelId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAssetModelAsync(
                new DeleteAssetModelRequest { AssetModelId = assetModelId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete asset model '{assetModelId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise asset model.
    /// </summary>
    public static async Task<DescribeAssetModelResult> DescribeAssetModelAsync(
        string assetModelId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAssetModelAsync(
                new DescribeAssetModelRequest { AssetModelId = assetModelId });
            return new DescribeAssetModelResult(
                AssetModelId: resp.AssetModelId,
                AssetModelArn: resp.AssetModelArn,
                AssetModelName: resp.AssetModelName,
                AssetModelDescription: resp.AssetModelDescription,
                AssetModelProperties: resp.AssetModelProperties,
                AssetModelHierarchies: resp.AssetModelHierarchies,
                AssetModelStatus: resp.AssetModelStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe asset model '{assetModelId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise asset models with optional pagination.
    /// </summary>
    public static async Task<ListAssetModelsResult> ListAssetModelsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAssetModelsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAssetModelsAsync(request);
            return new ListAssetModelsResult(
                AssetModelSummaries: resp.AssetModelSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list asset models");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise asset model.
    /// </summary>
    public static async Task<UpdateAssetModelResult> UpdateAssetModelAsync(
        string assetModelId,
        string assetModelName,
        string? assetModelDescription = null,
        List<AssetModelProperty>? assetModelProperties = null,
        List<AssetModelHierarchy>? assetModelHierarchies = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAssetModelRequest
        {
            AssetModelId = assetModelId,
            AssetModelName = assetModelName
        };
        if (assetModelDescription != null)
            request.AssetModelDescription = assetModelDescription;
        if (assetModelProperties != null)
            request.AssetModelProperties = assetModelProperties;
        if (assetModelHierarchies != null)
            request.AssetModelHierarchies = assetModelHierarchies;

        try
        {
            var resp = await client.UpdateAssetModelAsync(request);
            return new UpdateAssetModelResult(AssetModelStatus: resp.AssetModelStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update asset model '{assetModelId}'");
        }
    }

    // ── Asset Association operations ────────────────────────────────

    /// <summary>
    /// Associate a child asset with a parent asset.
    /// </summary>
    public static async Task AssociateAssetsAsync(
        string assetId,
        string hierarchyId,
        string childAssetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateAssetsAsync(new AssociateAssetsRequest
            {
                AssetId = assetId,
                HierarchyId = hierarchyId,
                ChildAssetId = childAssetId
            });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate asset '{childAssetId}' with '{assetId}'");
        }
    }

    /// <summary>
    /// Disassociate a child asset from a parent asset.
    /// </summary>
    public static async Task DisassociateAssetsAsync(
        string assetId,
        string hierarchyId,
        string childAssetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateAssetsAsync(new DisassociateAssetsRequest
            {
                AssetId = assetId,
                HierarchyId = hierarchyId,
                ChildAssetId = childAssetId
            });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate asset '{childAssetId}' from '{assetId}'");
        }
    }

    /// <summary>
    /// List assets associated with a parent asset.
    /// </summary>
    public static async Task<ListAssociatedAssetsResult> ListAssociatedAssetsAsync(
        string assetId,
        string? hierarchyId = null,
        TraversalDirection? traversalDirection = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAssociatedAssetsRequest { AssetId = assetId };
        if (hierarchyId != null) request.HierarchyId = hierarchyId;
        if (traversalDirection != null) request.TraversalDirection = traversalDirection;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAssociatedAssetsAsync(request);
            return new ListAssociatedAssetsResult(
                AssetSummaries: resp.AssetSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list associated assets for '{assetId}'");
        }
    }

    // ── Portal operations ───────────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise portal.
    /// </summary>
    public static async Task<CreateSiteWisePortalResult> CreatePortalAsync(
        string portalName,
        string portalContactEmail,
        string roleArn,
        string? portalDescription = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePortalRequest
        {
            PortalName = portalName,
            PortalContactEmail = portalContactEmail,
            RoleArn = roleArn
        };
        if (portalDescription != null) request.PortalDescription = portalDescription;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePortalAsync(request);
            return new CreateSiteWisePortalResult(
                PortalId: resp.PortalId,
                PortalArn: resp.PortalArn,
                PortalStartUrl: resp.PortalStartUrl,
                PortalStatus: resp.PortalStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create portal '{portalName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise portal.
    /// </summary>
    public static async Task DeletePortalAsync(
        string portalId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePortalAsync(
                new DeletePortalRequest { PortalId = portalId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete portal '{portalId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise portal.
    /// </summary>
    public static async Task<DescribeSiteWisePortalResult> DescribePortalAsync(
        string portalId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePortalAsync(
                new DescribePortalRequest { PortalId = portalId });
            return new DescribeSiteWisePortalResult(
                PortalId: resp.PortalId,
                PortalArn: resp.PortalArn,
                PortalName: resp.PortalName,
                PortalDescription: resp.PortalDescription,
                PortalStartUrl: resp.PortalStartUrl,
                PortalContactEmail: resp.PortalContactEmail,
                PortalStatus: resp.PortalStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe portal '{portalId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise portals with optional pagination.
    /// </summary>
    public static async Task<ListSiteWisePortalsResult> ListPortalsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPortalsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListPortalsAsync(request);
            return new ListSiteWisePortalsResult(
                PortalSummaries: resp.PortalSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list portals");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise portal.
    /// </summary>
    public static async Task<UpdateSiteWisePortalResult> UpdatePortalAsync(
        string portalId,
        string portalName,
        string portalContactEmail,
        string roleArn,
        string? portalDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdatePortalRequest
        {
            PortalId = portalId,
            PortalName = portalName,
            PortalContactEmail = portalContactEmail,
            RoleArn = roleArn
        };
        if (portalDescription != null) request.PortalDescription = portalDescription;

        try
        {
            var resp = await client.UpdatePortalAsync(request);
            return new UpdateSiteWisePortalResult(PortalStatus: resp.PortalStatus);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update portal '{portalId}'");
        }
    }

    // ── Project operations ──────────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise project.
    /// </summary>
    public static async Task<CreateSiteWiseProjectResult> CreateProjectAsync(
        string projectName,
        string portalId,
        string? projectDescription = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateProjectRequest
        {
            ProjectName = projectName,
            PortalId = portalId
        };
        if (projectDescription != null) request.ProjectDescription = projectDescription;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateProjectAsync(request);
            return new CreateSiteWiseProjectResult(
                ProjectId: resp.ProjectId,
                ProjectArn: resp.ProjectArn);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create project '{projectName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise project.
    /// </summary>
    public static async Task DeleteProjectAsync(
        string projectId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProjectAsync(
                new DeleteProjectRequest { ProjectId = projectId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete project '{projectId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise project.
    /// </summary>
    public static async Task<DescribeSiteWiseProjectResult> DescribeProjectAsync(
        string projectId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeProjectAsync(
                new DescribeProjectRequest { ProjectId = projectId });
            return new DescribeSiteWiseProjectResult(
                ProjectId: resp.ProjectId,
                ProjectArn: resp.ProjectArn,
                ProjectName: resp.ProjectName,
                ProjectDescription: resp.ProjectDescription,
                PortalId: resp.PortalId);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe project '{projectId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise projects with optional pagination.
    /// </summary>
    public static async Task<ListSiteWiseProjectsResult> ListProjectsAsync(
        string portalId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListProjectsRequest { PortalId = portalId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListProjectsAsync(request);
            return new ListSiteWiseProjectsResult(
                ProjectSummaries: resp.ProjectSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list projects");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise project.
    /// </summary>
    public static async Task UpdateProjectAsync(
        string projectId,
        string projectName,
        string? projectDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateProjectRequest
        {
            ProjectId = projectId,
            ProjectName = projectName
        };
        if (projectDescription != null) request.ProjectDescription = projectDescription;

        try
        {
            await client.UpdateProjectAsync(request);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update project '{projectId}'");
        }
    }

    // ── Dashboard operations ────────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise dashboard.
    /// </summary>
    public static async Task<CreateSiteWiseDashboardResult> CreateDashboardAsync(
        string dashboardName,
        string projectId,
        string dashboardDefinition,
        string? dashboardDescription = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDashboardRequest
        {
            DashboardName = dashboardName,
            ProjectId = projectId,
            DashboardDefinition = dashboardDefinition
        };
        if (dashboardDescription != null)
            request.DashboardDescription = dashboardDescription;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDashboardAsync(request);
            return new CreateSiteWiseDashboardResult(
                DashboardId: resp.DashboardId,
                DashboardArn: resp.DashboardArn);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dashboard '{dashboardName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise dashboard.
    /// </summary>
    public static async Task DeleteDashboardAsync(
        string dashboardId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDashboardAsync(
                new DeleteDashboardRequest { DashboardId = dashboardId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dashboard '{dashboardId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise dashboard.
    /// </summary>
    public static async Task<DescribeSiteWiseDashboardResult> DescribeDashboardAsync(
        string dashboardId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDashboardAsync(
                new DescribeDashboardRequest { DashboardId = dashboardId });
            return new DescribeSiteWiseDashboardResult(
                DashboardId: resp.DashboardId,
                DashboardArn: resp.DashboardArn,
                DashboardName: resp.DashboardName,
                DashboardDescription: resp.DashboardDescription,
                DashboardDefinition: resp.DashboardDefinition,
                ProjectId: resp.ProjectId);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dashboard '{dashboardId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise dashboards with optional pagination.
    /// </summary>
    public static async Task<ListSiteWiseDashboardsResult> ListDashboardsAsync(
        string projectId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDashboardsRequest { ProjectId = projectId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDashboardsAsync(request);
            return new ListSiteWiseDashboardsResult(
                DashboardSummaries: resp.DashboardSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list dashboards");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise dashboard.
    /// </summary>
    public static async Task UpdateDashboardAsync(
        string dashboardId,
        string dashboardName,
        string dashboardDefinition,
        string? dashboardDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDashboardRequest
        {
            DashboardId = dashboardId,
            DashboardName = dashboardName,
            DashboardDefinition = dashboardDefinition
        };
        if (dashboardDescription != null)
            request.DashboardDescription = dashboardDescription;

        try
        {
            await client.UpdateDashboardAsync(request);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update dashboard '{dashboardId}'");
        }
    }

    // ── Access Policy operations ────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise access policy.
    /// </summary>
    public static async Task<CreateSiteWiseAccessPolicyResult> CreateAccessPolicyAsync(
        Identity accessPolicyIdentity,
        Resource accessPolicyResource,
        Permission accessPolicyPermission,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAccessPolicyRequest
        {
            AccessPolicyIdentity = accessPolicyIdentity,
            AccessPolicyResource = accessPolicyResource,
            AccessPolicyPermission = accessPolicyPermission
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAccessPolicyAsync(request);
            return new CreateSiteWiseAccessPolicyResult(
                AccessPolicyId: resp.AccessPolicyId,
                AccessPolicyArn: resp.AccessPolicyArn);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create access policy");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise access policy.
    /// </summary>
    public static async Task DeleteAccessPolicyAsync(
        string accessPolicyId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAccessPolicyAsync(
                new DeleteAccessPolicyRequest { AccessPolicyId = accessPolicyId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete access policy '{accessPolicyId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise access policy.
    /// </summary>
    public static async Task<DescribeSiteWiseAccessPolicyResult> DescribeAccessPolicyAsync(
        string accessPolicyId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccessPolicyAsync(
                new DescribeAccessPolicyRequest { AccessPolicyId = accessPolicyId });
            return new DescribeSiteWiseAccessPolicyResult(
                AccessPolicyId: resp.AccessPolicyId,
                AccessPolicyArn: resp.AccessPolicyArn,
                AccessPolicyIdentity: resp.AccessPolicyIdentity,
                AccessPolicyResource: resp.AccessPolicyResource,
                AccessPolicyPermission: resp.AccessPolicyPermission);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe access policy '{accessPolicyId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise access policies with optional filtering.
    /// </summary>
    public static async Task<ListSiteWiseAccessPoliciesResult> ListAccessPoliciesAsync(
        IdentityType? identityType = null,
        string? identityId = null,
        ResourceType? resourceType = null,
        string? resourceId = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAccessPoliciesRequest();
        if (identityType != null) request.IdentityType = identityType;
        if (identityId != null) request.IdentityId = identityId;
        if (resourceType != null) request.ResourceType = resourceType;
        if (resourceId != null) request.ResourceId = resourceId;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAccessPoliciesAsync(request);
            return new ListSiteWiseAccessPoliciesResult(
                AccessPolicySummaries: resp.AccessPolicySummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list access policies");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise access policy.
    /// </summary>
    public static async Task UpdateAccessPolicyAsync(
        string accessPolicyId,
        Identity accessPolicyIdentity,
        Resource accessPolicyResource,
        Permission accessPolicyPermission,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateAccessPolicyAsync(new UpdateAccessPolicyRequest
            {
                AccessPolicyId = accessPolicyId,
                AccessPolicyIdentity = accessPolicyIdentity,
                AccessPolicyResource = accessPolicyResource,
                AccessPolicyPermission = accessPolicyPermission
            });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update access policy '{accessPolicyId}'");
        }
    }

    // ── Property Value operations ───────────────────────────────────

    /// <summary>
    /// Get the current value of an asset property.
    /// </summary>
    public static async Task<GetAssetPropertyValueResult> GetAssetPropertyValueAsync(
        string? assetId = null,
        string? propertyId = null,
        string? propertyAlias = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAssetPropertyValueRequest();
        if (assetId != null) request.AssetId = assetId;
        if (propertyId != null) request.PropertyId = propertyId;
        if (propertyAlias != null) request.PropertyAlias = propertyAlias;

        try
        {
            var resp = await client.GetAssetPropertyValueAsync(request);
            return new GetAssetPropertyValueResult(PropertyValue: resp.PropertyValue);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get asset property value");
        }
    }

    /// <summary>
    /// Get the value history of an asset property.
    /// </summary>
    public static async Task<GetAssetPropertyValueHistoryResult>
        GetAssetPropertyValueHistoryAsync(
        string? assetId = null,
        string? propertyId = null,
        string? propertyAlias = null,
        DateTime? startDate = null,
        DateTime? endDate = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAssetPropertyValueHistoryRequest();
        if (assetId != null) request.AssetId = assetId;
        if (propertyId != null) request.PropertyId = propertyId;
        if (propertyAlias != null) request.PropertyAlias = propertyAlias;
        if (startDate.HasValue) request.StartDate = startDate.Value;
        if (endDate.HasValue) request.EndDate = endDate.Value;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetAssetPropertyValueHistoryAsync(request);
            return new GetAssetPropertyValueHistoryResult(
                AssetPropertyValueHistory: resp.AssetPropertyValueHistory,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get asset property value history");
        }
    }

    /// <summary>
    /// Get aggregated values for an asset property.
    /// </summary>
    public static async Task<GetAssetPropertyAggregatesResult>
        GetAssetPropertyAggregatesAsync(
        List<string> aggregateTypes,
        string resolution,
        DateTime startDate,
        DateTime endDate,
        string? assetId = null,
        string? propertyId = null,
        string? propertyAlias = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAssetPropertyAggregatesRequest
        {
            AggregateTypes = aggregateTypes,
            Resolution = resolution,
            StartDate = startDate,
            EndDate = endDate
        };
        if (assetId != null) request.AssetId = assetId;
        if (propertyId != null) request.PropertyId = propertyId;
        if (propertyAlias != null) request.PropertyAlias = propertyAlias;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetAssetPropertyAggregatesAsync(request);
            return new GetAssetPropertyAggregatesResult(
                AggregatedValues: resp.AggregatedValues,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get asset property aggregates");
        }
    }

    /// <summary>
    /// Batch put asset property values.
    /// </summary>
    public static async Task<BatchPutAssetPropertyValueResult>
        BatchPutAssetPropertyValueAsync(
        List<PutAssetPropertyValueEntry> entries,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchPutAssetPropertyValueAsync(
                new BatchPutAssetPropertyValueRequest { Entries = entries });
            return new BatchPutAssetPropertyValueResult(
                ErrorEntries: resp.ErrorEntries);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch put asset property values");
        }
    }

    // ── Gateway operations ──────────────────────────────────────────

    /// <summary>
    /// Create an IoT SiteWise gateway.
    /// </summary>
    public static async Task<CreateSiteWiseGatewayResult> CreateGatewayAsync(
        string gatewayName,
        GatewayPlatform gatewayPlatform,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGatewayRequest
        {
            GatewayName = gatewayName,
            GatewayPlatform = gatewayPlatform
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateGatewayAsync(request);
            return new CreateSiteWiseGatewayResult(
                GatewayId: resp.GatewayId,
                GatewayArn: resp.GatewayArn);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create gateway '{gatewayName}'");
        }
    }

    /// <summary>
    /// Delete an IoT SiteWise gateway.
    /// </summary>
    public static async Task DeleteGatewayAsync(
        string gatewayId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteGatewayAsync(
                new DeleteGatewayRequest { GatewayId = gatewayId });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete gateway '{gatewayId}'");
        }
    }

    /// <summary>
    /// Describe an IoT SiteWise gateway.
    /// </summary>
    public static async Task<DescribeSiteWiseGatewayResult> DescribeGatewayAsync(
        string gatewayId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeGatewayAsync(
                new DescribeGatewayRequest { GatewayId = gatewayId });
            return new DescribeSiteWiseGatewayResult(
                GatewayId: resp.GatewayId,
                GatewayArn: resp.GatewayArn,
                GatewayName: resp.GatewayName,
                GatewayPlatform: resp.GatewayPlatform,
                GatewayCapabilitySummaries: resp.GatewayCapabilitySummaries);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe gateway '{gatewayId}'");
        }
    }

    /// <summary>
    /// List IoT SiteWise gateways with optional pagination.
    /// </summary>
    public static async Task<ListSiteWiseGatewaysResult> ListGatewaysAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGatewaysRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListGatewaysAsync(request);
            return new ListSiteWiseGatewaysResult(
                GatewaySummaries: resp.GatewaySummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list gateways");
        }
    }

    /// <summary>
    /// Update an IoT SiteWise gateway.
    /// </summary>
    public static async Task UpdateGatewayAsync(
        string gatewayId,
        string gatewayName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateGatewayAsync(new UpdateGatewayRequest
            {
                GatewayId = gatewayId,
                GatewayName = gatewayName
            });
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update gateway '{gatewayId}'");
        }
    }

    // ── Tagging operations ──────────────────────────────────────────

    /// <summary>
    /// Add tags to an IoT SiteWise resource.
    /// </summary>
    public static async Task TagResourceAsync(
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
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an IoT SiteWise resource.
    /// </summary>
    public static async Task UntagResourceAsync(
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
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an IoT SiteWise resource.
    /// </summary>
    public static async Task<SiteWiseListTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new SiteWiseListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonIoTSiteWiseException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateAssetAsync"/>.</summary>
    public static CreateSiteWiseAssetResult CreateAsset(string assetName, string assetModelId, string? assetDescription = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAssetAsync(assetName, assetModelId, assetDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAssetAsync"/>.</summary>
    public static void DeleteAsset(string assetId, RegionEndpoint? region = null)
        => DeleteAssetAsync(assetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAssetAsync"/>.</summary>
    public static DescribeSiteWiseAssetResult DescribeAsset(string assetId, RegionEndpoint? region = null)
        => DescribeAssetAsync(assetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAssetsAsync"/>.</summary>
    public static ListSiteWiseAssetsResult ListAssets(string? assetModelId = null, ListAssetsFilter? filter = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAssetsAsync(assetModelId, filter, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAssetAsync"/>.</summary>
    public static UpdateSiteWiseAssetResult UpdateAsset(string assetId, string assetName, string? assetDescription = null, RegionEndpoint? region = null)
        => UpdateAssetAsync(assetId, assetName, assetDescription, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAssetModelAsync"/>.</summary>
    public static CreateAssetModelResult CreateAssetModel(string assetModelName, string? assetModelDescription = null, List<AssetModelPropertyDefinition>? assetModelProperties = null, List<AssetModelHierarchyDefinition>? assetModelHierarchies = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAssetModelAsync(assetModelName, assetModelDescription, assetModelProperties, assetModelHierarchies, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAssetModelAsync"/>.</summary>
    public static void DeleteAssetModel(string assetModelId, RegionEndpoint? region = null)
        => DeleteAssetModelAsync(assetModelId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAssetModelAsync"/>.</summary>
    public static DescribeAssetModelResult DescribeAssetModel(string assetModelId, RegionEndpoint? region = null)
        => DescribeAssetModelAsync(assetModelId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAssetModelsAsync"/>.</summary>
    public static ListAssetModelsResult ListAssetModels(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAssetModelsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAssetModelAsync"/>.</summary>
    public static UpdateAssetModelResult UpdateAssetModel(string assetModelId, string assetModelName, string? assetModelDescription = null, List<AssetModelProperty>? assetModelProperties = null, List<AssetModelHierarchy>? assetModelHierarchies = null, RegionEndpoint? region = null)
        => UpdateAssetModelAsync(assetModelId, assetModelName, assetModelDescription, assetModelProperties, assetModelHierarchies, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateAssetsAsync"/>.</summary>
    public static void AssociateAssets(string assetId, string hierarchyId, string childAssetId, RegionEndpoint? region = null)
        => AssociateAssetsAsync(assetId, hierarchyId, childAssetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateAssetsAsync"/>.</summary>
    public static void DisassociateAssets(string assetId, string hierarchyId, string childAssetId, RegionEndpoint? region = null)
        => DisassociateAssetsAsync(assetId, hierarchyId, childAssetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAssociatedAssetsAsync"/>.</summary>
    public static ListAssociatedAssetsResult ListAssociatedAssets(string assetId, string? hierarchyId = null, TraversalDirection? traversalDirection = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAssociatedAssetsAsync(assetId, hierarchyId, traversalDirection, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePortalAsync"/>.</summary>
    public static CreateSiteWisePortalResult CreatePortal(string portalName, string portalContactEmail, string roleArn, string? portalDescription = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreatePortalAsync(portalName, portalContactEmail, roleArn, portalDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePortalAsync"/>.</summary>
    public static void DeletePortal(string portalId, RegionEndpoint? region = null)
        => DeletePortalAsync(portalId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribePortalAsync"/>.</summary>
    public static DescribeSiteWisePortalResult DescribePortal(string portalId, RegionEndpoint? region = null)
        => DescribePortalAsync(portalId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPortalsAsync"/>.</summary>
    public static ListSiteWisePortalsResult ListPortals(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListPortalsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdatePortalAsync"/>.</summary>
    public static UpdateSiteWisePortalResult UpdatePortal(string portalId, string portalName, string portalContactEmail, string roleArn, string? portalDescription = null, RegionEndpoint? region = null)
        => UpdatePortalAsync(portalId, portalName, portalContactEmail, roleArn, portalDescription, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProjectAsync"/>.</summary>
    public static CreateSiteWiseProjectResult CreateProject(string projectName, string portalId, string? projectDescription = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateProjectAsync(projectName, portalId, projectDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteProjectAsync"/>.</summary>
    public static void DeleteProject(string projectId, RegionEndpoint? region = null)
        => DeleteProjectAsync(projectId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeProjectAsync"/>.</summary>
    public static DescribeSiteWiseProjectResult DescribeProject(string projectId, RegionEndpoint? region = null)
        => DescribeProjectAsync(projectId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListProjectsAsync"/>.</summary>
    public static ListSiteWiseProjectsResult ListProjects(string portalId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListProjectsAsync(portalId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateProjectAsync"/>.</summary>
    public static void UpdateProject(string projectId, string projectName, string? projectDescription = null, RegionEndpoint? region = null)
        => UpdateProjectAsync(projectId, projectName, projectDescription, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDashboardAsync"/>.</summary>
    public static CreateSiteWiseDashboardResult CreateDashboard(string dashboardName, string projectId, string dashboardDefinition, string? dashboardDescription = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateDashboardAsync(dashboardName, projectId, dashboardDefinition, dashboardDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDashboardAsync"/>.</summary>
    public static void DeleteDashboard(string dashboardId, RegionEndpoint? region = null)
        => DeleteDashboardAsync(dashboardId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDashboardAsync"/>.</summary>
    public static DescribeSiteWiseDashboardResult DescribeDashboard(string dashboardId, RegionEndpoint? region = null)
        => DescribeDashboardAsync(dashboardId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDashboardsAsync"/>.</summary>
    public static ListSiteWiseDashboardsResult ListDashboards(string projectId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListDashboardsAsync(projectId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDashboardAsync"/>.</summary>
    public static void UpdateDashboard(string dashboardId, string dashboardName, string dashboardDefinition, string? dashboardDescription = null, RegionEndpoint? region = null)
        => UpdateDashboardAsync(dashboardId, dashboardName, dashboardDefinition, dashboardDescription, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAccessPolicyAsync"/>.</summary>
    public static CreateSiteWiseAccessPolicyResult CreateAccessPolicy(Identity accessPolicyIdentity, Resource accessPolicyResource, Permission accessPolicyPermission, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAccessPolicyAsync(accessPolicyIdentity, accessPolicyResource, accessPolicyPermission, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAccessPolicyAsync"/>.</summary>
    public static void DeleteAccessPolicy(string accessPolicyId, RegionEndpoint? region = null)
        => DeleteAccessPolicyAsync(accessPolicyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAccessPolicyAsync"/>.</summary>
    public static DescribeSiteWiseAccessPolicyResult DescribeAccessPolicy(string accessPolicyId, RegionEndpoint? region = null)
        => DescribeAccessPolicyAsync(accessPolicyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccessPoliciesAsync"/>.</summary>
    public static ListSiteWiseAccessPoliciesResult ListAccessPolicies(IdentityType? identityType = null, string? identityId = null, ResourceType? resourceType = null, string? resourceId = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAccessPoliciesAsync(identityType, identityId, resourceType, resourceId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAccessPolicyAsync"/>.</summary>
    public static void UpdateAccessPolicy(string accessPolicyId, Identity accessPolicyIdentity, Resource accessPolicyResource, Permission accessPolicyPermission, RegionEndpoint? region = null)
        => UpdateAccessPolicyAsync(accessPolicyId, accessPolicyIdentity, accessPolicyResource, accessPolicyPermission, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAssetPropertyValueAsync"/>.</summary>
    public static GetAssetPropertyValueResult GetAssetPropertyValue(string? assetId = null, string? propertyId = null, string? propertyAlias = null, RegionEndpoint? region = null)
        => GetAssetPropertyValueAsync(assetId, propertyId, propertyAlias, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAssetPropertyValueHistoryAsync"/>.</summary>
    public static GetAssetPropertyValueHistoryResult GetAssetPropertyValueHistory(string? assetId = null, string? propertyId = null, string? propertyAlias = null, DateTime? startDate = null, DateTime? endDate = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetAssetPropertyValueHistoryAsync(assetId, propertyId, propertyAlias, startDate, endDate, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAssetPropertyAggregatesAsync"/>.</summary>
    public static GetAssetPropertyAggregatesResult GetAssetPropertyAggregates(List<string> aggregateTypes, string resolution, DateTime startDate, DateTime endDate, string? assetId = null, string? propertyId = null, string? propertyAlias = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetAssetPropertyAggregatesAsync(aggregateTypes, resolution, startDate, endDate, assetId, propertyId, propertyAlias, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchPutAssetPropertyValueAsync"/>.</summary>
    public static BatchPutAssetPropertyValueResult BatchPutAssetPropertyValue(List<PutAssetPropertyValueEntry> entries, RegionEndpoint? region = null)
        => BatchPutAssetPropertyValueAsync(entries, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateGatewayAsync"/>.</summary>
    public static CreateSiteWiseGatewayResult CreateGateway(string gatewayName, GatewayPlatform gatewayPlatform, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateGatewayAsync(gatewayName, gatewayPlatform, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGatewayAsync"/>.</summary>
    public static void DeleteGateway(string gatewayId, RegionEndpoint? region = null)
        => DeleteGatewayAsync(gatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeGatewayAsync"/>.</summary>
    public static DescribeSiteWiseGatewayResult DescribeGateway(string gatewayId, RegionEndpoint? region = null)
        => DescribeGatewayAsync(gatewayId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGatewaysAsync"/>.</summary>
    public static ListSiteWiseGatewaysResult ListGateways(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListGatewaysAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateGatewayAsync"/>.</summary>
    public static void UpdateGateway(string gatewayId, string gatewayName, RegionEndpoint? region = null)
        => UpdateGatewayAsync(gatewayId, gatewayName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static SiteWiseListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
