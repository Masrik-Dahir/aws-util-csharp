using Amazon;
using Amazon.CodeStarconnections;
using Amazon.CodeStarconnections.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CscCreateConnectionResult(
    string? ConnectionArn = null,
    List<Amazon.CodeStarconnections.Model.Tag>? Tags = null);

public sealed record CscDeleteConnectionResult(bool Success = true);

public sealed record CscGetConnectionResult(
    Connection? Connection = null);

public sealed record CscListConnectionsResult(
    List<Connection>? Connections = null,
    string? NextToken = null);

public sealed record CscCreateHostResult(
    string? HostArn = null,
    List<Amazon.CodeStarconnections.Model.Tag>? Tags = null);

public sealed record CscDeleteHostResult(bool Success = true);

public sealed record CscGetHostResult(
    string? Name = null,
    string? Status = null,
    string? ProviderType = null,
    string? ProviderEndpoint = null,
    VpcConfiguration? VpcConfiguration = null);

public sealed record CscListHostsResult(
    List<Host>? Hosts = null,
    string? NextToken = null);

public sealed record CscUpdateHostResult(bool Success = true);

public sealed record CscTagResourceResult(bool Success = true);
public sealed record CscUntagResourceResult(bool Success = true);

public sealed record CscListTagsForResourceResult(
    List<Amazon.CodeStarconnections.Model.Tag>? Tags = null);

public sealed record CscCreateRepositoryLinkResult(
    RepositoryLinkInfo? RepositoryLinkInfo = null);

public sealed record CscDeleteRepositoryLinkResult(bool Success = true);

public sealed record CscGetRepositoryLinkResult(
    RepositoryLinkInfo? RepositoryLinkInfo = null);

public sealed record CscListRepositoryLinksResult(
    List<RepositoryLinkInfo>? RepositoryLinks = null,
    string? NextToken = null);

public sealed record CscUpdateRepositoryLinkResult(
    RepositoryLinkInfo? RepositoryLinkInfo = null);

public sealed record CscCreateSyncConfigurationResult(
    SyncConfiguration? SyncConfiguration = null);

public sealed record CscDeleteSyncConfigurationResult(bool Success = true);

public sealed record CscGetSyncConfigurationResult(
    SyncConfiguration? SyncConfiguration = null);

public sealed record CscListSyncConfigurationsResult(
    List<SyncConfiguration>? SyncConfigurations = null,
    string? NextToken = null);

public sealed record CscUpdateSyncConfigurationResult(
    SyncConfiguration? SyncConfiguration = null);

public sealed record CscGetSyncBlockerSummaryResult(
    SyncBlockerSummary? SyncBlockerSummary = null);

public sealed record CscUpdateSyncBlockerResult(
    string? ResourceName = null,
    SyncBlocker? SyncBlocker = null);

public sealed record CscGetRepositorySyncStatusResult(
    List<RepositorySyncEvent>? Events = null,
    string? LatestSync = null);

public sealed record CscGetResourceSyncStatusResult(
    List<ResourceSyncEvent>? Events = null,
    string? LatestSync = null,
    ResourceSyncAttempt? DesiredState = null,
    ResourceSyncAttempt? LatestSuccessfulSync = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodeStar Connections.
/// </summary>
public static class CodeStarConnectionsService
{
    private static AmazonCodeStarconnectionsClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodeStarconnectionsClient>(region);

    /// <summary>
    /// Create a connection.
    /// </summary>
    public static async Task<CscCreateConnectionResult>
        CreateConnectionAsync(
            CreateConnectionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateConnectionAsync(request);
            return new CscCreateConnectionResult(
                ConnectionArn: resp.ConnectionArn,
                Tags: resp.Tags);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create connection");
        }
    }

    /// <summary>
    /// Delete a connection.
    /// </summary>
    public static async Task<CscDeleteConnectionResult>
        DeleteConnectionAsync(
            string connectionArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConnectionAsync(
                new DeleteConnectionRequest
                {
                    ConnectionArn = connectionArn
                });
            return new CscDeleteConnectionResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete connection");
        }
    }

    /// <summary>
    /// Get information about a connection.
    /// </summary>
    public static async Task<CscGetConnectionResult> GetConnectionAsync(
        string connectionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetConnectionAsync(
                new GetConnectionRequest
                {
                    ConnectionArn = connectionArn
                });
            return new CscGetConnectionResult(
                Connection: resp.Connection);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get connection");
        }
    }

    /// <summary>
    /// List connections.
    /// </summary>
    public static async Task<CscListConnectionsResult>
        ListConnectionsAsync(
            string? providerTypeFilter = null,
            string? hostArnFilter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConnectionsRequest();
        if (providerTypeFilter != null)
            request.ProviderTypeFilter =
                new ProviderType(providerTypeFilter);
        if (hostArnFilter != null) request.HostArnFilter = hostArnFilter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListConnectionsAsync(request);
            return new CscListConnectionsResult(
                Connections: resp.Connections,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list connections");
        }
    }

    /// <summary>
    /// Create a host.
    /// </summary>
    public static async Task<CscCreateHostResult> CreateHostAsync(
        CreateHostRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateHostAsync(request);
            return new CscCreateHostResult(
                HostArn: resp.HostArn,
                Tags: resp.Tags);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create host");
        }
    }

    /// <summary>
    /// Delete a host.
    /// </summary>
    public static async Task<CscDeleteHostResult> DeleteHostAsync(
        string hostArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteHostAsync(
                new DeleteHostRequest { HostArn = hostArn });
            return new CscDeleteHostResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete host");
        }
    }

    /// <summary>
    /// Get information about a host.
    /// </summary>
    public static async Task<CscGetHostResult> GetHostAsync(
        string hostArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetHostAsync(
                new GetHostRequest { HostArn = hostArn });
            return new CscGetHostResult(
                Name: resp.Name,
                Status: resp.Status,
                ProviderType: resp.ProviderType?.Value,
                ProviderEndpoint: resp.ProviderEndpoint,
                VpcConfiguration: resp.VpcConfiguration);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get host");
        }
    }

    /// <summary>
    /// List hosts.
    /// </summary>
    public static async Task<CscListHostsResult> ListHostsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListHostsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListHostsAsync(request);
            return new CscListHostsResult(
                Hosts: resp.Hosts,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list hosts");
        }
    }

    /// <summary>
    /// Update a host.
    /// </summary>
    public static async Task<CscUpdateHostResult> UpdateHostAsync(
        UpdateHostRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateHostAsync(request);
            return new CscUpdateHostResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update host");
        }
    }

    /// <summary>
    /// Tag a CodeStar Connections resource.
    /// </summary>
    public static async Task<CscTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Amazon.CodeStarconnections.Model.Tag> tags,
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
            return new CscTagResourceResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag CodeStar Connections resource");
        }
    }

    /// <summary>
    /// Remove tags from a CodeStar Connections resource.
    /// </summary>
    public static async Task<CscUntagResourceResult> UntagResourceAsync(
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
            return new CscUntagResourceResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag CodeStar Connections resource");
        }
    }

    /// <summary>
    /// List tags for a CodeStar Connections resource.
    /// </summary>
    public static async Task<CscListTagsForResourceResult>
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
            return new CscListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for CodeStar Connections resource");
        }
    }

    /// <summary>
    /// Create a repository link.
    /// </summary>
    public static async Task<CscCreateRepositoryLinkResult>
        CreateRepositoryLinkAsync(
            CreateRepositoryLinkRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRepositoryLinkAsync(request);
            return new CscCreateRepositoryLinkResult(
                RepositoryLinkInfo: resp.RepositoryLinkInfo);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create repository link");
        }
    }

    /// <summary>
    /// Delete a repository link.
    /// </summary>
    public static async Task<CscDeleteRepositoryLinkResult>
        DeleteRepositoryLinkAsync(
            string repositoryLinkId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRepositoryLinkAsync(
                new DeleteRepositoryLinkRequest
                {
                    RepositoryLinkId = repositoryLinkId
                });
            return new CscDeleteRepositoryLinkResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete repository link");
        }
    }

    /// <summary>
    /// Get information about a repository link.
    /// </summary>
    public static async Task<CscGetRepositoryLinkResult>
        GetRepositoryLinkAsync(
            string repositoryLinkId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRepositoryLinkAsync(
                new GetRepositoryLinkRequest
                {
                    RepositoryLinkId = repositoryLinkId
                });
            return new CscGetRepositoryLinkResult(
                RepositoryLinkInfo: resp.RepositoryLinkInfo);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get repository link");
        }
    }

    /// <summary>
    /// List repository links.
    /// </summary>
    public static async Task<CscListRepositoryLinksResult>
        ListRepositoryLinksAsync(
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRepositoryLinksRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRepositoryLinksAsync(request);
            return new CscListRepositoryLinksResult(
                RepositoryLinks: resp.RepositoryLinks,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list repository links");
        }
    }

    /// <summary>
    /// Update a repository link.
    /// </summary>
    public static async Task<CscUpdateRepositoryLinkResult>
        UpdateRepositoryLinkAsync(
            UpdateRepositoryLinkRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRepositoryLinkAsync(request);
            return new CscUpdateRepositoryLinkResult(
                RepositoryLinkInfo: resp.RepositoryLinkInfo);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update repository link");
        }
    }

    /// <summary>
    /// Create a sync configuration.
    /// </summary>
    public static async Task<CscCreateSyncConfigurationResult>
        CreateSyncConfigurationAsync(
            CreateSyncConfigurationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.CreateSyncConfigurationAsync(request);
            return new CscCreateSyncConfigurationResult(
                SyncConfiguration: resp.SyncConfiguration);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create sync configuration");
        }
    }

    /// <summary>
    /// Delete a sync configuration.
    /// </summary>
    public static async Task<CscDeleteSyncConfigurationResult>
        DeleteSyncConfigurationAsync(
            string syncType,
            string resourceName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSyncConfigurationAsync(
                new DeleteSyncConfigurationRequest
                {
                    SyncType = new SyncConfigurationType(syncType),
                    ResourceName = resourceName
                });
            return new CscDeleteSyncConfigurationResult();
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete sync configuration");
        }
    }

    /// <summary>
    /// Get information about a sync configuration.
    /// </summary>
    public static async Task<CscGetSyncConfigurationResult>
        GetSyncConfigurationAsync(
            string syncType,
            string resourceName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSyncConfigurationAsync(
                new GetSyncConfigurationRequest
                {
                    SyncType = new SyncConfigurationType(syncType),
                    ResourceName = resourceName
                });
            return new CscGetSyncConfigurationResult(
                SyncConfiguration: resp.SyncConfiguration);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get sync configuration");
        }
    }

    /// <summary>
    /// List sync configurations.
    /// </summary>
    public static async Task<CscListSyncConfigurationsResult>
        ListSyncConfigurationsAsync(
            string repositoryLinkId,
            string syncType,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSyncConfigurationsRequest
        {
            RepositoryLinkId = repositoryLinkId,
            SyncType = new SyncConfigurationType(syncType)
        };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp =
                await client.ListSyncConfigurationsAsync(request);
            return new CscListSyncConfigurationsResult(
                SyncConfigurations: resp.SyncConfigurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list sync configurations");
        }
    }

    /// <summary>
    /// Update a sync configuration.
    /// </summary>
    public static async Task<CscUpdateSyncConfigurationResult>
        UpdateSyncConfigurationAsync(
            UpdateSyncConfigurationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.UpdateSyncConfigurationAsync(request);
            return new CscUpdateSyncConfigurationResult(
                SyncConfiguration: resp.SyncConfiguration);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update sync configuration");
        }
    }

    /// <summary>
    /// Get the sync blocker summary for a resource.
    /// </summary>
    public static async Task<CscGetSyncBlockerSummaryResult>
        GetSyncBlockerSummaryAsync(
            string syncType,
            string resourceName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSyncBlockerSummaryAsync(
                new GetSyncBlockerSummaryRequest
                {
                    SyncType = new SyncConfigurationType(syncType),
                    ResourceName = resourceName
                });
            return new CscGetSyncBlockerSummaryResult(
                SyncBlockerSummary: resp.SyncBlockerSummary);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get sync blocker summary");
        }
    }

    /// <summary>
    /// Update a sync blocker.
    /// </summary>
    public static async Task<CscUpdateSyncBlockerResult>
        UpdateSyncBlockerAsync(
            UpdateSyncBlockerRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateSyncBlockerAsync(request);
            return new CscUpdateSyncBlockerResult(
                ResourceName: resp.ResourceName,
                SyncBlocker: resp.SyncBlocker);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update sync blocker");
        }
    }

    /// <summary>
    /// Get the sync status for a repository.
    /// </summary>
    public static async Task<CscGetRepositorySyncStatusResult>
        GetRepositorySyncStatusAsync(
            string branch,
            string repositoryLinkId,
            string syncType,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRepositorySyncStatusAsync(
                new GetRepositorySyncStatusRequest
                {
                    Branch = branch,
                    RepositoryLinkId = repositoryLinkId,
                    SyncType = new SyncConfigurationType(syncType)
                });
            return new CscGetRepositorySyncStatusResult(
                Events: null,
                LatestSync: resp.LatestSync?.ToString());
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get repository sync status");
        }
    }

    /// <summary>
    /// Get the sync status for a resource.
    /// </summary>
    public static async Task<CscGetResourceSyncStatusResult>
        GetResourceSyncStatusAsync(
            string resourceName,
            string syncType,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetResourceSyncStatusAsync(
                new GetResourceSyncStatusRequest
                {
                    ResourceName = resourceName,
                    SyncType = new SyncConfigurationType(syncType)
                });
            return new CscGetResourceSyncStatusResult(
                Events: null,
                LatestSync: resp.LatestSync?.ToString(),
                DesiredState: null,
                LatestSuccessfulSync: resp.LatestSuccessfulSync);
        }
        catch (AmazonCodeStarconnectionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get resource sync status");
        }
    }
}
