using Amazon;
using Amazon.CodeArtifact;
using Amazon.CodeArtifact.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CaCreateDomainResult(
    DomainDescription? Domain = null);

public sealed record CaDeleteDomainResult(
    DomainDescription? Domain = null);

public sealed record CaDescribeDomainResult(
    DomainDescription? Domain = null);

public sealed record CaListDomainsResult(
    List<DomainSummary>? Domains = null,
    string? NextToken = null);

public sealed record CaCreateRepositoryResult(
    RepositoryDescription? Repository = null);

public sealed record CaDeleteRepositoryResult(
    RepositoryDescription? Repository = null);

public sealed record CaDescribeRepositoryResult(
    RepositoryDescription? Repository = null);

public sealed record CaListRepositoriesResult(
    List<RepositorySummary>? Repositories = null,
    string? NextToken = null);

public sealed record CaUpdateRepositoryResult(
    RepositoryDescription? Repository = null);

public sealed record CaListRepositoriesInDomainResult(
    List<RepositorySummary>? Repositories = null,
    string? NextToken = null);

public sealed record CaAssociateExternalConnectionResult(
    RepositoryDescription? Repository = null);

public sealed record CaDisassociateExternalConnectionResult(
    RepositoryDescription? Repository = null);

public sealed record CaGetAuthorizationTokenResult(
    string? AuthorizationToken = null,
    DateTime? Expiration = null);

public sealed record CaGetDomainPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaPutDomainPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaDeleteDomainPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaGetRepositoryEndpointResult(
    string? RepositoryEndpoint = null);

public sealed record CaGetRepositoryPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaPutRepositoryPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaDeleteRepositoryPermissionsPolicyResult(
    ResourcePolicy? Policy = null);

public sealed record CaListPackagesResult(
    List<PackageSummary>? Packages = null,
    string? NextToken = null);

public sealed record CaListPackageVersionsResult(
    List<PackageVersionSummary>? Versions = null,
    string? NextToken = null);

public sealed record CaDescribePackageVersionResult(
    PackageVersionDescription? PackageVersion = null);

public sealed record CaGetPackageVersionReadmeResult(
    string? Readme = null,
    string? Format = null,
    string? Namespace = null,
    string? Package = null,
    string? Version = null);

public sealed record CaListPackageVersionAssetsResult(
    List<AssetSummary>? Assets = null,
    string? NextToken = null);

public sealed record CaGetPackageVersionAssetResult(
    string? AssetName = null,
    string? PackageVersion = null,
    string? PackageVersionRevision = null,
    Stream? Asset = null);

public sealed record CaDisposePackageVersionsResult(
    Dictionary<string, SuccessfulPackageVersionInfo>? SuccessfulVersions = null,
    Dictionary<string, PackageVersionError>? FailedVersions = null);

public sealed record CaDeletePackageVersionsResult(
    Dictionary<string, SuccessfulPackageVersionInfo>? SuccessfulVersions = null,
    Dictionary<string, PackageVersionError>? FailedVersions = null);

public sealed record CaCopyPackageVersionsResult(
    Dictionary<string, SuccessfulPackageVersionInfo>? SuccessfulVersions = null,
    Dictionary<string, PackageVersionError>? FailedVersions = null);

public sealed record CaUpdatePackageVersionsStatusResult(
    Dictionary<string, SuccessfulPackageVersionInfo>? SuccessfulVersions = null,
    Dictionary<string, PackageVersionError>? FailedVersions = null);

public sealed record CaPublishPackageVersionResult(
    string? Format = null,
    string? Namespace = null,
    string? Package = null,
    string? Version = null,
    string? VersionRevision = null,
    string? Status = null,
    AssetSummary? Asset = null);

public sealed record CaTagResourceResult(bool Success = true);
public sealed record CaUntagResourceResult(bool Success = true);

public sealed record CaListTagsForResourceResult(
    List<Amazon.CodeArtifact.Model.Tag>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodeArtifact.
/// </summary>
public static class CodeArtifactService
{
    private static AmazonCodeArtifactClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodeArtifactClient>(region);

    /// <summary>
    /// Create a CodeArtifact domain.
    /// </summary>
    public static async Task<CaCreateDomainResult> CreateDomainAsync(
        CreateDomainRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDomainAsync(request);
            return new CaCreateDomainResult(Domain: resp.Domain);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeArtifact domain");
        }
    }

    /// <summary>
    /// Delete a CodeArtifact domain.
    /// </summary>
    public static async Task<CaDeleteDomainResult> DeleteDomainAsync(
        string domain,
        string? domainOwner = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDomainRequest { Domain = domain };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp = await client.DeleteDomainAsync(request);
            return new CaDeleteDomainResult(Domain: resp.Domain);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeArtifact domain '{domain}'");
        }
    }

    /// <summary>
    /// Describe a CodeArtifact domain.
    /// </summary>
    public static async Task<CaDescribeDomainResult> DescribeDomainAsync(
        string domain,
        string? domainOwner = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDomainRequest { Domain = domain };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp = await client.DescribeDomainAsync(request);
            return new CaDescribeDomainResult(Domain: resp.Domain);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe CodeArtifact domain '{domain}'");
        }
    }

    /// <summary>
    /// List CodeArtifact domains.
    /// </summary>
    public static async Task<CaListDomainsResult> ListDomainsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDomainsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDomainsAsync(request);
            return new CaListDomainsResult(
                Domains: resp.Domains,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list CodeArtifact domains");
        }
    }

    /// <summary>
    /// Create a CodeArtifact repository.
    /// </summary>
    public static async Task<CaCreateRepositoryResult>
        CreateRepositoryAsync(
            CreateRepositoryRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRepositoryAsync(request);
            return new CaCreateRepositoryResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeArtifact repository");
        }
    }

    /// <summary>
    /// Delete a CodeArtifact repository.
    /// </summary>
    public static async Task<CaDeleteRepositoryResult>
        DeleteRepositoryAsync(
            string domain,
            string repository,
            string? domainOwner = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRepositoryRequest
        {
            Domain = domain,
            Repository = repository
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp = await client.DeleteRepositoryAsync(request);
            return new CaDeleteRepositoryResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeArtifact repository '{repository}'");
        }
    }

    /// <summary>
    /// Describe a CodeArtifact repository.
    /// </summary>
    public static async Task<CaDescribeRepositoryResult>
        DescribeRepositoryAsync(
            string domain,
            string repository,
            string? domainOwner = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeRepositoryRequest
        {
            Domain = domain,
            Repository = repository
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp = await client.DescribeRepositoryAsync(request);
            return new CaDescribeRepositoryResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe CodeArtifact repository '{repository}'");
        }
    }

    /// <summary>
    /// List CodeArtifact repositories.
    /// </summary>
    public static async Task<CaListRepositoriesResult>
        ListRepositoriesAsync(
            string? repositoryPrefix = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRepositoriesRequest();
        if (repositoryPrefix != null)
            request.RepositoryPrefix = repositoryPrefix;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRepositoriesAsync(request);
            return new CaListRepositoriesResult(
                Repositories: resp.Repositories,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list CodeArtifact repositories");
        }
    }

    /// <summary>
    /// Update a CodeArtifact repository.
    /// </summary>
    public static async Task<CaUpdateRepositoryResult>
        UpdateRepositoryAsync(
            UpdateRepositoryRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRepositoryAsync(request);
            return new CaUpdateRepositoryResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update CodeArtifact repository");
        }
    }

    /// <summary>
    /// List repositories in a domain.
    /// </summary>
    public static async Task<CaListRepositoriesInDomainResult>
        ListRepositoriesInDomainAsync(
            string domain,
            string? domainOwner = null,
            string? administratorAccount = null,
            string? repositoryPrefix = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRepositoriesInDomainRequest
        {
            Domain = domain
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;
        if (administratorAccount != null)
            request.AdministratorAccount = administratorAccount;
        if (repositoryPrefix != null)
            request.RepositoryPrefix = repositoryPrefix;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp =
                await client.ListRepositoriesInDomainAsync(request);
            return new CaListRepositoriesInDomainResult(
                Repositories: resp.Repositories,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list repositories in domain");
        }
    }

    /// <summary>
    /// Associate an external connection with a repository.
    /// </summary>
    public static async Task<CaAssociateExternalConnectionResult>
        AssociateExternalConnectionAsync(
            AssociateExternalConnectionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.AssociateExternalConnectionAsync(request);
            return new CaAssociateExternalConnectionResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to associate external connection");
        }
    }

    /// <summary>
    /// Disassociate an external connection from a repository.
    /// </summary>
    public static async Task<CaDisassociateExternalConnectionResult>
        DisassociateExternalConnectionAsync(
            DisassociateExternalConnectionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.DisassociateExternalConnectionAsync(request);
            return new CaDisassociateExternalConnectionResult(
                Repository: resp.Repository);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disassociate external connection");
        }
    }

    /// <summary>
    /// Get an authorization token for a domain.
    /// </summary>
    public static async Task<CaGetAuthorizationTokenResult>
        GetAuthorizationTokenAsync(
            string domain,
            string? domainOwner = null,
            long? durationSeconds = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAuthorizationTokenRequest
        {
            Domain = domain
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;
        if (durationSeconds.HasValue)
            request.DurationSeconds = durationSeconds.Value;

        try
        {
            var resp = await client.GetAuthorizationTokenAsync(request);
            return new CaGetAuthorizationTokenResult(
                AuthorizationToken: resp.AuthorizationToken,
                Expiration: resp.Expiration);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get authorization token");
        }
    }

    /// <summary>
    /// Get the domain permissions policy.
    /// </summary>
    public static async Task<CaGetDomainPermissionsPolicyResult>
        GetDomainPermissionsPolicyAsync(
            string domain,
            string? domainOwner = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDomainPermissionsPolicyRequest
        {
            Domain = domain
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp =
                await client.GetDomainPermissionsPolicyAsync(request);
            return new CaGetDomainPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get domain permissions policy");
        }
    }

    /// <summary>
    /// Set the domain permissions policy.
    /// </summary>
    public static async Task<CaPutDomainPermissionsPolicyResult>
        PutDomainPermissionsPolicyAsync(
            PutDomainPermissionsPolicyRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.PutDomainPermissionsPolicyAsync(request);
            return new CaPutDomainPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put domain permissions policy");
        }
    }

    /// <summary>
    /// Delete the domain permissions policy.
    /// </summary>
    public static async Task<CaDeleteDomainPermissionsPolicyResult>
        DeleteDomainPermissionsPolicyAsync(
            string domain,
            string? domainOwner = null,
            string? policyRevision = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDomainPermissionsPolicyRequest
        {
            Domain = domain
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;
        if (policyRevision != null)
            request.PolicyRevision = policyRevision;

        try
        {
            var resp =
                await client.DeleteDomainPermissionsPolicyAsync(request);
            return new CaDeleteDomainPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete domain permissions policy");
        }
    }

    /// <summary>
    /// Get the endpoint of a repository for a specific format.
    /// </summary>
    public static async Task<CaGetRepositoryEndpointResult>
        GetRepositoryEndpointAsync(
            string domain,
            string repository,
            string format,
            string? domainOwner = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRepositoryEndpointRequest
        {
            Domain = domain,
            Repository = repository,
            Format = new PackageFormat(format)
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp =
                await client.GetRepositoryEndpointAsync(request);
            return new CaGetRepositoryEndpointResult(
                RepositoryEndpoint: resp.RepositoryEndpoint);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get repository endpoint");
        }
    }

    /// <summary>
    /// Get the repository permissions policy.
    /// </summary>
    public static async Task<CaGetRepositoryPermissionsPolicyResult>
        GetRepositoryPermissionsPolicyAsync(
            string domain,
            string repository,
            string? domainOwner = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRepositoryPermissionsPolicyRequest
        {
            Domain = domain,
            Repository = repository
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;

        try
        {
            var resp =
                await client.GetRepositoryPermissionsPolicyAsync(request);
            return new CaGetRepositoryPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get repository permissions policy");
        }
    }

    /// <summary>
    /// Set the repository permissions policy.
    /// </summary>
    public static async Task<CaPutRepositoryPermissionsPolicyResult>
        PutRepositoryPermissionsPolicyAsync(
            PutRepositoryPermissionsPolicyRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.PutRepositoryPermissionsPolicyAsync(request);
            return new CaPutRepositoryPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put repository permissions policy");
        }
    }

    /// <summary>
    /// Delete the repository permissions policy.
    /// </summary>
    public static async Task<CaDeleteRepositoryPermissionsPolicyResult>
        DeleteRepositoryPermissionsPolicyAsync(
            string domain,
            string repository,
            string? domainOwner = null,
            string? policyRevision = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRepositoryPermissionsPolicyRequest
        {
            Domain = domain,
            Repository = repository
        };
        if (domainOwner != null) request.DomainOwner = domainOwner;
        if (policyRevision != null)
            request.PolicyRevision = policyRevision;

        try
        {
            var resp =
                await client.DeleteRepositoryPermissionsPolicyAsync(
                    request);
            return new CaDeleteRepositoryPermissionsPolicyResult(
                Policy: resp.Policy);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete repository permissions policy");
        }
    }

    /// <summary>
    /// List packages in a repository.
    /// </summary>
    public static async Task<CaListPackagesResult> ListPackagesAsync(
        ListPackagesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListPackagesAsync(request);
            return new CaListPackagesResult(
                Packages: resp.Packages,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list packages");
        }
    }

    /// <summary>
    /// List versions of a package.
    /// </summary>
    public static async Task<CaListPackageVersionsResult>
        ListPackageVersionsAsync(
            ListPackageVersionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListPackageVersionsAsync(request);
            return new CaListPackageVersionsResult(
                Versions: resp.Versions,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list package versions");
        }
    }

    /// <summary>
    /// Describe a package version.
    /// </summary>
    public static async Task<CaDescribePackageVersionResult>
        DescribePackageVersionAsync(
            DescribePackageVersionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePackageVersionAsync(request);
            return new CaDescribePackageVersionResult(
                PackageVersion: resp.PackageVersion);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe package version");
        }
    }

    /// <summary>
    /// Get the readme for a package version.
    /// </summary>
    public static async Task<CaGetPackageVersionReadmeResult>
        GetPackageVersionReadmeAsync(
            GetPackageVersionReadmeRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.GetPackageVersionReadmeAsync(request);
            return new CaGetPackageVersionReadmeResult(
                Readme: resp.Readme,
                Format: resp.Format?.Value,
                Namespace: resp.Namespace,
                Package: resp.Package,
                Version: resp.Version);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get package version readme");
        }
    }

    /// <summary>
    /// List assets for a package version.
    /// </summary>
    public static async Task<CaListPackageVersionAssetsResult>
        ListPackageVersionAssetsAsync(
            ListPackageVersionAssetsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.ListPackageVersionAssetsAsync(request);
            return new CaListPackageVersionAssetsResult(
                Assets: resp.Assets,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list package version assets");
        }
    }

    /// <summary>
    /// Get a package version asset.
    /// </summary>
    public static async Task<CaGetPackageVersionAssetResult>
        GetPackageVersionAssetAsync(
            GetPackageVersionAssetRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.GetPackageVersionAssetAsync(request);
            return new CaGetPackageVersionAssetResult(
                AssetName: resp.AssetName,
                PackageVersion: resp.PackageVersion,
                PackageVersionRevision: resp.PackageVersionRevision,
                Asset: resp.Asset);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get package version asset");
        }
    }

    /// <summary>
    /// Dispose of package versions.
    /// </summary>
    public static async Task<CaDisposePackageVersionsResult>
        DisposePackageVersionsAsync(
            DisposePackageVersionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.DisposePackageVersionsAsync(request);
            return new CaDisposePackageVersionsResult(
                SuccessfulVersions: resp.SuccessfulVersions,
                FailedVersions: resp.FailedVersions);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to dispose package versions");
        }
    }

    /// <summary>
    /// Delete package versions.
    /// </summary>
    public static async Task<CaDeletePackageVersionsResult>
        DeletePackageVersionsAsync(
            DeletePackageVersionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.DeletePackageVersionsAsync(request);
            return new CaDeletePackageVersionsResult(
                SuccessfulVersions: resp.SuccessfulVersions,
                FailedVersions: resp.FailedVersions);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete package versions");
        }
    }

    /// <summary>
    /// Copy package versions between repositories.
    /// </summary>
    public static async Task<CaCopyPackageVersionsResult>
        CopyPackageVersionsAsync(
            CopyPackageVersionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CopyPackageVersionsAsync(request);
            return new CaCopyPackageVersionsResult(
                SuccessfulVersions: resp.SuccessfulVersions,
                FailedVersions: resp.FailedVersions);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to copy package versions");
        }
    }

    /// <summary>
    /// Update the status of package versions.
    /// </summary>
    public static async Task<CaUpdatePackageVersionsStatusResult>
        UpdatePackageVersionsStatusAsync(
            UpdatePackageVersionsStatusRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.UpdatePackageVersionsStatusAsync(request);
            return new CaUpdatePackageVersionsStatusResult(
                SuccessfulVersions: resp.SuccessfulVersions,
                FailedVersions: resp.FailedVersions);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update package versions status");
        }
    }

    /// <summary>
    /// Publish a package version.
    /// </summary>
    public static async Task<CaPublishPackageVersionResult>
        PublishPackageVersionAsync(
            PublishPackageVersionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.PublishPackageVersionAsync(request);
            return new CaPublishPackageVersionResult(
                Format: resp.Format?.Value,
                Namespace: resp.Namespace,
                Package: resp.Package,
                Version: resp.Version,
                VersionRevision: resp.VersionRevision,
                Status: resp.Status?.Value,
                Asset: resp.Asset);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to publish package version");
        }
    }

    /// <summary>
    /// Tag a CodeArtifact resource.
    /// </summary>
    public static async Task<CaTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Amazon.CodeArtifact.Model.Tag> tags,
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
            return new CaTagResourceResult();
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag CodeArtifact resource");
        }
    }

    /// <summary>
    /// Remove tags from a CodeArtifact resource.
    /// </summary>
    public static async Task<CaUntagResourceResult> UntagResourceAsync(
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
            return new CaUntagResourceResult();
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag CodeArtifact resource");
        }
    }

    /// <summary>
    /// List tags for a CodeArtifact resource.
    /// </summary>
    public static async Task<CaListTagsForResourceResult>
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
            return new CaListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonCodeArtifactException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for CodeArtifact resource");
        }
    }
}
