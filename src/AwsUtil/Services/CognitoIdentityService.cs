using Amazon;
using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CiCreateIdentityPoolResult(
    string? IdentityPoolId = null,
    string? IdentityPoolName = null,
    bool? AllowUnauthenticatedIdentities = null,
    bool? AllowClassicFlow = null);

public sealed record CiDeleteIdentityPoolResult(bool Success = true);

public sealed record CiDescribeIdentityPoolResult(
    string? IdentityPoolId = null,
    string? IdentityPoolName = null,
    bool? AllowUnauthenticatedIdentities = null,
    bool? AllowClassicFlow = null,
    Dictionary<string, string>? SupportedLoginProviders = null,
    List<CognitoIdentityProviderInfo>? CognitoIdentityProviders = null,
    List<string>? OpenIdConnectProviderARNs = null,
    List<string>? SamlProviderARNs = null,
    Dictionary<string, string>? IdentityPoolTags = null);

public sealed record CiListIdentityPoolsResult(
    List<IdentityPoolShortDescription>? IdentityPools = null,
    string? NextToken = null);

public sealed record CiUpdateIdentityPoolResult(
    string? IdentityPoolId = null,
    string? IdentityPoolName = null,
    bool? AllowUnauthenticatedIdentities = null);

public sealed record CiGetIdResult(string? IdentityId = null);

public sealed record CiGetCredentialsForIdentityResult(
    string? IdentityId = null,
    string? AccessKeyId = null,
    string? SecretKey = null,
    string? SessionToken = null,
    DateTime? Expiration = null);

public sealed record CiGetOpenIdTokenResult(
    string? IdentityId = null,
    string? Token = null);

public sealed record CiGetOpenIdTokenForDeveloperIdentityResult(
    string? IdentityId = null,
    string? Token = null);

public sealed record CiLookupDeveloperIdentityResult(
    string? IdentityId = null,
    List<string>? DeveloperUserIdentifierList = null,
    string? NextToken = null);

public sealed record CiMergeDeveloperIdentitiesResult(
    string? IdentityId = null);

public sealed record CiUnlinkDeveloperIdentityResult(bool Success = true);
public sealed record CiUnlinkIdentityResult(bool Success = true);

public sealed record CiDescribeIdentityResult(
    string? IdentityId = null,
    List<string>? Logins = null,
    DateTime? CreationDate = null,
    DateTime? LastModifiedDate = null);

public sealed record CiListIdentitiesResult(
    string? IdentityPoolId = null,
    List<IdentityDescription>? Identities = null,
    string? NextToken = null);

public sealed record CiDeleteIdentitiesResult(
    List<UnprocessedIdentityId>? UnprocessedIdentityIds = null);

public sealed record CiSetIdentityPoolRolesResult(bool Success = true);

public sealed record CiGetIdentityPoolRolesResult(
    string? IdentityPoolId = null,
    Dictionary<string, string>? Roles = null,
    Dictionary<string, RoleMapping>? RoleMappings = null);

public sealed record CiSetPrincipalTagAttributeMapResult(
    string? IdentityPoolId = null,
    string? IdentityProviderName = null,
    bool? UseDefaults = null,
    Dictionary<string, string>? PrincipalTags = null);

public sealed record CiGetPrincipalTagAttributeMapResult(
    string? IdentityPoolId = null,
    string? IdentityProviderName = null,
    bool? UseDefaults = null,
    Dictionary<string, string>? PrincipalTags = null);

public sealed record CiTagResourceResult(bool Success = true);
public sealed record CiUntagResourceResult(bool Success = true);

public sealed record CiListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Cognito Identity (federated identities).
/// </summary>
public static class CognitoIdentityService
{
    private static AmazonCognitoIdentityClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCognitoIdentityClient>(region);

    /// <summary>Create an identity pool.</summary>
    public static async Task<CiCreateIdentityPoolResult> CreateIdentityPoolAsync(
        CreateIdentityPoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateIdentityPoolAsync(request);
            return new CiCreateIdentityPoolResult(
                IdentityPoolId: resp.IdentityPoolId,
                IdentityPoolName: resp.IdentityPoolName,
                AllowUnauthenticatedIdentities: resp.AllowUnauthenticatedIdentities,
                AllowClassicFlow: resp.AllowClassicFlow);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Cognito identity pool");
        }
    }

    /// <summary>Delete an identity pool.</summary>
    public static async Task<CiDeleteIdentityPoolResult> DeleteIdentityPoolAsync(
        string identityPoolId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteIdentityPoolAsync(
                new DeleteIdentityPoolRequest { IdentityPoolId = identityPoolId });
            return new CiDeleteIdentityPoolResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete identity pool '{identityPoolId}'");
        }
    }

    /// <summary>Describe an identity pool.</summary>
    public static async Task<CiDescribeIdentityPoolResult> DescribeIdentityPoolAsync(
        string identityPoolId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeIdentityPoolAsync(
                new DescribeIdentityPoolRequest { IdentityPoolId = identityPoolId });
            return new CiDescribeIdentityPoolResult(
                IdentityPoolId: resp.IdentityPoolId,
                IdentityPoolName: resp.IdentityPoolName,
                AllowUnauthenticatedIdentities: resp.AllowUnauthenticatedIdentities,
                AllowClassicFlow: resp.AllowClassicFlow,
                SupportedLoginProviders: resp.SupportedLoginProviders,
                CognitoIdentityProviders: resp.CognitoIdentityProviders,
                OpenIdConnectProviderARNs: resp.OpenIdConnectProviderARNs,
                SamlProviderARNs: resp.SamlProviderARNs,
                IdentityPoolTags: resp.IdentityPoolTags);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe identity pool '{identityPoolId}'");
        }
    }

    /// <summary>List identity pools.</summary>
    public static async Task<CiListIdentityPoolsResult> ListIdentityPoolsAsync(
        int maxResults,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListIdentityPoolsRequest { MaxResults = maxResults };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListIdentityPoolsAsync(request);
            return new CiListIdentityPoolsResult(
                IdentityPools: resp.IdentityPools,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list identity pools");
        }
    }

    /// <summary>Update an identity pool.</summary>
    public static async Task<CiUpdateIdentityPoolResult> UpdateIdentityPoolAsync(
        UpdateIdentityPoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateIdentityPoolAsync(request);
            return new CiUpdateIdentityPoolResult(
                IdentityPoolId: resp.IdentityPoolId,
                IdentityPoolName: resp.IdentityPoolName,
                AllowUnauthenticatedIdentities: resp.AllowUnauthenticatedIdentities);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update identity pool");
        }
    }

    /// <summary>Get a unique identity ID for a user.</summary>
    public static async Task<CiGetIdResult> GetIdAsync(
        string identityPoolId,
        string? accountId = null,
        Dictionary<string, string>? logins = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetIdRequest { IdentityPoolId = identityPoolId };
        if (accountId != null) request.AccountId = accountId;
        if (logins != null) request.Logins = logins;

        try
        {
            var resp = await client.GetIdAsync(request);
            return new CiGetIdResult(IdentityId: resp.IdentityId);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get ID from identity pool '{identityPoolId}'");
        }
    }

    /// <summary>Get temporary AWS credentials for an identity.</summary>
    public static async Task<CiGetCredentialsForIdentityResult>
        GetCredentialsForIdentityAsync(
            string identityId,
            Dictionary<string, string>? logins = null,
            string? customRoleArn = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetCredentialsForIdentityRequest
        {
            IdentityId = identityId
        };
        if (logins != null) request.Logins = logins;
        if (customRoleArn != null) request.CustomRoleArn = customRoleArn;

        try
        {
            var resp = await client.GetCredentialsForIdentityAsync(request);
            return new CiGetCredentialsForIdentityResult(
                IdentityId: resp.IdentityId,
                AccessKeyId: resp.Credentials?.AccessKeyId,
                SecretKey: resp.Credentials?.SecretKey,
                SessionToken: resp.Credentials?.SessionToken,
                Expiration: resp.Credentials?.Expiration);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get credentials for identity '{identityId}'");
        }
    }

    /// <summary>Get an OpenID token for an identity.</summary>
    public static async Task<CiGetOpenIdTokenResult> GetOpenIdTokenAsync(
        string identityId,
        Dictionary<string, string>? logins = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetOpenIdTokenRequest { IdentityId = identityId };
        if (logins != null) request.Logins = logins;

        try
        {
            var resp = await client.GetOpenIdTokenAsync(request);
            return new CiGetOpenIdTokenResult(
                IdentityId: resp.IdentityId,
                Token: resp.Token);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get OpenID token for identity '{identityId}'");
        }
    }

    /// <summary>Get an OpenID token for a developer-authenticated identity.</summary>
    public static async Task<CiGetOpenIdTokenForDeveloperIdentityResult>
        GetOpenIdTokenForDeveloperIdentityAsync(
            string identityPoolId,
            Dictionary<string, string> logins,
            string? identityId = null,
            string? principalTags = null,
            long? tokenDuration = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetOpenIdTokenForDeveloperIdentityRequest
        {
            IdentityPoolId = identityPoolId,
            Logins = logins
        };
        if (identityId != null) request.IdentityId = identityId;
        if (tokenDuration.HasValue) request.TokenDuration = tokenDuration.Value;

        try
        {
            var resp = await client
                .GetOpenIdTokenForDeveloperIdentityAsync(request);
            return new CiGetOpenIdTokenForDeveloperIdentityResult(
                IdentityId: resp.IdentityId,
                Token: resp.Token);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get OpenID token for developer identity");
        }
    }

    /// <summary>Look up a developer identity.</summary>
    public static async Task<CiLookupDeveloperIdentityResult>
        LookupDeveloperIdentityAsync(
            string identityPoolId,
            string? identityId = null,
            string? developerUserIdentifier = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new LookupDeveloperIdentityRequest
        {
            IdentityPoolId = identityPoolId
        };
        if (identityId != null) request.IdentityId = identityId;
        if (developerUserIdentifier != null)
            request.DeveloperUserIdentifier = developerUserIdentifier;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.LookupDeveloperIdentityAsync(request);
            return new CiLookupDeveloperIdentityResult(
                IdentityId: resp.IdentityId,
                DeveloperUserIdentifierList: resp.DeveloperUserIdentifierList,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to lookup developer identity");
        }
    }

    /// <summary>Merge two developer identities.</summary>
    public static async Task<CiMergeDeveloperIdentitiesResult>
        MergeDeveloperIdentitiesAsync(
            string sourceUserIdentifier,
            string destinationUserIdentifier,
            string developerProviderName,
            string identityPoolId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.MergeDeveloperIdentitiesAsync(
                new MergeDeveloperIdentitiesRequest
                {
                    SourceUserIdentifier = sourceUserIdentifier,
                    DestinationUserIdentifier = destinationUserIdentifier,
                    DeveloperProviderName = developerProviderName,
                    IdentityPoolId = identityPoolId
                });
            return new CiMergeDeveloperIdentitiesResult(
                IdentityId: resp.IdentityId);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge developer identities");
        }
    }

    /// <summary>Unlink a developer identity from a federated identity.</summary>
    public static async Task<CiUnlinkDeveloperIdentityResult>
        UnlinkDeveloperIdentityAsync(
            string identityId,
            string identityPoolId,
            string developerProviderName,
            string developerUserIdentifier,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UnlinkDeveloperIdentityAsync(
                new UnlinkDeveloperIdentityRequest
                {
                    IdentityId = identityId,
                    IdentityPoolId = identityPoolId,
                    DeveloperProviderName = developerProviderName,
                    DeveloperUserIdentifier = developerUserIdentifier
                });
            return new CiUnlinkDeveloperIdentityResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to unlink developer identity '{identityId}'");
        }
    }

    /// <summary>Unlink a federated identity.</summary>
    public static async Task<CiUnlinkIdentityResult> UnlinkIdentityAsync(
        string identityId,
        Dictionary<string, string> logins,
        List<string> loginsToRemove,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UnlinkIdentityAsync(new UnlinkIdentityRequest
            {
                IdentityId = identityId,
                Logins = logins,
                LoginsToRemove = loginsToRemove
            });
            return new CiUnlinkIdentityResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to unlink identity '{identityId}'");
        }
    }

    /// <summary>Describe an identity.</summary>
    public static async Task<CiDescribeIdentityResult> DescribeIdentityAsync(
        string identityId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeIdentityAsync(
                new DescribeIdentityRequest { IdentityId = identityId });
            return new CiDescribeIdentityResult(
                IdentityId: resp.IdentityId,
                Logins: resp.Logins,
                CreationDate: resp.CreationDate,
                LastModifiedDate: resp.LastModifiedDate);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe identity '{identityId}'");
        }
    }

    /// <summary>List identities in an identity pool.</summary>
    public static async Task<CiListIdentitiesResult> ListIdentitiesAsync(
        string identityPoolId,
        int maxResults,
        string? nextToken = null,
        bool? hideDisabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListIdentitiesRequest
        {
            IdentityPoolId = identityPoolId,
            MaxResults = maxResults
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (hideDisabled.HasValue) request.HideDisabled = hideDisabled.Value;

        try
        {
            var resp = await client.ListIdentitiesAsync(request);
            return new CiListIdentitiesResult(
                IdentityPoolId: resp.IdentityPoolId,
                Identities: resp.Identities,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list identities in pool '{identityPoolId}'");
        }
    }

    /// <summary>Delete identities from the identity store.</summary>
    public static async Task<CiDeleteIdentitiesResult> DeleteIdentitiesAsync(
        List<string> identityIdsToDelete,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteIdentitiesAsync(
                new DeleteIdentitiesRequest
                {
                    IdentityIdsToDelete = identityIdsToDelete
                });
            return new CiDeleteIdentitiesResult(
                UnprocessedIdentityIds: resp.UnprocessedIdentityIds);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete identities");
        }
    }

    /// <summary>Set roles for an identity pool.</summary>
    public static async Task<CiSetIdentityPoolRolesResult>
        SetIdentityPoolRolesAsync(
            string identityPoolId,
            Dictionary<string, string> roles,
            Dictionary<string, RoleMapping>? roleMappings = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetIdentityPoolRolesRequest
        {
            IdentityPoolId = identityPoolId,
            Roles = roles
        };
        if (roleMappings != null) request.RoleMappings = roleMappings;

        try
        {
            await client.SetIdentityPoolRolesAsync(request);
            return new CiSetIdentityPoolRolesResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set roles for identity pool '{identityPoolId}'");
        }
    }

    /// <summary>Get roles for an identity pool.</summary>
    public static async Task<CiGetIdentityPoolRolesResult>
        GetIdentityPoolRolesAsync(
            string identityPoolId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetIdentityPoolRolesAsync(
                new GetIdentityPoolRolesRequest
                {
                    IdentityPoolId = identityPoolId
                });
            return new CiGetIdentityPoolRolesResult(
                IdentityPoolId: resp.IdentityPoolId,
                Roles: resp.Roles,
                RoleMappings: resp.RoleMappings);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get roles for identity pool '{identityPoolId}'");
        }
    }

    /// <summary>Set principal tag attribute map for an identity provider.</summary>
    public static async Task<CiSetPrincipalTagAttributeMapResult>
        SetPrincipalTagAttributeMapAsync(
            string identityPoolId,
            string identityProviderName,
            bool? useDefaults = null,
            Dictionary<string, string>? principalTags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetPrincipalTagAttributeMapRequest
        {
            IdentityPoolId = identityPoolId,
            IdentityProviderName = identityProviderName
        };
        if (useDefaults.HasValue) request.UseDefaults = useDefaults.Value;
        if (principalTags != null) request.PrincipalTags = principalTags;

        try
        {
            var resp = await client.SetPrincipalTagAttributeMapAsync(request);
            return new CiSetPrincipalTagAttributeMapResult(
                IdentityPoolId: resp.IdentityPoolId,
                IdentityProviderName: resp.IdentityProviderName,
                UseDefaults: resp.UseDefaults,
                PrincipalTags: resp.PrincipalTags);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to set principal tag attribute map");
        }
    }

    /// <summary>Get principal tag attribute map for an identity provider.</summary>
    public static async Task<CiGetPrincipalTagAttributeMapResult>
        GetPrincipalTagAttributeMapAsync(
            string identityPoolId,
            string identityProviderName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPrincipalTagAttributeMapAsync(
                new GetPrincipalTagAttributeMapRequest
                {
                    IdentityPoolId = identityPoolId,
                    IdentityProviderName = identityProviderName
                });
            return new CiGetPrincipalTagAttributeMapResult(
                IdentityPoolId: resp.IdentityPoolId,
                IdentityProviderName: resp.IdentityProviderName,
                UseDefaults: resp.UseDefaults,
                PrincipalTags: resp.PrincipalTags);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get principal tag attribute map");
        }
    }

    /// <summary>Tag a Cognito Identity resource.</summary>
    public static async Task<CiTagResourceResult> TagResourceAsync(
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
            return new CiTagResourceResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Cognito Identity resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a Cognito Identity resource.</summary>
    public static async Task<CiUntagResourceResult> UntagResourceAsync(
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
            return new CiUntagResourceResult();
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Cognito Identity resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a Cognito Identity resource.</summary>
    public static async Task<CiListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new CiListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonCognitoIdentityException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Cognito Identity resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateIdentityPoolAsync"/>.</summary>
    public static CiCreateIdentityPoolResult CreateIdentityPool(CreateIdentityPoolRequest request, RegionEndpoint? region = null)
        => CreateIdentityPoolAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteIdentityPoolAsync"/>.</summary>
    public static CiDeleteIdentityPoolResult DeleteIdentityPool(string identityPoolId, RegionEndpoint? region = null)
        => DeleteIdentityPoolAsync(identityPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeIdentityPoolAsync"/>.</summary>
    public static CiDescribeIdentityPoolResult DescribeIdentityPool(string identityPoolId, RegionEndpoint? region = null)
        => DescribeIdentityPoolAsync(identityPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListIdentityPoolsAsync"/>.</summary>
    public static CiListIdentityPoolsResult ListIdentityPools(int maxResults, string? nextToken = null, RegionEndpoint? region = null)
        => ListIdentityPoolsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateIdentityPoolAsync"/>.</summary>
    public static CiUpdateIdentityPoolResult UpdateIdentityPool(UpdateIdentityPoolRequest request, RegionEndpoint? region = null)
        => UpdateIdentityPoolAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetIdAsync"/>.</summary>
    public static CiGetIdResult GetId(string identityPoolId, string? accountId = null, Dictionary<string, string>? logins = null, RegionEndpoint? region = null)
        => GetIdAsync(identityPoolId, accountId, logins, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCredentialsForIdentityAsync"/>.</summary>
    public static CiGetCredentialsForIdentityResult GetCredentialsForIdentity(string identityId, Dictionary<string, string>? logins = null, string? customRoleArn = null, RegionEndpoint? region = null)
        => GetCredentialsForIdentityAsync(identityId, logins, customRoleArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetOpenIdTokenAsync"/>.</summary>
    public static CiGetOpenIdTokenResult GetOpenIdToken(string identityId, Dictionary<string, string>? logins = null, RegionEndpoint? region = null)
        => GetOpenIdTokenAsync(identityId, logins, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetOpenIdTokenForDeveloperIdentityAsync"/>.</summary>
    public static CiGetOpenIdTokenForDeveloperIdentityResult GetOpenIdTokenForDeveloperIdentity(string identityPoolId, Dictionary<string, string> logins, string? identityId = null, string? principalTags = null, long? tokenDuration = null, RegionEndpoint? region = null)
        => GetOpenIdTokenForDeveloperIdentityAsync(identityPoolId, logins, identityId, principalTags, tokenDuration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LookupDeveloperIdentityAsync"/>.</summary>
    public static CiLookupDeveloperIdentityResult LookupDeveloperIdentity(string identityPoolId, string? identityId = null, string? developerUserIdentifier = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => LookupDeveloperIdentityAsync(identityPoolId, identityId, developerUserIdentifier, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergeDeveloperIdentitiesAsync"/>.</summary>
    public static CiMergeDeveloperIdentitiesResult MergeDeveloperIdentities(string sourceUserIdentifier, string destinationUserIdentifier, string developerProviderName, string identityPoolId, RegionEndpoint? region = null)
        => MergeDeveloperIdentitiesAsync(sourceUserIdentifier, destinationUserIdentifier, developerProviderName, identityPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UnlinkDeveloperIdentityAsync"/>.</summary>
    public static CiUnlinkDeveloperIdentityResult UnlinkDeveloperIdentity(string identityId, string identityPoolId, string developerProviderName, string developerUserIdentifier, RegionEndpoint? region = null)
        => UnlinkDeveloperIdentityAsync(identityId, identityPoolId, developerProviderName, developerUserIdentifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UnlinkIdentityAsync"/>.</summary>
    public static CiUnlinkIdentityResult UnlinkIdentity(string identityId, Dictionary<string, string> logins, List<string> loginsToRemove, RegionEndpoint? region = null)
        => UnlinkIdentityAsync(identityId, logins, loginsToRemove, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeIdentityAsync"/>.</summary>
    public static CiDescribeIdentityResult DescribeIdentity(string identityId, RegionEndpoint? region = null)
        => DescribeIdentityAsync(identityId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListIdentitiesAsync"/>.</summary>
    public static CiListIdentitiesResult ListIdentities(string identityPoolId, int maxResults, string? nextToken = null, bool? hideDisabled = null, RegionEndpoint? region = null)
        => ListIdentitiesAsync(identityPoolId, maxResults, nextToken, hideDisabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteIdentitiesAsync"/>.</summary>
    public static CiDeleteIdentitiesResult DeleteIdentities(List<string> identityIdsToDelete, RegionEndpoint? region = null)
        => DeleteIdentitiesAsync(identityIdsToDelete, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetIdentityPoolRolesAsync"/>.</summary>
    public static CiSetIdentityPoolRolesResult SetIdentityPoolRoles(string identityPoolId, Dictionary<string, string> roles, Dictionary<string, RoleMapping>? roleMappings = null, RegionEndpoint? region = null)
        => SetIdentityPoolRolesAsync(identityPoolId, roles, roleMappings, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetIdentityPoolRolesAsync"/>.</summary>
    public static CiGetIdentityPoolRolesResult GetIdentityPoolRoles(string identityPoolId, RegionEndpoint? region = null)
        => GetIdentityPoolRolesAsync(identityPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetPrincipalTagAttributeMapAsync"/>.</summary>
    public static CiSetPrincipalTagAttributeMapResult SetPrincipalTagAttributeMap(string identityPoolId, string identityProviderName, bool? useDefaults = null, Dictionary<string, string>? principalTags = null, RegionEndpoint? region = null)
        => SetPrincipalTagAttributeMapAsync(identityPoolId, identityProviderName, useDefaults, principalTags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPrincipalTagAttributeMapAsync"/>.</summary>
    public static CiGetPrincipalTagAttributeMapResult GetPrincipalTagAttributeMap(string identityPoolId, string identityProviderName, RegionEndpoint? region = null)
        => GetPrincipalTagAttributeMapAsync(identityPoolId, identityProviderName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static CiTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static CiUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static CiListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
