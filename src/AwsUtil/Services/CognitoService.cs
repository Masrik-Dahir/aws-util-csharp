using Amazon;
using Amazon.CognitoIdentityProvider;
using Amazon.CognitoIdentityProvider.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────────────

public sealed record CreateUserPoolResult(string? UserPoolId = null, string? Arn = null);
public sealed record DescribeUserPoolResult(UserPoolType? UserPool = null);
public sealed record ListUserPoolsResult(List<UserPoolDescriptionType>? UserPools = null, string? NextToken = null);
public sealed record CreateUserPoolClientResult(UserPoolClientType? UserPoolClient = null);
public sealed record DescribeUserPoolClientResult(UserPoolClientType? UserPoolClient = null);
public sealed record ListUserPoolClientsResult(List<UserPoolClientDescription>? UserPoolClients = null, string? NextToken = null);
public sealed record AdminCreateUserResult(UserType? User = null);
public sealed record AdminGetUserResult(
    string? Username = null,
    List<AttributeType>? UserAttributes = null,
    string? UserStatus = null,
    bool? Enabled = null,
    DateTime? UserCreateDate = null,
    DateTime? UserLastModifiedDate = null);
public sealed record AdminInitiateAuthResult(
    ChallengeNameType? ChallengeName = null,
    Dictionary<string, string>? ChallengeParameters = null,
    string? Session = null,
    AuthenticationResultType? AuthenticationResult = null);
public sealed record AdminRespondToAuthChallengeResult(
    ChallengeNameType? ChallengeName = null,
    Dictionary<string, string>? ChallengeParameters = null,
    string? Session = null,
    AuthenticationResultType? AuthenticationResult = null);
public sealed record AdminListGroupsForUserResult(List<GroupType>? Groups = null, string? NextToken = null);
public sealed record CreateGroupResult(GroupType? Group = null);
public sealed record GetGroupResult(GroupType? Group = null);
public sealed record ListGroupsResult(List<GroupType>? Groups = null, string? NextToken = null);
public sealed record UpdateGroupResult(GroupType? Group = null);
public sealed record AdminListDevicesResult(List<DeviceType>? Devices = null, string? PaginationToken = null);
public sealed record ListUsersResult(List<UserType>? Users = null, string? PaginationToken = null);
public sealed record ListUsersInGroupResult(List<UserType>? Users = null, string? NextToken = null);
public sealed record SignUpResult(bool UserConfirmed, string? UserSub = null);
public sealed record InitiateAuthResult(
    ChallengeNameType? ChallengeName = null,
    Dictionary<string, string>? ChallengeParameters = null,
    string? Session = null,
    AuthenticationResultType? AuthenticationResult = null);
public sealed record RespondToAuthChallengeResult(
    ChallengeNameType? ChallengeName = null,
    Dictionary<string, string>? ChallengeParameters = null,
    string? Session = null,
    AuthenticationResultType? AuthenticationResult = null);
public sealed record GetUserResult(
    string? Username = null,
    List<AttributeType>? UserAttributes = null,
    List<MFAOptionType>? MFAOptions = null,
    string? PreferredMfaSetting = null,
    List<string>? UserMFASettingList = null);
public sealed record AssociateSoftwareTokenResult(string? SecretCode = null, string? Session = null);
public sealed record VerifySoftwareTokenResult(string? Status = null, string? Session = null);
public sealed record CreateIdentityProviderResult(IdentityProviderType? IdentityProvider = null);
public sealed record DescribeIdentityProviderResult(IdentityProviderType? IdentityProvider = null);
public sealed record ListIdentityProvidersResult(List<ProviderDescription>? Providers = null, string? NextToken = null);
public sealed record UpdateIdentityProviderResult(IdentityProviderType? IdentityProvider = null);

// ── Service ─────────────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Cognito User Pools.
/// </summary>
public static class CognitoService
{
    private static AmazonCognitoIdentityProviderClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCognitoIdentityProviderClient>(region);

    // ── User Pool ───────────────────────────────────────────────────────

    /// <summary>
    /// Create a new Cognito user pool.
    /// </summary>
    public static async Task<CreateUserPoolResult> CreateUserPoolAsync(
        CreateUserPoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserPoolAsync(request);
            return new CreateUserPoolResult(
                UserPoolId: resp.UserPool.Id,
                Arn: resp.UserPool.Arn);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create user pool");
        }
    }

    /// <summary>
    /// Delete a Cognito user pool.
    /// </summary>
    public static async Task DeleteUserPoolAsync(
        string userPoolId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserPoolAsync(new DeleteUserPoolRequest
            {
                UserPoolId = userPoolId
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete user pool '{userPoolId}'");
        }
    }

    /// <summary>
    /// Describe a Cognito user pool.
    /// </summary>
    public static async Task<DescribeUserPoolResult> DescribeUserPoolAsync(
        string userPoolId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeUserPoolAsync(new DescribeUserPoolRequest
            {
                UserPoolId = userPoolId
            });
            return new DescribeUserPoolResult(UserPool: resp.UserPool);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe user pool '{userPoolId}'");
        }
    }

    /// <summary>
    /// List Cognito user pools.
    /// </summary>
    public static async Task<ListUserPoolsResult> ListUserPoolsAsync(
        int maxResults = 60,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUserPoolsRequest { MaxResults = maxResults };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListUserPoolsAsync(request);
            return new ListUserPoolsResult(
                UserPools: resp.UserPools,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list user pools");
        }
    }

    /// <summary>
    /// Update a Cognito user pool.
    /// </summary>
    public static async Task UpdateUserPoolAsync(
        UpdateUserPoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateUserPoolAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update user pool");
        }
    }

    // ── User Pool Client ────────────────────────────────────────────────

    /// <summary>
    /// Create a user pool client (app client).
    /// </summary>
    public static async Task<CreateUserPoolClientResult> CreateUserPoolClientAsync(
        CreateUserPoolClientRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserPoolClientAsync(request);
            return new CreateUserPoolClientResult(UserPoolClient: resp.UserPoolClient);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create user pool client");
        }
    }

    /// <summary>
    /// Delete a user pool client.
    /// </summary>
    public static async Task DeleteUserPoolClientAsync(
        string userPoolId,
        string clientId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserPoolClientAsync(new DeleteUserPoolClientRequest
            {
                UserPoolId = userPoolId,
                ClientId = clientId
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete user pool client '{clientId}'");
        }
    }

    /// <summary>
    /// Describe a user pool client.
    /// </summary>
    public static async Task<DescribeUserPoolClientResult> DescribeUserPoolClientAsync(
        string userPoolId,
        string clientId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeUserPoolClientAsync(new DescribeUserPoolClientRequest
            {
                UserPoolId = userPoolId,
                ClientId = clientId
            });
            return new DescribeUserPoolClientResult(UserPoolClient: resp.UserPoolClient);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe user pool client '{clientId}'");
        }
    }

    /// <summary>
    /// List user pool clients.
    /// </summary>
    public static async Task<ListUserPoolClientsResult> ListUserPoolClientsAsync(
        string userPoolId,
        int maxResults = 60,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUserPoolClientsRequest
        {
            UserPoolId = userPoolId,
            MaxResults = maxResults
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListUserPoolClientsAsync(request);
            return new ListUserPoolClientsResult(
                UserPoolClients: resp.UserPoolClients,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list user pool clients");
        }
    }

    /// <summary>
    /// Update a user pool client.
    /// </summary>
    public static async Task<DescribeUserPoolClientResult> UpdateUserPoolClientAsync(
        UpdateUserPoolClientRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateUserPoolClientAsync(request);
            return new DescribeUserPoolClientResult(UserPoolClient: resp.UserPoolClient);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update user pool client");
        }
    }

    // ── Admin User Operations ───────────────────────────────────────────

    /// <summary>
    /// Admin-create a user in a user pool.
    /// </summary>
    public static async Task<AdminCreateUserResult> AdminCreateUserAsync(
        AdminCreateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AdminCreateUserAsync(request);
            return new AdminCreateUserResult(User: resp.User);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to admin-create user");
        }
    }

    /// <summary>
    /// Admin-delete a user from a user pool.
    /// </summary>
    public static async Task AdminDeleteUserAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminDeleteUserAsync(new AdminDeleteUserRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-delete user '{username}'");
        }
    }

    /// <summary>
    /// Admin-get a user from a user pool.
    /// </summary>
    public static async Task<AdminGetUserResult> AdminGetUserAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AdminGetUserAsync(new AdminGetUserRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
            return new AdminGetUserResult(
                Username: resp.Username,
                UserAttributes: resp.UserAttributes,
                UserStatus: resp.UserStatus?.Value,
                Enabled: resp.Enabled,
                UserCreateDate: resp.UserCreateDate,
                UserLastModifiedDate: resp.UserLastModifiedDate);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-get user '{username}'");
        }
    }

    /// <summary>
    /// Admin-set password for a user.
    /// </summary>
    public static async Task AdminSetUserPasswordAsync(
        string userPoolId,
        string username,
        string password,
        bool permanent = true,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminSetUserPasswordAsync(new AdminSetUserPasswordRequest
            {
                UserPoolId = userPoolId,
                Username = username,
                Password = password,
                Permanent = permanent
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-set password for user '{username}'");
        }
    }

    /// <summary>
    /// Admin-confirm sign-up for a user.
    /// </summary>
    public static async Task AdminConfirmSignUpAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminConfirmSignUpAsync(new AdminConfirmSignUpRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-confirm sign-up for user '{username}'");
        }
    }

    /// <summary>
    /// Admin-disable a user.
    /// </summary>
    public static async Task AdminDisableUserAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminDisableUserAsync(new AdminDisableUserRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-disable user '{username}'");
        }
    }

    /// <summary>
    /// Admin-enable a user.
    /// </summary>
    public static async Task AdminEnableUserAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminEnableUserAsync(new AdminEnableUserRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-enable user '{username}'");
        }
    }

    /// <summary>
    /// Admin-initiate authentication.
    /// </summary>
    public static async Task<AdminInitiateAuthResult> AdminInitiateAuthAsync(
        AdminInitiateAuthRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AdminInitiateAuthAsync(request);
            return new AdminInitiateAuthResult(
                ChallengeName: resp.ChallengeName,
                ChallengeParameters: resp.ChallengeParameters,
                Session: resp.Session,
                AuthenticationResult: resp.AuthenticationResult);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to admin-initiate auth");
        }
    }

    /// <summary>
    /// Admin-respond to an authentication challenge.
    /// </summary>
    public static async Task<AdminRespondToAuthChallengeResult> AdminRespondToAuthChallengeAsync(
        AdminRespondToAuthChallengeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AdminRespondToAuthChallengeAsync(request);
            return new AdminRespondToAuthChallengeResult(
                ChallengeName: resp.ChallengeName,
                ChallengeParameters: resp.ChallengeParameters,
                Session: resp.Session,
                AuthenticationResult: resp.AuthenticationResult);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to admin-respond to auth challenge");
        }
    }

    /// <summary>
    /// Admin-global sign-out for a user.
    /// </summary>
    public static async Task AdminUserGlobalSignOutAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminUserGlobalSignOutAsync(new AdminUserGlobalSignOutRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-global sign-out user '{username}'");
        }
    }

    /// <summary>
    /// Admin-list groups for a user.
    /// </summary>
    public static async Task<AdminListGroupsForUserResult> AdminListGroupsForUserAsync(
        string userPoolId,
        string username,
        int? limit = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AdminListGroupsForUserRequest
        {
            UserPoolId = userPoolId,
            Username = username
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.AdminListGroupsForUserAsync(request);
            return new AdminListGroupsForUserResult(
                Groups: resp.Groups,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-list groups for user '{username}'");
        }
    }

    /// <summary>
    /// Admin-add a user to a group.
    /// </summary>
    public static async Task AdminAddUserToGroupAsync(
        string userPoolId,
        string username,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminAddUserToGroupAsync(new AdminAddUserToGroupRequest
            {
                UserPoolId = userPoolId,
                Username = username,
                GroupName = groupName
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add user '{username}' to group '{groupName}'");
        }
    }

    /// <summary>
    /// Admin-remove a user from a group.
    /// </summary>
    public static async Task AdminRemoveUserFromGroupAsync(
        string userPoolId,
        string username,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminRemoveUserFromGroupAsync(new AdminRemoveUserFromGroupRequest
            {
                UserPoolId = userPoolId,
                Username = username,
                GroupName = groupName
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove user '{username}' from group '{groupName}'");
        }
    }

    /// <summary>
    /// Admin-reset user password.
    /// </summary>
    public static async Task AdminResetUserPasswordAsync(
        string userPoolId,
        string username,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminResetUserPasswordAsync(new AdminResetUserPasswordRequest
            {
                UserPoolId = userPoolId,
                Username = username
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-reset password for user '{username}'");
        }
    }

    /// <summary>
    /// Admin-set user MFA preference.
    /// </summary>
    public static async Task AdminSetUserMFAPreferenceAsync(
        AdminSetUserMFAPreferenceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminSetUserMFAPreferenceAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to admin-set user MFA preference");
        }
    }

    /// <summary>
    /// Admin-update user attributes.
    /// </summary>
    public static async Task AdminUpdateUserAttributesAsync(
        string userPoolId,
        string username,
        List<AttributeType> userAttributes,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AdminUpdateUserAttributesAsync(new AdminUpdateUserAttributesRequest
            {
                UserPoolId = userPoolId,
                Username = username,
                UserAttributes = userAttributes
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-update attributes for user '{username}'");
        }
    }

    /// <summary>
    /// Admin-list devices for a user.
    /// </summary>
    public static async Task<AdminListDevicesResult> AdminListDevicesAsync(
        string userPoolId,
        string username,
        int? limit = null,
        string? paginationToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AdminListDevicesRequest
        {
            UserPoolId = userPoolId,
            Username = username
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (paginationToken != null) request.PaginationToken = paginationToken;

        try
        {
            var resp = await client.AdminListDevicesAsync(request);
            return new AdminListDevicesResult(
                Devices: resp.Devices,
                PaginationToken: resp.PaginationToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to admin-list devices for user '{username}'");
        }
    }

    // ── Group Operations ────────────────────────────────────────────────

    /// <summary>
    /// Create a group in a user pool.
    /// </summary>
    public static async Task<CreateGroupResult> CreateGroupAsync(
        CreateGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGroupAsync(request);
            return new CreateGroupResult(Group: resp.Group);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create group");
        }
    }

    /// <summary>
    /// Delete a group from a user pool.
    /// </summary>
    public static async Task DeleteGroupAsync(
        string userPoolId,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteGroupAsync(new DeleteGroupRequest
            {
                UserPoolId = userPoolId,
                GroupName = groupName
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete group '{groupName}'");
        }
    }

    /// <summary>
    /// Get a group from a user pool.
    /// </summary>
    public static async Task<GetGroupResult> GetGroupAsync(
        string userPoolId,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGroupAsync(new GetGroupRequest
            {
                UserPoolId = userPoolId,
                GroupName = groupName
            });
            return new GetGroupResult(Group: resp.Group);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get group '{groupName}'");
        }
    }

    /// <summary>
    /// List groups in a user pool.
    /// </summary>
    public static async Task<ListGroupsResult> ListGroupsAsync(
        string userPoolId,
        int? limit = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupsRequest { UserPoolId = userPoolId };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListGroupsAsync(request);
            return new ListGroupsResult(
                Groups: resp.Groups,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list groups");
        }
    }

    /// <summary>
    /// Update a group in a user pool.
    /// </summary>
    public static async Task<UpdateGroupResult> UpdateGroupAsync(
        UpdateGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateGroupAsync(request);
            return new UpdateGroupResult(Group: resp.Group);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update group");
        }
    }

    // ── User Listing ────────────────────────────────────────────────────

    /// <summary>
    /// List users in a user pool.
    /// </summary>
    public static async Task<ListUsersResult> ListUsersAsync(
        string userPoolId,
        string? filter = null,
        int? limit = null,
        string? paginationToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsersRequest { UserPoolId = userPoolId };
        if (filter != null) request.Filter = filter;
        if (limit.HasValue) request.Limit = limit.Value;
        if (paginationToken != null) request.PaginationToken = paginationToken;

        try
        {
            var resp = await client.ListUsersAsync(request);
            return new ListUsersResult(
                Users: resp.Users,
                PaginationToken: resp.PaginationToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list users");
        }
    }

    /// <summary>
    /// List users in a group.
    /// </summary>
    public static async Task<ListUsersInGroupResult> ListUsersInGroupAsync(
        string userPoolId,
        string groupName,
        int? limit = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsersInGroupRequest
        {
            UserPoolId = userPoolId,
            GroupName = groupName
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListUsersInGroupAsync(request);
            return new ListUsersInGroupResult(
                Users: resp.Users,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list users in group '{groupName}'");
        }
    }

    // ── Client-Side Auth ────────────────────────────────────────────────

    /// <summary>
    /// Sign up a new user.
    /// </summary>
    public static async Task<SignUpResult> SignUpAsync(
        SignUpRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SignUpAsync(request);
            return new SignUpResult(
                UserConfirmed: resp.UserConfirmed ?? false,
                UserSub: resp.UserSub);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to sign up user");
        }
    }

    /// <summary>
    /// Initiate authentication (client-side).
    /// </summary>
    public static async Task<InitiateAuthResult> InitiateAuthAsync(
        InitiateAuthRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.InitiateAuthAsync(request);
            return new InitiateAuthResult(
                ChallengeName: resp.ChallengeName,
                ChallengeParameters: resp.ChallengeParameters,
                Session: resp.Session,
                AuthenticationResult: resp.AuthenticationResult);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to initiate auth");
        }
    }

    /// <summary>
    /// Respond to an authentication challenge (client-side).
    /// </summary>
    public static async Task<RespondToAuthChallengeResult> RespondToAuthChallengeAsync(
        RespondToAuthChallengeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RespondToAuthChallengeAsync(request);
            return new RespondToAuthChallengeResult(
                ChallengeName: resp.ChallengeName,
                ChallengeParameters: resp.ChallengeParameters,
                Session: resp.Session,
                AuthenticationResult: resp.AuthenticationResult);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to respond to auth challenge");
        }
    }

    /// <summary>
    /// Confirm user sign-up with a confirmation code.
    /// </summary>
    public static async Task ConfirmSignUpAsync(
        ConfirmSignUpRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ConfirmSignUpAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to confirm sign-up");
        }
    }

    /// <summary>
    /// Initiate forgot-password flow.
    /// </summary>
    public static async Task ForgotPasswordAsync(
        ForgotPasswordRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ForgotPasswordAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to initiate forgot password");
        }
    }

    /// <summary>
    /// Confirm forgot-password with a confirmation code and new password.
    /// </summary>
    public static async Task ConfirmForgotPasswordAsync(
        ConfirmForgotPasswordRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ConfirmForgotPasswordAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to confirm forgot password");
        }
    }

    /// <summary>
    /// Change the password for an authenticated user.
    /// </summary>
    public static async Task ChangePasswordAsync(
        ChangePasswordRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ChangePasswordAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to change password");
        }
    }

    /// <summary>
    /// Global sign-out for the current user (invalidates all tokens).
    /// </summary>
    public static async Task GlobalSignOutAsync(
        GlobalSignOutRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.GlobalSignOutAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to global sign-out");
        }
    }

    /// <summary>
    /// Get the current authenticated user's information.
    /// </summary>
    public static async Task<GetUserResult> GetUserAsync(
        GetUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetUserAsync(request);
            return new GetUserResult(
                Username: resp.Username,
                UserAttributes: resp.UserAttributes,
                MFAOptions: resp.MFAOptions,
                PreferredMfaSetting: resp.PreferredMfaSetting,
                UserMFASettingList: resp.UserMFASettingList);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get user");
        }
    }

    /// <summary>
    /// Set MFA preference for the current user.
    /// </summary>
    public static async Task SetUserMFAPreferenceAsync(
        SetUserMFAPreferenceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetUserMFAPreferenceAsync(request);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to set user MFA preference");
        }
    }

    // ── MFA / Software Token ────────────────────────────────────────────

    /// <summary>
    /// Associate a TOTP software token for MFA.
    /// </summary>
    public static async Task<AssociateSoftwareTokenResult> AssociateSoftwareTokenAsync(
        AssociateSoftwareTokenRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AssociateSoftwareTokenAsync(request);
            return new AssociateSoftwareTokenResult(
                SecretCode: resp.SecretCode,
                Session: resp.Session);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to associate software token");
        }
    }

    /// <summary>
    /// Verify a TOTP software token for MFA.
    /// </summary>
    public static async Task<VerifySoftwareTokenResult> VerifySoftwareTokenAsync(
        VerifySoftwareTokenRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.VerifySoftwareTokenAsync(request);
            return new VerifySoftwareTokenResult(
                Status: resp.Status?.Value,
                Session: resp.Session);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to verify software token");
        }
    }

    // ── Identity Providers ──────────────────────────────────────────────

    /// <summary>
    /// Create an identity provider for a user pool.
    /// </summary>
    public static async Task<CreateIdentityProviderResult> CreateIdentityProviderAsync(
        CreateIdentityProviderRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateIdentityProviderAsync(request);
            return new CreateIdentityProviderResult(IdentityProvider: resp.IdentityProvider);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create identity provider");
        }
    }

    /// <summary>
    /// Delete an identity provider from a user pool.
    /// </summary>
    public static async Task DeleteIdentityProviderAsync(
        string userPoolId,
        string providerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteIdentityProviderAsync(new DeleteIdentityProviderRequest
            {
                UserPoolId = userPoolId,
                ProviderName = providerName
            });
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete identity provider '{providerName}'");
        }
    }

    /// <summary>
    /// Describe an identity provider.
    /// </summary>
    public static async Task<DescribeIdentityProviderResult> DescribeIdentityProviderAsync(
        string userPoolId,
        string providerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeIdentityProviderAsync(new DescribeIdentityProviderRequest
            {
                UserPoolId = userPoolId,
                ProviderName = providerName
            });
            return new DescribeIdentityProviderResult(IdentityProvider: resp.IdentityProvider);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe identity provider '{providerName}'");
        }
    }

    /// <summary>
    /// List identity providers for a user pool.
    /// </summary>
    public static async Task<ListIdentityProvidersResult> ListIdentityProvidersAsync(
        string userPoolId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListIdentityProvidersRequest { UserPoolId = userPoolId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListIdentityProvidersAsync(request);
            return new ListIdentityProvidersResult(
                Providers: resp.Providers,
                NextToken: resp.NextToken);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list identity providers");
        }
    }

    /// <summary>
    /// Update an identity provider for a user pool.
    /// </summary>
    public static async Task<UpdateIdentityProviderResult> UpdateIdentityProviderAsync(
        UpdateIdentityProviderRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateIdentityProviderAsync(request);
            return new UpdateIdentityProviderResult(IdentityProvider: resp.IdentityProvider);
        }
        catch (AmazonCognitoIdentityProviderException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update identity provider");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateUserPoolAsync"/>.</summary>
    public static CreateUserPoolResult CreateUserPool(CreateUserPoolRequest request, RegionEndpoint? region = null)
        => CreateUserPoolAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserPoolAsync"/>.</summary>
    public static void DeleteUserPool(string userPoolId, RegionEndpoint? region = null)
        => DeleteUserPoolAsync(userPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUserPoolAsync"/>.</summary>
    public static DescribeUserPoolResult DescribeUserPool(string userPoolId, RegionEndpoint? region = null)
        => DescribeUserPoolAsync(userPoolId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUserPoolsAsync"/>.</summary>
    public static ListUserPoolsResult ListUserPools(int maxResults = 60, string? nextToken = null, RegionEndpoint? region = null)
        => ListUserPoolsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserPoolAsync"/>.</summary>
    public static void UpdateUserPool(UpdateUserPoolRequest request, RegionEndpoint? region = null)
        => UpdateUserPoolAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserPoolClientAsync"/>.</summary>
    public static CreateUserPoolClientResult CreateUserPoolClient(CreateUserPoolClientRequest request, RegionEndpoint? region = null)
        => CreateUserPoolClientAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserPoolClientAsync"/>.</summary>
    public static void DeleteUserPoolClient(string userPoolId, string clientId, RegionEndpoint? region = null)
        => DeleteUserPoolClientAsync(userPoolId, clientId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUserPoolClientAsync"/>.</summary>
    public static DescribeUserPoolClientResult DescribeUserPoolClient(string userPoolId, string clientId, RegionEndpoint? region = null)
        => DescribeUserPoolClientAsync(userPoolId, clientId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUserPoolClientsAsync"/>.</summary>
    public static ListUserPoolClientsResult ListUserPoolClients(string userPoolId, int maxResults = 60, string? nextToken = null, RegionEndpoint? region = null)
        => ListUserPoolClientsAsync(userPoolId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserPoolClientAsync"/>.</summary>
    public static DescribeUserPoolClientResult UpdateUserPoolClient(UpdateUserPoolClientRequest request, RegionEndpoint? region = null)
        => UpdateUserPoolClientAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminCreateUserAsync"/>.</summary>
    public static AdminCreateUserResult AdminCreateUser(AdminCreateUserRequest request, RegionEndpoint? region = null)
        => AdminCreateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminDeleteUserAsync"/>.</summary>
    public static void AdminDeleteUser(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminDeleteUserAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminGetUserAsync"/>.</summary>
    public static AdminGetUserResult AdminGetUser(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminGetUserAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminSetUserPasswordAsync"/>.</summary>
    public static void AdminSetUserPassword(string userPoolId, string username, string password, bool permanent = true, RegionEndpoint? region = null)
        => AdminSetUserPasswordAsync(userPoolId, username, password, permanent, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminConfirmSignUpAsync"/>.</summary>
    public static void AdminConfirmSignUp(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminConfirmSignUpAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminDisableUserAsync"/>.</summary>
    public static void AdminDisableUser(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminDisableUserAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminEnableUserAsync"/>.</summary>
    public static void AdminEnableUser(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminEnableUserAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminInitiateAuthAsync"/>.</summary>
    public static AdminInitiateAuthResult AdminInitiateAuth(AdminInitiateAuthRequest request, RegionEndpoint? region = null)
        => AdminInitiateAuthAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminRespondToAuthChallengeAsync"/>.</summary>
    public static AdminRespondToAuthChallengeResult AdminRespondToAuthChallenge(AdminRespondToAuthChallengeRequest request, RegionEndpoint? region = null)
        => AdminRespondToAuthChallengeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminUserGlobalSignOutAsync"/>.</summary>
    public static void AdminUserGlobalSignOut(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminUserGlobalSignOutAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminListGroupsForUserAsync"/>.</summary>
    public static AdminListGroupsForUserResult AdminListGroupsForUser(string userPoolId, string username, int? limit = null, string? nextToken = null, RegionEndpoint? region = null)
        => AdminListGroupsForUserAsync(userPoolId, username, limit, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminAddUserToGroupAsync"/>.</summary>
    public static void AdminAddUserToGroup(string userPoolId, string username, string groupName, RegionEndpoint? region = null)
        => AdminAddUserToGroupAsync(userPoolId, username, groupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminRemoveUserFromGroupAsync"/>.</summary>
    public static void AdminRemoveUserFromGroup(string userPoolId, string username, string groupName, RegionEndpoint? region = null)
        => AdminRemoveUserFromGroupAsync(userPoolId, username, groupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminResetUserPasswordAsync"/>.</summary>
    public static void AdminResetUserPassword(string userPoolId, string username, RegionEndpoint? region = null)
        => AdminResetUserPasswordAsync(userPoolId, username, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminSetUserMFAPreferenceAsync"/>.</summary>
    public static void AdminSetUserMFAPreference(AdminSetUserMFAPreferenceRequest request, RegionEndpoint? region = null)
        => AdminSetUserMFAPreferenceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminUpdateUserAttributesAsync"/>.</summary>
    public static void AdminUpdateUserAttributes(string userPoolId, string username, List<AttributeType> userAttributes, RegionEndpoint? region = null)
        => AdminUpdateUserAttributesAsync(userPoolId, username, userAttributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AdminListDevicesAsync"/>.</summary>
    public static AdminListDevicesResult AdminListDevices(string userPoolId, string username, int? limit = null, string? paginationToken = null, RegionEndpoint? region = null)
        => AdminListDevicesAsync(userPoolId, username, limit, paginationToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateGroupAsync"/>.</summary>
    public static CreateGroupResult CreateGroup(CreateGroupRequest request, RegionEndpoint? region = null)
        => CreateGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGroupAsync"/>.</summary>
    public static void DeleteGroup(string userPoolId, string groupName, RegionEndpoint? region = null)
        => DeleteGroupAsync(userPoolId, groupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetGroupAsync"/>.</summary>
    public static GetGroupResult GetGroup(string userPoolId, string groupName, RegionEndpoint? region = null)
        => GetGroupAsync(userPoolId, groupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGroupsAsync"/>.</summary>
    public static ListGroupsResult ListGroups(string userPoolId, int? limit = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListGroupsAsync(userPoolId, limit, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateGroupAsync"/>.</summary>
    public static UpdateGroupResult UpdateGroup(UpdateGroupRequest request, RegionEndpoint? region = null)
        => UpdateGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUsersAsync"/>.</summary>
    public static ListUsersResult ListUsers(string userPoolId, string? filter = null, int? limit = null, string? paginationToken = null, RegionEndpoint? region = null)
        => ListUsersAsync(userPoolId, filter, limit, paginationToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUsersInGroupAsync"/>.</summary>
    public static ListUsersInGroupResult ListUsersInGroup(string userPoolId, string groupName, int? limit = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListUsersInGroupAsync(userPoolId, groupName, limit, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SignUpAsync"/>.</summary>
    public static SignUpResult SignUp(SignUpRequest request, RegionEndpoint? region = null)
        => SignUpAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="InitiateAuthAsync"/>.</summary>
    public static InitiateAuthResult InitiateAuth(InitiateAuthRequest request, RegionEndpoint? region = null)
        => InitiateAuthAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RespondToAuthChallengeAsync"/>.</summary>
    public static RespondToAuthChallengeResult RespondToAuthChallenge(RespondToAuthChallengeRequest request, RegionEndpoint? region = null)
        => RespondToAuthChallengeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ConfirmSignUpAsync"/>.</summary>
    public static void ConfirmSignUp(ConfirmSignUpRequest request, RegionEndpoint? region = null)
        => ConfirmSignUpAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ForgotPasswordAsync"/>.</summary>
    public static void ForgotPassword(ForgotPasswordRequest request, RegionEndpoint? region = null)
        => ForgotPasswordAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ConfirmForgotPasswordAsync"/>.</summary>
    public static void ConfirmForgotPassword(ConfirmForgotPasswordRequest request, RegionEndpoint? region = null)
        => ConfirmForgotPasswordAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ChangePasswordAsync"/>.</summary>
    public static void ChangePassword(ChangePasswordRequest request, RegionEndpoint? region = null)
        => ChangePasswordAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GlobalSignOutAsync"/>.</summary>
    public static void GlobalSignOut(GlobalSignOutRequest request, RegionEndpoint? region = null)
        => GlobalSignOutAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetUserAsync"/>.</summary>
    public static GetUserResult GetUser(GetUserRequest request, RegionEndpoint? region = null)
        => GetUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetUserMFAPreferenceAsync"/>.</summary>
    public static void SetUserMFAPreference(SetUserMFAPreferenceRequest request, RegionEndpoint? region = null)
        => SetUserMFAPreferenceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateSoftwareTokenAsync"/>.</summary>
    public static AssociateSoftwareTokenResult AssociateSoftwareToken(AssociateSoftwareTokenRequest request, RegionEndpoint? region = null)
        => AssociateSoftwareTokenAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="VerifySoftwareTokenAsync"/>.</summary>
    public static VerifySoftwareTokenResult VerifySoftwareToken(VerifySoftwareTokenRequest request, RegionEndpoint? region = null)
        => VerifySoftwareTokenAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateIdentityProviderAsync"/>.</summary>
    public static CreateIdentityProviderResult CreateIdentityProvider(CreateIdentityProviderRequest request, RegionEndpoint? region = null)
        => CreateIdentityProviderAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteIdentityProviderAsync"/>.</summary>
    public static void DeleteIdentityProvider(string userPoolId, string providerName, RegionEndpoint? region = null)
        => DeleteIdentityProviderAsync(userPoolId, providerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeIdentityProviderAsync"/>.</summary>
    public static DescribeIdentityProviderResult DescribeIdentityProvider(string userPoolId, string providerName, RegionEndpoint? region = null)
        => DescribeIdentityProviderAsync(userPoolId, providerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListIdentityProvidersAsync"/>.</summary>
    public static ListIdentityProvidersResult ListIdentityProviders(string userPoolId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListIdentityProvidersAsync(userPoolId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateIdentityProviderAsync"/>.</summary>
    public static UpdateIdentityProviderResult UpdateIdentityProvider(UpdateIdentityProviderRequest request, RegionEndpoint? region = null)
        => UpdateIdentityProviderAsync(request, region).GetAwaiter().GetResult();

}
