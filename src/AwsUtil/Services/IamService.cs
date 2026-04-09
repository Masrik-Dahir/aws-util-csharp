using Amazon;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record IamUserResult(
    string? UserName = null, string? UserId = null,
    string? Arn = null, string? CreateDate = null,
    string? Path = null);

public sealed record IamRoleResult(
    string? RoleName = null, string? RoleId = null,
    string? Arn = null, string? CreateDate = null,
    string? Path = null, string? AssumeRolePolicyDocument = null);

public sealed record IamPolicyResult(
    string? PolicyName = null, string? PolicyId = null,
    string? Arn = null, string? Path = null,
    string? Description = null, string? CreateDate = null,
    int? AttachmentCount = null);

public sealed record IamGroupResult(
    string? GroupName = null, string? GroupId = null,
    string? Arn = null, string? CreateDate = null,
    string? Path = null);

public sealed record IamAccessKeyResult(
    string? AccessKeyId = null, string? UserName = null,
    string? Status = null, string? CreateDate = null,
    string? SecretAccessKey = null);

public sealed record IamInstanceProfileResult(
    string? InstanceProfileName = null, string? InstanceProfileId = null,
    string? Arn = null, string? CreateDate = null,
    List<string>? RoleNames = null);

public sealed record IamAttachedPolicyResult(
    string? PolicyName = null, string? PolicyArn = null);

public sealed record IamRolePolicyResult(
    string? RoleName = null, string? PolicyName = null,
    string? PolicyDocument = null);

public sealed record IamAccountSummaryResult(
    Dictionary<string, int>? SummaryMap = null);

public sealed record IamCredentialReportResult(
    string? Content = null, string? GeneratedTime = null,
    string? ReportFormat = null);

public sealed record IamSimulatePolicyResult(
    List<Dictionary<string, string>>? EvaluationResults = null);

public sealed record IamLoginProfileResult(
    string? UserName = null, string? CreateDate = null,
    bool? PasswordResetRequired = null);

public sealed record IamMfaDeviceResult(
    string? UserName = null, string? SerialNumber = null,
    string? EnableDate = null);

public sealed record IamTagResult(string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for AWS Identity and Access Management (IAM).
/// </summary>
public static class IamService
{
    private static AmazonIdentityManagementServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIdentityManagementServiceClient>(region);

    // ── Users ────────────────────────────────────────────────────────

    /// <summary>
    /// Create an IAM user.
    /// </summary>
    public static async Task<IamUserResult> CreateUserAsync(
        string userName,
        string? path = null,
        List<Amazon.IdentityManagement.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateUserRequest { UserName = userName };
        if (path != null) request.Path = path;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateUserAsync(request);
            return new IamUserResult(
                UserName: resp.User.UserName,
                UserId: resp.User.UserId,
                Arn: resp.User.Arn,
                CreateDate: resp.User.CreateDate.ToString(),
                Path: resp.User.Path);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create user '{userName}'");
        }
    }

    /// <summary>
    /// Delete an IAM user.
    /// </summary>
    public static async Task DeleteUserAsync(
        string userName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserAsync(
                new DeleteUserRequest { UserName = userName });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete user '{userName}'");
        }
    }

    /// <summary>
    /// List IAM users.
    /// </summary>
    public static async Task<List<IamUserResult>> ListUsersAsync(
        string? pathPrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsersRequest();
        if (pathPrefix != null) request.PathPrefix = pathPrefix;

        try
        {
            var results = new List<IamUserResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListUsersAsync(request);
                results.AddRange(resp.Users.Select(u => new IamUserResult(
                    UserName: u.UserName,
                    UserId: u.UserId,
                    Arn: u.Arn,
                    CreateDate: u.CreateDate.ToString(),
                    Path: u.Path)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list users");
        }
    }

    /// <summary>
    /// Get details of an IAM user.
    /// </summary>
    public static async Task<IamUserResult> GetUserAsync(
        string? userName = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetUserRequest();
        if (userName != null) request.UserName = userName;

        try
        {
            var resp = await client.GetUserAsync(request);
            return new IamUserResult(
                UserName: resp.User.UserName,
                UserId: resp.User.UserId,
                Arn: resp.User.Arn,
                CreateDate: resp.User.CreateDate.ToString(),
                Path: resp.User.Path);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get user '{userName ?? "(current)"}'");
        }
    }

    // ── Roles ────────────────────────────────────────────────────────

    /// <summary>
    /// Create an IAM role.
    /// </summary>
    public static async Task<IamRoleResult> CreateRoleAsync(
        string roleName,
        string assumeRolePolicyDocument,
        string? description = null,
        string? path = null,
        int? maxSessionDuration = null,
        List<Amazon.IdentityManagement.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateRoleRequest
        {
            RoleName = roleName,
            AssumeRolePolicyDocument = assumeRolePolicyDocument
        };
        if (description != null) request.Description = description;
        if (path != null) request.Path = path;
        if (maxSessionDuration.HasValue) request.MaxSessionDuration = maxSessionDuration.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateRoleAsync(request);
            return new IamRoleResult(
                RoleName: resp.Role.RoleName,
                RoleId: resp.Role.RoleId,
                Arn: resp.Role.Arn,
                CreateDate: resp.Role.CreateDate.ToString(),
                Path: resp.Role.Path,
                AssumeRolePolicyDocument: resp.Role.AssumeRolePolicyDocument);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create role '{roleName}'");
        }
    }

    /// <summary>
    /// Delete an IAM role.
    /// </summary>
    public static async Task DeleteRoleAsync(
        string roleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRoleAsync(
                new DeleteRoleRequest { RoleName = roleName });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete role '{roleName}'");
        }
    }

    /// <summary>
    /// List IAM roles.
    /// </summary>
    public static async Task<List<IamRoleResult>> ListRolesAsync(
        string? pathPrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRolesRequest();
        if (pathPrefix != null) request.PathPrefix = pathPrefix;

        try
        {
            var results = new List<IamRoleResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListRolesAsync(request);
                results.AddRange(resp.Roles.Select(r => new IamRoleResult(
                    RoleName: r.RoleName,
                    RoleId: r.RoleId,
                    Arn: r.Arn,
                    CreateDate: r.CreateDate.ToString(),
                    Path: r.Path)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list roles");
        }
    }

    /// <summary>
    /// Get details of an IAM role.
    /// </summary>
    public static async Task<IamRoleResult> GetRoleAsync(
        string roleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRoleAsync(
                new GetRoleRequest { RoleName = roleName });
            return new IamRoleResult(
                RoleName: resp.Role.RoleName,
                RoleId: resp.Role.RoleId,
                Arn: resp.Role.Arn,
                CreateDate: resp.Role.CreateDate.ToString(),
                Path: resp.Role.Path,
                AssumeRolePolicyDocument: resp.Role.AssumeRolePolicyDocument);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get role '{roleName}'");
        }
    }

    // ── Role Policy Attachments ──────────────────────────────────────

    /// <summary>
    /// Attach a managed policy to a role.
    /// </summary>
    public static async Task AttachRolePolicyAsync(
        string roleName,
        string policyArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachRolePolicyAsync(new AttachRolePolicyRequest
            {
                RoleName = roleName,
                PolicyArn = policyArn
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach policy '{policyArn}' to role '{roleName}'");
        }
    }

    /// <summary>
    /// Detach a managed policy from a role.
    /// </summary>
    public static async Task DetachRolePolicyAsync(
        string roleName,
        string policyArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachRolePolicyAsync(new DetachRolePolicyRequest
            {
                RoleName = roleName,
                PolicyArn = policyArn
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach policy '{policyArn}' from role '{roleName}'");
        }
    }

    /// <summary>
    /// List managed policies attached to a role.
    /// </summary>
    public static async Task<List<IamAttachedPolicyResult>> ListAttachedRolePoliciesAsync(
        string roleName,
        string? pathPrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAttachedRolePoliciesRequest { RoleName = roleName };
        if (pathPrefix != null) request.PathPrefix = pathPrefix;

        try
        {
            var results = new List<IamAttachedPolicyResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListAttachedRolePoliciesAsync(request);
                results.AddRange(resp.AttachedPolicies.Select(p =>
                    new IamAttachedPolicyResult(
                        PolicyName: p.PolicyName,
                        PolicyArn: p.PolicyArn)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list attached role policies for '{roleName}'");
        }
    }

    // ── Policies ─────────────────────────────────────────────────────

    /// <summary>
    /// Create a managed IAM policy.
    /// </summary>
    public static async Task<IamPolicyResult> CreatePolicyAsync(
        string policyName,
        string policyDocument,
        string? description = null,
        string? path = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePolicyRequest
        {
            PolicyName = policyName,
            PolicyDocument = policyDocument
        };
        if (description != null) request.Description = description;
        if (path != null) request.Path = path;

        try
        {
            var resp = await client.CreatePolicyAsync(request);
            return new IamPolicyResult(
                PolicyName: resp.Policy.PolicyName,
                PolicyId: resp.Policy.PolicyId,
                Arn: resp.Policy.Arn,
                Path: resp.Policy.Path,
                Description: resp.Policy.Description,
                CreateDate: resp.Policy.CreateDate.ToString(),
                AttachmentCount: resp.Policy.AttachmentCount);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create policy '{policyName}'");
        }
    }

    /// <summary>
    /// Delete a managed IAM policy.
    /// </summary>
    public static async Task DeletePolicyAsync(
        string policyArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePolicyAsync(
                new DeletePolicyRequest { PolicyArn = policyArn });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete policy '{policyArn}'");
        }
    }

    /// <summary>
    /// Get details of a managed IAM policy.
    /// </summary>
    public static async Task<IamPolicyResult> GetPolicyAsync(
        string policyArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPolicyAsync(
                new GetPolicyRequest { PolicyArn = policyArn });
            return new IamPolicyResult(
                PolicyName: resp.Policy.PolicyName,
                PolicyId: resp.Policy.PolicyId,
                Arn: resp.Policy.Arn,
                Path: resp.Policy.Path,
                Description: resp.Policy.Description,
                CreateDate: resp.Policy.CreateDate.ToString(),
                AttachmentCount: resp.Policy.AttachmentCount);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get policy '{policyArn}'");
        }
    }

    /// <summary>
    /// List managed IAM policies.
    /// </summary>
    public static async Task<List<IamPolicyResult>> ListPoliciesAsync(
        string? scope = null,
        string? pathPrefix = null,
        bool? onlyAttached = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPoliciesRequest();
        if (scope != null) request.Scope = scope;
        if (pathPrefix != null) request.PathPrefix = pathPrefix;
        if (onlyAttached.HasValue) request.OnlyAttached = onlyAttached.Value;

        try
        {
            var results = new List<IamPolicyResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListPoliciesAsync(request);
                results.AddRange(resp.Policies.Select(p => new IamPolicyResult(
                    PolicyName: p.PolicyName,
                    PolicyId: p.PolicyId,
                    Arn: p.Arn,
                    Path: p.Path,
                    Description: p.Description,
                    CreateDate: p.CreateDate.ToString(),
                    AttachmentCount: p.AttachmentCount)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list policies");
        }
    }

    // ── Groups ───────────────────────────────────────────────────────

    /// <summary>
    /// Create an IAM group.
    /// </summary>
    public static async Task<IamGroupResult> CreateGroupAsync(
        string groupName,
        string? path = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGroupRequest { GroupName = groupName };
        if (path != null) request.Path = path;

        try
        {
            var resp = await client.CreateGroupAsync(request);
            return new IamGroupResult(
                GroupName: resp.Group.GroupName,
                GroupId: resp.Group.GroupId,
                Arn: resp.Group.Arn,
                CreateDate: resp.Group.CreateDate.ToString(),
                Path: resp.Group.Path);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create group '{groupName}'");
        }
    }

    /// <summary>
    /// Delete an IAM group.
    /// </summary>
    public static async Task DeleteGroupAsync(
        string groupName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteGroupAsync(
                new DeleteGroupRequest { GroupName = groupName });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete group '{groupName}'");
        }
    }

    /// <summary>
    /// List IAM groups.
    /// </summary>
    public static async Task<List<IamGroupResult>> ListGroupsAsync(
        string? pathPrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupsRequest();
        if (pathPrefix != null) request.PathPrefix = pathPrefix;

        try
        {
            var results = new List<IamGroupResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListGroupsAsync(request);
                results.AddRange(resp.Groups.Select(g => new IamGroupResult(
                    GroupName: g.GroupName,
                    GroupId: g.GroupId,
                    Arn: g.Arn,
                    CreateDate: g.CreateDate.ToString(),
                    Path: g.Path)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list groups");
        }
    }

    /// <summary>
    /// Add a user to a group.
    /// </summary>
    public static async Task AddUserToGroupAsync(
        string groupName,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddUserToGroupAsync(new AddUserToGroupRequest
            {
                GroupName = groupName,
                UserName = userName
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add user '{userName}' to group '{groupName}'");
        }
    }

    /// <summary>
    /// Remove a user from a group.
    /// </summary>
    public static async Task RemoveUserFromGroupAsync(
        string groupName,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveUserFromGroupAsync(new RemoveUserFromGroupRequest
            {
                GroupName = groupName,
                UserName = userName
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove user '{userName}' from group '{groupName}'");
        }
    }

    // ── Access Keys ──────────────────────────────────────────────────

    /// <summary>
    /// Create an access key for a user.
    /// </summary>
    public static async Task<IamAccessKeyResult> CreateAccessKeyAsync(
        string? userName = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAccessKeyRequest();
        if (userName != null) request.UserName = userName;

        try
        {
            var resp = await client.CreateAccessKeyAsync(request);
            return new IamAccessKeyResult(
                AccessKeyId: resp.AccessKey.AccessKeyId,
                UserName: resp.AccessKey.UserName,
                Status: resp.AccessKey.Status?.Value,
                CreateDate: resp.AccessKey.CreateDate.ToString(),
                SecretAccessKey: resp.AccessKey.SecretAccessKey);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create access key");
        }
    }

    /// <summary>
    /// Delete an access key.
    /// </summary>
    public static async Task DeleteAccessKeyAsync(
        string accessKeyId,
        string? userName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAccessKeyRequest { AccessKeyId = accessKeyId };
        if (userName != null) request.UserName = userName;

        try
        {
            await client.DeleteAccessKeyAsync(request);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete access key '{accessKeyId}'");
        }
    }

    /// <summary>
    /// List access keys for a user.
    /// </summary>
    public static async Task<List<IamAccessKeyResult>> ListAccessKeysAsync(
        string? userName = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAccessKeysRequest();
        if (userName != null) request.UserName = userName;

        try
        {
            var results = new List<IamAccessKeyResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListAccessKeysAsync(request);
                results.AddRange(resp.AccessKeyMetadata.Select(ak =>
                    new IamAccessKeyResult(
                        AccessKeyId: ak.AccessKeyId,
                        UserName: ak.UserName,
                        Status: ak.Status?.Value,
                        CreateDate: ak.CreateDate.ToString())));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list access keys");
        }
    }

    /// <summary>
    /// Update the status of an access key.
    /// </summary>
    public static async Task UpdateAccessKeyAsync(
        string accessKeyId,
        string status,
        string? userName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAccessKeyRequest
        {
            AccessKeyId = accessKeyId,
            Status = status
        };
        if (userName != null) request.UserName = userName;

        try
        {
            await client.UpdateAccessKeyAsync(request);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update access key '{accessKeyId}'");
        }
    }

    // ── Instance Profiles ────────────────────────────────────────────

    /// <summary>
    /// Create an instance profile.
    /// </summary>
    public static async Task<IamInstanceProfileResult> CreateInstanceProfileAsync(
        string instanceProfileName,
        string? path = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateInstanceProfileRequest
        {
            InstanceProfileName = instanceProfileName
        };
        if (path != null) request.Path = path;

        try
        {
            var resp = await client.CreateInstanceProfileAsync(request);
            return new IamInstanceProfileResult(
                InstanceProfileName: resp.InstanceProfile.InstanceProfileName,
                InstanceProfileId: resp.InstanceProfile.InstanceProfileId,
                Arn: resp.InstanceProfile.Arn,
                CreateDate: resp.InstanceProfile.CreateDate.ToString(),
                RoleNames: resp.InstanceProfile.Roles?
                    .Select(r => r.RoleName).ToList());
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create instance profile '{instanceProfileName}'");
        }
    }

    /// <summary>
    /// Delete an instance profile.
    /// </summary>
    public static async Task DeleteInstanceProfileAsync(
        string instanceProfileName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteInstanceProfileAsync(
                new DeleteInstanceProfileRequest
                {
                    InstanceProfileName = instanceProfileName
                });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete instance profile '{instanceProfileName}'");
        }
    }

    /// <summary>
    /// Add a role to an instance profile.
    /// </summary>
    public static async Task AddRoleToInstanceProfileAsync(
        string instanceProfileName,
        string roleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddRoleToInstanceProfileAsync(
                new AddRoleToInstanceProfileRequest
                {
                    InstanceProfileName = instanceProfileName,
                    RoleName = roleName
                });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add role '{roleName}' to instance profile '{instanceProfileName}'");
        }
    }

    /// <summary>
    /// Remove a role from an instance profile.
    /// </summary>
    public static async Task RemoveRoleFromInstanceProfileAsync(
        string instanceProfileName,
        string roleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveRoleFromInstanceProfileAsync(
                new RemoveRoleFromInstanceProfileRequest
                {
                    InstanceProfileName = instanceProfileName,
                    RoleName = roleName
                });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove role '{roleName}' from instance profile '{instanceProfileName}'");
        }
    }

    /// <summary>
    /// List instance profiles.
    /// </summary>
    public static async Task<List<IamInstanceProfileResult>> ListInstanceProfilesAsync(
        string? pathPrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListInstanceProfilesRequest();
        if (pathPrefix != null) request.PathPrefix = pathPrefix;

        try
        {
            var results = new List<IamInstanceProfileResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListInstanceProfilesAsync(request);
                results.AddRange(resp.InstanceProfiles.Select(ip =>
                    new IamInstanceProfileResult(
                        InstanceProfileName: ip.InstanceProfileName,
                        InstanceProfileId: ip.InstanceProfileId,
                        Arn: ip.Arn,
                        CreateDate: ip.CreateDate.ToString(),
                        RoleNames: ip.Roles?.Select(r => r.RoleName).ToList())));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list instance profiles");
        }
    }

    // ── Inline Role Policies ─────────────────────────────────────────

    /// <summary>
    /// Put an inline policy on a role.
    /// </summary>
    public static async Task PutRolePolicyAsync(
        string roleName,
        string policyName,
        string policyDocument,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutRolePolicyAsync(new PutRolePolicyRequest
            {
                RoleName = roleName,
                PolicyName = policyName,
                PolicyDocument = policyDocument
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put inline policy '{policyName}' on role '{roleName}'");
        }
    }

    /// <summary>
    /// Delete an inline policy from a role.
    /// </summary>
    public static async Task DeleteRolePolicyAsync(
        string roleName,
        string policyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRolePolicyAsync(new DeleteRolePolicyRequest
            {
                RoleName = roleName,
                PolicyName = policyName
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete inline policy '{policyName}' from role '{roleName}'");
        }
    }

    /// <summary>
    /// Get an inline policy document from a role.
    /// </summary>
    public static async Task<IamRolePolicyResult> GetRolePolicyAsync(
        string roleName,
        string policyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRolePolicyAsync(new GetRolePolicyRequest
            {
                RoleName = roleName,
                PolicyName = policyName
            });
            return new IamRolePolicyResult(
                RoleName: resp.RoleName,
                PolicyName: resp.PolicyName,
                PolicyDocument: resp.PolicyDocument);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get inline policy '{policyName}' from role '{roleName}'");
        }
    }

    /// <summary>
    /// List inline policy names on a role.
    /// </summary>
    public static async Task<List<string>> ListRolePoliciesAsync(
        string roleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRolePoliciesRequest { RoleName = roleName };

        try
        {
            var results = new List<string>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListRolePoliciesAsync(request);
                results.AddRange(resp.PolicyNames);
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list inline policies for role '{roleName}'");
        }
    }

    // ── Inline User Policies ─────────────────────────────────────────

    /// <summary>
    /// Put an inline policy on a user.
    /// </summary>
    public static async Task PutUserPolicyAsync(
        string userName,
        string policyName,
        string policyDocument,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutUserPolicyAsync(new PutUserPolicyRequest
            {
                UserName = userName,
                PolicyName = policyName,
                PolicyDocument = policyDocument
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put inline policy '{policyName}' on user '{userName}'");
        }
    }

    /// <summary>
    /// Delete an inline policy from a user.
    /// </summary>
    public static async Task DeleteUserPolicyAsync(
        string userName,
        string policyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserPolicyAsync(new DeleteUserPolicyRequest
            {
                UserName = userName,
                PolicyName = policyName
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete inline policy '{policyName}' from user '{userName}'");
        }
    }

    // ── Service-Linked Roles ─────────────────────────────────────────

    /// <summary>
    /// Create a service-linked role.
    /// </summary>
    public static async Task<IamRoleResult> CreateServiceLinkedRoleAsync(
        string awsServiceName,
        string? description = null,
        string? customSuffix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateServiceLinkedRoleRequest
        {
            AWSServiceName = awsServiceName
        };
        if (description != null) request.Description = description;
        if (customSuffix != null) request.CustomSuffix = customSuffix;

        try
        {
            var resp = await client.CreateServiceLinkedRoleAsync(request);
            return new IamRoleResult(
                RoleName: resp.Role.RoleName,
                RoleId: resp.Role.RoleId,
                Arn: resp.Role.Arn,
                CreateDate: resp.Role.CreateDate.ToString(),
                Path: resp.Role.Path);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create service-linked role for '{awsServiceName}'");
        }
    }

    /// <summary>
    /// Update the assume role policy document for a role.
    /// </summary>
    public static async Task UpdateAssumeRolePolicyAsync(
        string roleName,
        string policyDocument,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateAssumeRolePolicyAsync(
                new UpdateAssumeRolePolicyRequest
                {
                    RoleName = roleName,
                    PolicyDocument = policyDocument
                });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update assume role policy for '{roleName}'");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Tag an IAM role.
    /// </summary>
    public static async Task TagRoleAsync(
        string roleName,
        List<Amazon.IdentityManagement.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagRoleAsync(new TagRoleRequest
            {
                RoleName = roleName,
                Tags = tags
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag role '{roleName}'");
        }
    }

    /// <summary>
    /// Untag an IAM role.
    /// </summary>
    public static async Task UntagRoleAsync(
        string roleName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagRoleAsync(new UntagRoleRequest
            {
                RoleName = roleName,
                TagKeys = tagKeys
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag role '{roleName}'");
        }
    }

    /// <summary>
    /// Tag an IAM user.
    /// </summary>
    public static async Task TagUserAsync(
        string userName,
        List<Amazon.IdentityManagement.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagUserAsync(new TagUserRequest
            {
                UserName = userName,
                Tags = tags
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag user '{userName}'");
        }
    }

    /// <summary>
    /// Untag an IAM user.
    /// </summary>
    public static async Task UntagUserAsync(
        string userName,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagUserAsync(new UntagUserRequest
            {
                UserName = userName,
                TagKeys = tagKeys
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag user '{userName}'");
        }
    }

    // ── Account & Credential Reports ─────────────────────────────────

    /// <summary>
    /// Get the IAM account summary.
    /// </summary>
    public static async Task<IamAccountSummaryResult> GetAccountSummaryAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccountSummaryAsync(
                new GetAccountSummaryRequest());
            return new IamAccountSummaryResult(
                SummaryMap: resp.SummaryMap);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get account summary");
        }
    }

    /// <summary>
    /// Generate a credential report.
    /// </summary>
    public static async Task<string> GenerateCredentialReportAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GenerateCredentialReportAsync(
                new GenerateCredentialReportRequest());
            return resp.State?.Value ?? "UNKNOWN";
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to generate credential report");
        }
    }

    /// <summary>
    /// Get the credential report.
    /// </summary>
    public static async Task<IamCredentialReportResult> GetCredentialReportAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCredentialReportAsync(
                new GetCredentialReportRequest());
            var content = System.Text.Encoding.UTF8.GetString(
                resp.Content.ToArray());
            return new IamCredentialReportResult(
                Content: content,
                GeneratedTime: resp.GeneratedTime.ToString(),
                ReportFormat: resp.ReportFormat?.Value);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get credential report");
        }
    }

    // ── Policy Simulation ────────────────────────────────────────────

    /// <summary>
    /// Simulate the effect of policies for a principal.
    /// </summary>
    public static async Task<IamSimulatePolicyResult> SimulatePrincipalPolicyAsync(
        string policySourceArn,
        List<string> actionNames,
        List<string>? resourceArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SimulatePrincipalPolicyRequest
        {
            PolicySourceArn = policySourceArn,
            ActionNames = actionNames
        };
        if (resourceArns != null) request.ResourceArns = resourceArns;

        try
        {
            var results = new List<Dictionary<string, string>>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.SimulatePrincipalPolicyAsync(request);
                results.AddRange(resp.EvaluationResults.Select(er =>
                    new Dictionary<string, string>
                    {
                        ["EvalActionName"] = er.EvalActionName,
                        ["EvalDecision"] = er.EvalDecision?.Value ?? "",
                        ["EvalResourceName"] = er.EvalResourceName ?? ""
                    }));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return new IamSimulatePolicyResult(EvaluationResults: results);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to simulate principal policy");
        }
    }

    // ── Login Profiles ───────────────────────────────────────────────

    /// <summary>
    /// Create a login profile (console password) for a user.
    /// </summary>
    public static async Task<IamLoginProfileResult> CreateLoginProfileAsync(
        string userName,
        string password,
        bool passwordResetRequired = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateLoginProfileAsync(
                new CreateLoginProfileRequest
                {
                    UserName = userName,
                    Password = password,
                    PasswordResetRequired = passwordResetRequired
                });
            return new IamLoginProfileResult(
                UserName: resp.LoginProfile.UserName,
                CreateDate: resp.LoginProfile.CreateDate.ToString(),
                PasswordResetRequired: resp.LoginProfile.PasswordResetRequired);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create login profile for '{userName}'");
        }
    }

    /// <summary>
    /// Delete a login profile for a user.
    /// </summary>
    public static async Task DeleteLoginProfileAsync(
        string userName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteLoginProfileAsync(
                new DeleteLoginProfileRequest { UserName = userName });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete login profile for '{userName}'");
        }
    }

    /// <summary>
    /// Update the login profile for a user.
    /// </summary>
    public static async Task UpdateLoginProfileAsync(
        string userName,
        string? password = null,
        bool? passwordResetRequired = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateLoginProfileRequest { UserName = userName };
        if (password != null) request.Password = password;
        if (passwordResetRequired.HasValue)
            request.PasswordResetRequired = passwordResetRequired.Value;

        try
        {
            await client.UpdateLoginProfileAsync(request);
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update login profile for '{userName}'");
        }
    }

    // ── MFA Devices ──────────────────────────────────────────────────

    /// <summary>
    /// List MFA devices for a user.
    /// </summary>
    public static async Task<List<IamMfaDeviceResult>> ListMFADevicesAsync(
        string? userName = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMFADevicesRequest();
        if (userName != null) request.UserName = userName;

        try
        {
            var results = new List<IamMfaDeviceResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListMFADevicesAsync(request);
                results.AddRange(resp.MFADevices.Select(d =>
                    new IamMfaDeviceResult(
                        UserName: d.UserName,
                        SerialNumber: d.SerialNumber,
                        EnableDate: d.EnableDate.ToString())));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MFA devices");
        }
    }

    /// <summary>
    /// Enable an MFA device for a user.
    /// </summary>
    public static async Task EnableMFADeviceAsync(
        string userName,
        string serialNumber,
        string authenticationCode1,
        string authenticationCode2,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableMFADeviceAsync(new EnableMFADeviceRequest
            {
                UserName = userName,
                SerialNumber = serialNumber,
                AuthenticationCode1 = authenticationCode1,
                AuthenticationCode2 = authenticationCode2
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable MFA device for '{userName}'");
        }
    }

    /// <summary>
    /// Deactivate an MFA device for a user.
    /// </summary>
    public static async Task DeactivateMFADeviceAsync(
        string userName,
        string serialNumber,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeactivateMFADeviceAsync(new DeactivateMFADeviceRequest
            {
                UserName = userName,
                SerialNumber = serialNumber
            });
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deactivate MFA device for '{userName}'");
        }
    }

    // ── Tag Listing ──────────────────────────────────────────────────

    /// <summary>
    /// List tags on a user.
    /// </summary>
    public static async Task<List<IamTagResult>> ListUserTagsAsync(
        string userName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUserTagsRequest { UserName = userName };

        try
        {
            var results = new List<IamTagResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListUserTagsAsync(request);
                results.AddRange(resp.Tags.Select(t =>
                    new IamTagResult(Key: t.Key, Value: t.Value)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for user '{userName}'");
        }
    }

    /// <summary>
    /// List tags on a role.
    /// </summary>
    public static async Task<List<IamTagResult>> ListRoleTagsAsync(
        string roleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRoleTagsRequest { RoleName = roleName };

        try
        {
            var results = new List<IamTagResult>();
            string? marker = null;
            do
            {
                request.Marker = marker;
                var resp = await client.ListRoleTagsAsync(request);
                results.AddRange(resp.Tags.Select(t =>
                    new IamTagResult(Key: t.Key, Value: t.Value)));
                marker = (resp.IsTruncated ?? false) ? resp.Marker : null;
            } while (marker != null);

            return results;
        }
        catch (AmazonIdentityManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for role '{roleName}'");
        }
    }
}
