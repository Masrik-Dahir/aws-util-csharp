using Amazon;
using Amazon.SSOAdmin;
using Amazon.SSOAdmin.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record SsoPermissionSetResult(
    string? PermissionSetArn = null, string? Name = null,
    string? Description = null, string? SessionDuration = null,
    string? RelayState = null, string? CreatedDate = null);

public sealed record SsoProvisioningStatusResult(
    string? RequestId = null, string? Status = null,
    string? AccountId = null, string? PermissionSetArn = null,
    string? FailureReason = null, string? CreatedDate = null);

public sealed record SsoManagedPolicyResult(
    string? Name = null, string? Arn = null);

public sealed record SsoInlinePolicyResult(
    string? InlinePolicy = null);

public sealed record SsoCustomerManagedPolicyResult(
    string? Name = null, string? Path = null);

public sealed record SsoAccountAssignmentResult(
    string? AccountId = null, string? PermissionSetArn = null,
    string? PrincipalType = null, string? PrincipalId = null);

public sealed record SsoAccountAssignmentOperationResult(
    string? RequestId = null, string? Status = null,
    string? FailureReason = null, string? TargetId = null,
    string? PermissionSetArn = null, string? PrincipalType = null,
    string? PrincipalId = null, string? CreatedDate = null);

public sealed record SsoInstanceResult(
    string? InstanceArn = null, string? IdentityStoreId = null,
    string? OwnerAccountId = null, string? Name = null,
    string? Status = null);

public sealed record SsoPermissionsBoundaryResult(
    string? ManagedPolicyArn = null,
    string? CustomerManagedPolicyName = null,
    string? CustomerManagedPolicyPath = null);

public sealed record SsoTagResult(
    string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for AWS SSO Admin (IAM Identity Center).
/// </summary>
public static class SsoAdminService
{
    private static AmazonSSOAdminClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSSOAdminClient>(region);

    // ── Permission sets ─────────────────────────────────────────────

    /// <summary>
    /// Create a permission set.
    /// </summary>
    public static async Task<SsoPermissionSetResult> CreatePermissionSetAsync(
        string instanceArn,
        string name,
        string? description = null,
        string? sessionDuration = null,
        string? relayState = null,
        List<Amazon.SSOAdmin.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePermissionSetRequest
        {
            InstanceArn = instanceArn,
            Name = name
        };
        if (description != null) request.Description = description;
        if (sessionDuration != null) request.SessionDuration = sessionDuration;
        if (relayState != null) request.RelayState = relayState;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePermissionSetAsync(request);
            var ps = resp.PermissionSet;
            return new SsoPermissionSetResult(
                PermissionSetArn: ps.PermissionSetArn,
                Name: ps.Name, Description: ps.Description,
                SessionDuration: ps.SessionDuration,
                RelayState: ps.RelayState,
                CreatedDate: ps.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create permission set '{name}'");
        }
    }

    /// <summary>
    /// Delete a permission set.
    /// </summary>
    public static async Task DeletePermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePermissionSetAsync(
                new DeletePermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete permission set '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Describe a permission set.
    /// </summary>
    public static async Task<SsoPermissionSetResult> DescribePermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePermissionSetAsync(
                new DescribePermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
            var ps = resp.PermissionSet;
            return new SsoPermissionSetResult(
                PermissionSetArn: ps.PermissionSetArn,
                Name: ps.Name, Description: ps.Description,
                SessionDuration: ps.SessionDuration,
                RelayState: ps.RelayState,
                CreatedDate: ps.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe permission set '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// List permission sets.
    /// </summary>
    public static async Task<List<string>> ListPermissionSetsAsync(
        string instanceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<string>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListPermissionSetsRequest
                {
                    InstanceArn = instanceArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListPermissionSetsAsync(request);
                results.AddRange(resp.PermissionSets);
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list permission sets");
        }
    }

    /// <summary>
    /// Update a permission set.
    /// </summary>
    public static async Task UpdatePermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string? description = null,
        string? sessionDuration = null,
        string? relayState = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdatePermissionSetRequest
        {
            InstanceArn = instanceArn,
            PermissionSetArn = permissionSetArn
        };
        if (description != null) request.Description = description;
        if (sessionDuration != null) request.SessionDuration = sessionDuration;
        if (relayState != null) request.RelayState = relayState;

        try
        {
            await client.UpdatePermissionSetAsync(request);
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update permission set '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Provision a permission set.
    /// </summary>
    public static async Task<SsoProvisioningStatusResult> ProvisionPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string targetType,
        string? targetId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ProvisionPermissionSetRequest
        {
            InstanceArn = instanceArn,
            PermissionSetArn = permissionSetArn,
            TargetType = new ProvisionTargetType(targetType)
        };
        if (targetId != null) request.TargetId = targetId;

        try
        {
            var resp = await client.ProvisionPermissionSetAsync(request);
            var s = resp.PermissionSetProvisioningStatus;
            return new SsoProvisioningStatusResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                AccountId: s.AccountId,
                PermissionSetArn: s.PermissionSetArn,
                FailureReason: s.FailureReason,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to provision permission set '{permissionSetArn}'");
        }
    }

    // ── Managed policies ────────────────────────────────────────────

    /// <summary>
    /// Attach an AWS managed policy to a permission set.
    /// </summary>
    public static async Task AttachManagedPolicyToPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string managedPolicyArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachManagedPolicyToPermissionSetAsync(
                new AttachManagedPolicyToPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    ManagedPolicyArn = managedPolicyArn
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach managed policy '{managedPolicyArn}'");
        }
    }

    /// <summary>
    /// Detach an AWS managed policy from a permission set.
    /// </summary>
    public static async Task DetachManagedPolicyFromPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string managedPolicyArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachManagedPolicyFromPermissionSetAsync(
                new DetachManagedPolicyFromPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    ManagedPolicyArn = managedPolicyArn
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach managed policy '{managedPolicyArn}'");
        }
    }

    /// <summary>
    /// List AWS managed policies attached to a permission set.
    /// </summary>
    public static async Task<List<SsoManagedPolicyResult>> ListManagedPoliciesInPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoManagedPolicyResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListManagedPoliciesInPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListManagedPoliciesInPermissionSetAsync(request);
                foreach (var p in resp.AttachedManagedPolicies)
                {
                    results.Add(new SsoManagedPolicyResult(
                        Name: p.Name, Arn: p.Arn));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list managed policies for '{permissionSetArn}'");
        }
    }

    // ── Inline policy ───────────────────────────────────────────────

    /// <summary>
    /// Put an inline policy on a permission set.
    /// </summary>
    public static async Task PutInlinePolicyToPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string inlinePolicy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutInlinePolicyToPermissionSetAsync(
                new PutInlinePolicyToPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    InlinePolicy = inlinePolicy
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put inline policy on '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Get the inline policy for a permission set.
    /// </summary>
    public static async Task<SsoInlinePolicyResult> GetInlinePolicyForPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInlinePolicyForPermissionSetAsync(
                new GetInlinePolicyForPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
            return new SsoInlinePolicyResult(InlinePolicy: resp.InlinePolicy);
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get inline policy for '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Delete the inline policy from a permission set.
    /// </summary>
    public static async Task DeleteInlinePolicyFromPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteInlinePolicyFromPermissionSetAsync(
                new DeleteInlinePolicyFromPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete inline policy from '{permissionSetArn}'");
        }
    }

    // ── Customer managed policies ───────────────────────────────────

    /// <summary>
    /// Attach a customer managed policy reference to a permission set.
    /// </summary>
    public static async Task AttachCustomerManagedPolicyReferenceToPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string policyName,
        string? policyPath = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var policyRef = new CustomerManagedPolicyReference { Name = policyName };
        if (policyPath != null) policyRef.Path = policyPath;

        try
        {
            await client.AttachCustomerManagedPolicyReferenceToPermissionSetAsync(
                new AttachCustomerManagedPolicyReferenceToPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    CustomerManagedPolicyReference = policyRef
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach customer managed policy '{policyName}'");
        }
    }

    /// <summary>
    /// Detach a customer managed policy reference from a permission set.
    /// </summary>
    public static async Task DetachCustomerManagedPolicyReferenceFromPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string policyName,
        string? policyPath = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var policyRef = new CustomerManagedPolicyReference { Name = policyName };
        if (policyPath != null) policyRef.Path = policyPath;

        try
        {
            await client.DetachCustomerManagedPolicyReferenceFromPermissionSetAsync(
                new DetachCustomerManagedPolicyReferenceFromPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    CustomerManagedPolicyReference = policyRef
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach customer managed policy '{policyName}'");
        }
    }

    /// <summary>
    /// List customer managed policy references for a permission set.
    /// </summary>
    public static async Task<List<SsoCustomerManagedPolicyResult>> ListCustomerManagedPolicyReferencesInPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoCustomerManagedPolicyResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListCustomerManagedPolicyReferencesInPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListCustomerManagedPolicyReferencesInPermissionSetAsync(request);
                foreach (var p in resp.CustomerManagedPolicyReferences)
                {
                    results.Add(new SsoCustomerManagedPolicyResult(
                        Name: p.Name, Path: p.Path));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list customer managed policies for '{permissionSetArn}'");
        }
    }

    // ── Account assignments ─────────────────────────────────────────

    /// <summary>
    /// Create an account assignment.
    /// </summary>
    public static async Task<SsoAccountAssignmentOperationResult> CreateAccountAssignmentAsync(
        string instanceArn,
        string permissionSetArn,
        string targetId,
        string targetType,
        string principalType,
        string principalId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAccountAssignmentAsync(
                new CreateAccountAssignmentRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    TargetId = targetId,
                    TargetType = new TargetType(targetType),
                    PrincipalType = new PrincipalType(principalType),
                    PrincipalId = principalId
                });
            var s = resp.AccountAssignmentCreationStatus;
            return new SsoAccountAssignmentOperationResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                FailureReason: s.FailureReason,
                TargetId: s.TargetId,
                PermissionSetArn: s.PermissionSetArn,
                PrincipalType: s.PrincipalType?.Value,
                PrincipalId: s.PrincipalId,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create account assignment for '{targetId}'");
        }
    }

    /// <summary>
    /// Delete an account assignment.
    /// </summary>
    public static async Task<SsoAccountAssignmentOperationResult> DeleteAccountAssignmentAsync(
        string instanceArn,
        string permissionSetArn,
        string targetId,
        string targetType,
        string principalType,
        string principalId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteAccountAssignmentAsync(
                new DeleteAccountAssignmentRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    TargetId = targetId,
                    TargetType = new TargetType(targetType),
                    PrincipalType = new PrincipalType(principalType),
                    PrincipalId = principalId
                });
            var s = resp.AccountAssignmentDeletionStatus;
            return new SsoAccountAssignmentOperationResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                FailureReason: s.FailureReason,
                TargetId: s.TargetId,
                PermissionSetArn: s.PermissionSetArn,
                PrincipalType: s.PrincipalType?.Value,
                PrincipalId: s.PrincipalId,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete account assignment for '{targetId}'");
        }
    }

    /// <summary>
    /// List account assignments.
    /// </summary>
    public static async Task<List<SsoAccountAssignmentResult>> ListAccountAssignmentsAsync(
        string instanceArn,
        string accountId,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoAccountAssignmentResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountAssignmentsRequest
                {
                    InstanceArn = instanceArn,
                    AccountId = accountId,
                    PermissionSetArn = permissionSetArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountAssignmentsAsync(request);
                foreach (var a in resp.AccountAssignments)
                {
                    results.Add(new SsoAccountAssignmentResult(
                        AccountId: a.AccountId,
                        PermissionSetArn: a.PermissionSetArn,
                        PrincipalType: a.PrincipalType?.Value,
                        PrincipalId: a.PrincipalId));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list account assignments for '{accountId}'");
        }
    }

    /// <summary>
    /// List accounts for a provisioned permission set.
    /// </summary>
    public static async Task<List<string>> ListAccountsForProvisionedPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<string>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountsForProvisionedPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountsForProvisionedPermissionSetAsync(request);
                results.AddRange(resp.AccountIds);
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list accounts for permission set '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// List permission sets provisioned to an account.
    /// </summary>
    public static async Task<List<string>> ListPermissionSetsProvisionedToAccountAsync(
        string instanceArn,
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<string>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListPermissionSetsProvisionedToAccountRequest
                {
                    InstanceArn = instanceArn,
                    AccountId = accountId
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListPermissionSetsProvisionedToAccountAsync(request);
                results.AddRange(resp.PermissionSets);
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list permission sets for account '{accountId}'");
        }
    }

    // ── Instances ───────────────────────────────────────────────────

    /// <summary>
    /// List SSO instances.
    /// </summary>
    public static async Task<List<SsoInstanceResult>> ListInstancesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoInstanceResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListInstancesRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListInstancesAsync(request);
                foreach (var i in resp.Instances)
                {
                    results.Add(new SsoInstanceResult(
                        InstanceArn: i.InstanceArn,
                        IdentityStoreId: i.IdentityStoreId,
                        OwnerAccountId: i.OwnerAccountId,
                        Name: i.Name,
                        Status: i.Status?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list SSO instances");
        }
    }

    // ── Operation status ────────────────────────────────────────────

    /// <summary>
    /// Describe account assignment creation status.
    /// </summary>
    public static async Task<SsoAccountAssignmentOperationResult> DescribeAccountAssignmentCreationStatusAsync(
        string instanceArn,
        string requestId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccountAssignmentCreationStatusAsync(
                new DescribeAccountAssignmentCreationStatusRequest
                {
                    InstanceArn = instanceArn,
                    AccountAssignmentCreationRequestId = requestId
                });
            var s = resp.AccountAssignmentCreationStatus;
            return new SsoAccountAssignmentOperationResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                FailureReason: s.FailureReason,
                TargetId: s.TargetId,
                PermissionSetArn: s.PermissionSetArn,
                PrincipalType: s.PrincipalType?.Value,
                PrincipalId: s.PrincipalId,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe creation status '{requestId}'");
        }
    }

    /// <summary>
    /// Describe account assignment deletion status.
    /// </summary>
    public static async Task<SsoAccountAssignmentOperationResult> DescribeAccountAssignmentDeletionStatusAsync(
        string instanceArn,
        string requestId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccountAssignmentDeletionStatusAsync(
                new DescribeAccountAssignmentDeletionStatusRequest
                {
                    InstanceArn = instanceArn,
                    AccountAssignmentDeletionRequestId = requestId
                });
            var s = resp.AccountAssignmentDeletionStatus;
            return new SsoAccountAssignmentOperationResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                FailureReason: s.FailureReason,
                TargetId: s.TargetId,
                PermissionSetArn: s.PermissionSetArn,
                PrincipalType: s.PrincipalType?.Value,
                PrincipalId: s.PrincipalId,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe deletion status '{requestId}'");
        }
    }

    /// <summary>
    /// List account assignment creation statuses.
    /// </summary>
    public static async Task<List<SsoAccountAssignmentOperationResult>> ListAccountAssignmentCreationStatusAsync(
        string instanceArn,
        OperationStatusFilter? filter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoAccountAssignmentOperationResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountAssignmentCreationStatusRequest
                {
                    InstanceArn = instanceArn
                };
                if (filter != null) request.Filter = filter;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountAssignmentCreationStatusAsync(request);
                foreach (var s in resp.AccountAssignmentsCreationStatus)
                {
                    results.Add(new SsoAccountAssignmentOperationResult(
                        RequestId: s.RequestId,
                        Status: s.Status?.Value,
                        CreatedDate: s.CreatedDate?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list assignment creation statuses");
        }
    }

    /// <summary>
    /// List account assignment deletion statuses.
    /// </summary>
    public static async Task<List<SsoAccountAssignmentOperationResult>> ListAccountAssignmentDeletionStatusAsync(
        string instanceArn,
        OperationStatusFilter? filter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoAccountAssignmentOperationResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountAssignmentDeletionStatusRequest
                {
                    InstanceArn = instanceArn
                };
                if (filter != null) request.Filter = filter;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountAssignmentDeletionStatusAsync(request);
                foreach (var s in resp.AccountAssignmentsDeletionStatus)
                {
                    results.Add(new SsoAccountAssignmentOperationResult(
                        RequestId: s.RequestId,
                        Status: s.Status?.Value,
                        CreatedDate: s.CreatedDate?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list assignment deletion statuses");
        }
    }

    /// <summary>
    /// Describe permission set provisioning status.
    /// </summary>
    public static async Task<SsoProvisioningStatusResult> DescribePermissionSetProvisioningStatusAsync(
        string instanceArn,
        string requestId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePermissionSetProvisioningStatusAsync(
                new DescribePermissionSetProvisioningStatusRequest
                {
                    InstanceArn = instanceArn,
                    ProvisionPermissionSetRequestId = requestId
                });
            var s = resp.PermissionSetProvisioningStatus;
            return new SsoProvisioningStatusResult(
                RequestId: s.RequestId,
                Status: s.Status?.Value,
                AccountId: s.AccountId,
                PermissionSetArn: s.PermissionSetArn,
                FailureReason: s.FailureReason,
                CreatedDate: s.CreatedDate?.ToString());
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe provisioning status '{requestId}'");
        }
    }

    /// <summary>
    /// List permission set provisioning statuses.
    /// </summary>
    public static async Task<List<SsoProvisioningStatusResult>> ListPermissionSetProvisioningStatusAsync(
        string instanceArn,
        OperationStatusFilter? filter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoProvisioningStatusResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListPermissionSetProvisioningStatusRequest
                {
                    InstanceArn = instanceArn
                };
                if (filter != null) request.Filter = filter;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListPermissionSetProvisioningStatusAsync(request);
                foreach (var s in resp.PermissionSetsProvisioningStatus)
                {
                    results.Add(new SsoProvisioningStatusResult(
                        RequestId: s.RequestId,
                        Status: s.Status?.Value,
                        CreatedDate: s.CreatedDate?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list provisioning statuses");
        }
    }

    // ── Permissions boundary ────────────────────────────────────────

    /// <summary>
    /// Put a permissions boundary on a permission set.
    /// </summary>
    public static async Task PutPermissionsBoundaryToPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        string? managedPolicyArn = null,
        string? customerManagedPolicyName = null,
        string? customerManagedPolicyPath = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var boundary = new PermissionsBoundary();

        if (managedPolicyArn != null)
            boundary.ManagedPolicyArn = managedPolicyArn;
        if (customerManagedPolicyName != null)
        {
            var policyRef = new CustomerManagedPolicyReference
            {
                Name = customerManagedPolicyName
            };
            if (customerManagedPolicyPath != null)
                policyRef.Path = customerManagedPolicyPath;
            boundary.CustomerManagedPolicyReference = policyRef;
        }

        try
        {
            await client.PutPermissionsBoundaryToPermissionSetAsync(
                new PutPermissionsBoundaryToPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn,
                    PermissionsBoundary = boundary
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put permissions boundary on '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Delete the permissions boundary from a permission set.
    /// </summary>
    public static async Task DeletePermissionsBoundaryFromPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePermissionsBoundaryFromPermissionSetAsync(
                new DeletePermissionsBoundaryFromPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete permissions boundary from '{permissionSetArn}'");
        }
    }

    /// <summary>
    /// Get the permissions boundary for a permission set.
    /// </summary>
    public static async Task<SsoPermissionsBoundaryResult> GetPermissionsBoundaryForPermissionSetAsync(
        string instanceArn,
        string permissionSetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPermissionsBoundaryForPermissionSetAsync(
                new GetPermissionsBoundaryForPermissionSetRequest
                {
                    InstanceArn = instanceArn,
                    PermissionSetArn = permissionSetArn
                });
            var b = resp.PermissionsBoundary;
            return new SsoPermissionsBoundaryResult(
                ManagedPolicyArn: b.ManagedPolicyArn,
                CustomerManagedPolicyName: b.CustomerManagedPolicyReference?.Name,
                CustomerManagedPolicyPath: b.CustomerManagedPolicyReference?.Path);
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get permissions boundary for '{permissionSetArn}'");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────

    /// <summary>
    /// Tag a resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string instanceArn,
        string resourceArn,
        List<Amazon.SSOAdmin.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                InstanceArn = instanceArn,
                ResourceArn = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string instanceArn,
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                InstanceArn = instanceArn,
                ResourceArn = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<SsoTagResult>> ListTagsForResourceAsync(
        string instanceArn,
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SsoTagResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListTagsForResourceRequest
                {
                    InstanceArn = instanceArn,
                    ResourceArn = resourceArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListTagsForResourceAsync(request);
                foreach (var t in resp.Tags)
                {
                    results.Add(new SsoTagResult(Key: t.Key, Value: t.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSSOAdminException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}
