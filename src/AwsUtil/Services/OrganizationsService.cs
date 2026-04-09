using Amazon;
using Amazon.Organizations;
using Amazon.Organizations.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record OrganizationResult(
    string? Id = null, string? Arn = null,
    string? MasterAccountArn = null, string? MasterAccountId = null,
    string? MasterAccountEmail = null, string? FeatureSet = null);

public sealed record OrgAccountResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Email = null,
    string? Status = null, string? JoinedMethod = null,
    string? JoinedTimestamp = null);

public sealed record CreateAccountStatusResult(
    string? Id = null, string? AccountName = null,
    string? AccountId = null, string? State = null,
    string? RequestedTimestamp = null, string? CompletedTimestamp = null,
    string? FailureReason = null);

public sealed record OrgUnitResult(
    string? Id = null, string? Arn = null,
    string? Name = null);

public sealed record OrgChildResult(
    string? Id = null, string? Type = null);

public sealed record OrgParentResult(
    string? Id = null, string? Type = null);

public sealed record OrgRootResult(
    string? Id = null, string? Arn = null,
    string? Name = null, List<string>? PolicyTypes = null);

public sealed record OrgPolicyResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Description = null,
    string? Type = null, bool? AwsManaged = null,
    string? Content = null);

public sealed record OrgPolicySummaryResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Description = null,
    string? Type = null, bool? AwsManaged = null);

public sealed record PolicyTargetResult(
    string? TargetId = null, string? Arn = null,
    string? Name = null, string? Type = null);

public sealed record OrgHandshakeResult(
    string? Id = null, string? Arn = null,
    string? State = null, string? Action = null,
    string? RequestedTimestamp = null, string? ExpirationTimestamp = null);

public sealed record OrgTagResult(string? Key = null, string? Value = null);

public sealed record EnabledServiceResult(
    string? ServicePrincipal = null, string? DateEnabled = null);

public sealed record DelegatedAdminResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Email = null,
    string? Status = null, string? JoinedMethod = null,
    string? DelegationEnabledDate = null);

/// <summary>
/// Utility helpers for AWS Organizations.
/// </summary>
public static class OrganizationsService
{
    private static AmazonOrganizationsClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonOrganizationsClient>(region);

    // ── Organization ────────────────────────────────────────────────

    /// <summary>
    /// Create an organization.
    /// </summary>
    public static async Task<OrganizationResult> CreateOrganizationAsync(
        string? featureSet = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateOrganizationRequest();
        if (featureSet != null) request.FeatureSet = new OrganizationFeatureSet(featureSet);

        try
        {
            var resp = await client.CreateOrganizationAsync(request);
            var org = resp.Organization;
            return new OrganizationResult(
                Id: org.Id, Arn: org.Arn,
                MasterAccountArn: org.MasterAccountArn,
                MasterAccountId: org.MasterAccountId,
                MasterAccountEmail: org.MasterAccountEmail,
                FeatureSet: org.FeatureSet?.Value);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create organization");
        }
    }

    /// <summary>
    /// Delete the organization.
    /// </summary>
    public static async Task DeleteOrganizationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteOrganizationAsync(new DeleteOrganizationRequest());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete organization");
        }
    }

    /// <summary>
    /// Describe the organization.
    /// </summary>
    public static async Task<OrganizationResult> DescribeOrganizationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeOrganizationAsync(
                new DescribeOrganizationRequest());
            var org = resp.Organization;
            return new OrganizationResult(
                Id: org.Id, Arn: org.Arn,
                MasterAccountArn: org.MasterAccountArn,
                MasterAccountId: org.MasterAccountId,
                MasterAccountEmail: org.MasterAccountEmail,
                FeatureSet: org.FeatureSet?.Value);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe organization");
        }
    }

    // ── Accounts ────────────────────────────────────────────────────

    /// <summary>
    /// List all accounts in the organization.
    /// </summary>
    public static async Task<List<OrgAccountResult>> ListAccountsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgAccountResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountsAsync(request);
                foreach (var a in resp.Accounts)
                {
                    results.Add(new OrgAccountResult(
                        Id: a.Id, Arn: a.Arn, Name: a.Name,
                        Email: a.Email, Status: a.Status?.Value,
                        JoinedMethod: a.JoinedMethod?.Value,
                        JoinedTimestamp: a.JoinedTimestamp?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list accounts");
        }
    }

    /// <summary>
    /// Describe a specific account.
    /// </summary>
    public static async Task<OrgAccountResult> DescribeAccountAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccountAsync(
                new DescribeAccountRequest { AccountId = accountId });
            var a = resp.Account;
            return new OrgAccountResult(
                Id: a.Id, Arn: a.Arn, Name: a.Name,
                Email: a.Email, Status: a.Status?.Value,
                JoinedMethod: a.JoinedMethod?.Value,
                JoinedTimestamp: a.JoinedTimestamp?.ToString());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe account '{accountId}'");
        }
    }

    /// <summary>
    /// Create a new account in the organization.
    /// </summary>
    public static async Task<CreateAccountStatusResult> CreateAccountAsync(
        string email,
        string accountName,
        string? iamUserAccessToBilling = null,
        string? roleName = null,
        List<Amazon.Organizations.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAccountRequest
        {
            Email = email,
            AccountName = accountName
        };
        if (iamUserAccessToBilling != null)
            request.IamUserAccessToBilling = new IAMUserAccessToBilling(iamUserAccessToBilling);
        if (roleName != null) request.RoleName = roleName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAccountAsync(request);
            var s = resp.CreateAccountStatus;
            return new CreateAccountStatusResult(
                Id: s.Id, AccountName: s.AccountName,
                AccountId: s.AccountId, State: s.State?.Value,
                RequestedTimestamp: s.RequestedTimestamp?.ToString(),
                CompletedTimestamp: s.CompletedTimestamp?.ToString(),
                FailureReason: s.FailureReason?.Value);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create account '{accountName}'");
        }
    }

    /// <summary>
    /// Close an account in the organization.
    /// </summary>
    public static async Task CloseAccountAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CloseAccountAsync(
                new CloseAccountRequest { AccountId = accountId });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to close account '{accountId}'");
        }
    }

    /// <summary>
    /// Move an account to a different organizational unit.
    /// </summary>
    public static async Task MoveAccountAsync(
        string accountId,
        string sourceParentId,
        string destinationParentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.MoveAccountAsync(new MoveAccountRequest
            {
                AccountId = accountId,
                SourceParentId = sourceParentId,
                DestinationParentId = destinationParentId
            });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to move account '{accountId}'");
        }
    }

    // ── Organizational Units ────────────────────────────────────────

    /// <summary>
    /// Create an organizational unit.
    /// </summary>
    public static async Task<OrgUnitResult> CreateOrganizationalUnitAsync(
        string parentId,
        string name,
        List<Amazon.Organizations.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateOrganizationalUnitRequest
        {
            ParentId = parentId,
            Name = name
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateOrganizationalUnitAsync(request);
            var ou = resp.OrganizationalUnit;
            return new OrgUnitResult(Id: ou.Id, Arn: ou.Arn, Name: ou.Name);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create organizational unit '{name}'");
        }
    }

    /// <summary>
    /// Delete an organizational unit.
    /// </summary>
    public static async Task DeleteOrganizationalUnitAsync(
        string organizationalUnitId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteOrganizationalUnitAsync(
                new DeleteOrganizationalUnitRequest
                {
                    OrganizationalUnitId = organizationalUnitId
                });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete OU '{organizationalUnitId}'");
        }
    }

    /// <summary>
    /// Describe an organizational unit.
    /// </summary>
    public static async Task<OrgUnitResult> DescribeOrganizationalUnitAsync(
        string organizationalUnitId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeOrganizationalUnitAsync(
                new DescribeOrganizationalUnitRequest
                {
                    OrganizationalUnitId = organizationalUnitId
                });
            var ou = resp.OrganizationalUnit;
            return new OrgUnitResult(Id: ou.Id, Arn: ou.Arn, Name: ou.Name);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe OU '{organizationalUnitId}'");
        }
    }

    /// <summary>
    /// List organizational units for a parent.
    /// </summary>
    public static async Task<List<OrgUnitResult>> ListOrganizationalUnitsForParentAsync(
        string parentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgUnitResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListOrganizationalUnitsForParentRequest
                {
                    ParentId = parentId
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListOrganizationalUnitsForParentAsync(request);
                foreach (var ou in resp.OrganizationalUnits)
                {
                    results.Add(new OrgUnitResult(
                        Id: ou.Id, Arn: ou.Arn, Name: ou.Name));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list OUs for parent '{parentId}'");
        }
    }

    /// <summary>
    /// List children of a parent.
    /// </summary>
    public static async Task<List<OrgChildResult>> ListChildrenAsync(
        string parentId,
        string childType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgChildResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListChildrenRequest
                {
                    ParentId = parentId,
                    ChildType = new ChildType(childType)
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListChildrenAsync(request);
                foreach (var c in resp.Children)
                {
                    results.Add(new OrgChildResult(
                        Id: c.Id, Type: c.Type?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list children for parent '{parentId}'");
        }
    }

    /// <summary>
    /// List parents of a child.
    /// </summary>
    public static async Task<List<OrgParentResult>> ListParentsAsync(
        string childId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgParentResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListParentsRequest { ChildId = childId };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListParentsAsync(request);
                foreach (var p in resp.Parents)
                {
                    results.Add(new OrgParentResult(
                        Id: p.Id, Type: p.Type?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list parents for child '{childId}'");
        }
    }

    /// <summary>
    /// List roots of the organization.
    /// </summary>
    public static async Task<List<OrgRootResult>> ListRootsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgRootResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListRootsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListRootsAsync(request);
                foreach (var r in resp.Roots)
                {
                    results.Add(new OrgRootResult(
                        Id: r.Id, Arn: r.Arn, Name: r.Name,
                        PolicyTypes: r.PolicyTypes?
                            .Select(pt => pt.Type?.Value ?? "")
                            .ToList()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list roots");
        }
    }

    // ── Policies ────────────────────────────────────────────────────

    /// <summary>
    /// Create a policy.
    /// </summary>
    public static async Task<OrgPolicyResult> CreatePolicyAsync(
        string content,
        string description,
        string name,
        string type,
        List<Amazon.Organizations.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePolicyRequest
        {
            Content = content,
            Description = description,
            Name = name,
            Type = new PolicyType(type)
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePolicyAsync(request);
            var p = resp.Policy;
            return new OrgPolicyResult(
                Id: p.PolicySummary.Id, Arn: p.PolicySummary.Arn,
                Name: p.PolicySummary.Name,
                Description: p.PolicySummary.Description,
                Type: p.PolicySummary.Type?.Value,
                AwsManaged: p.PolicySummary.AwsManaged,
                Content: p.Content);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create policy '{name}'");
        }
    }

    /// <summary>
    /// Delete a policy.
    /// </summary>
    public static async Task DeletePolicyAsync(
        string policyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePolicyAsync(
                new DeletePolicyRequest { PolicyId = policyId });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete policy '{policyId}'");
        }
    }

    /// <summary>
    /// Describe a policy.
    /// </summary>
    public static async Task<OrgPolicyResult> DescribePolicyAsync(
        string policyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePolicyAsync(
                new DescribePolicyRequest { PolicyId = policyId });
            var p = resp.Policy;
            return new OrgPolicyResult(
                Id: p.PolicySummary.Id, Arn: p.PolicySummary.Arn,
                Name: p.PolicySummary.Name,
                Description: p.PolicySummary.Description,
                Type: p.PolicySummary.Type?.Value,
                AwsManaged: p.PolicySummary.AwsManaged,
                Content: p.Content);
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe policy '{policyId}'");
        }
    }

    /// <summary>
    /// List policies of a given type.
    /// </summary>
    public static async Task<List<OrgPolicySummaryResult>> ListPoliciesAsync(
        string filter,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgPolicySummaryResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListPoliciesRequest
                {
                    Filter = new PolicyType(filter)
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListPoliciesAsync(request);
                foreach (var p in resp.Policies)
                {
                    results.Add(new OrgPolicySummaryResult(
                        Id: p.Id, Arn: p.Arn, Name: p.Name,
                        Description: p.Description,
                        Type: p.Type?.Value,
                        AwsManaged: p.AwsManaged));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list policies");
        }
    }

    /// <summary>
    /// Attach a policy to a target.
    /// </summary>
    public static async Task AttachPolicyAsync(
        string policyId,
        string targetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachPolicyAsync(new AttachPolicyRequest
            {
                PolicyId = policyId,
                TargetId = targetId
            });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach policy '{policyId}' to '{targetId}'");
        }
    }

    /// <summary>
    /// Detach a policy from a target.
    /// </summary>
    public static async Task DetachPolicyAsync(
        string policyId,
        string targetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachPolicyAsync(new DetachPolicyRequest
            {
                PolicyId = policyId,
                TargetId = targetId
            });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach policy '{policyId}' from '{targetId}'");
        }
    }

    /// <summary>
    /// List targets for a policy.
    /// </summary>
    public static async Task<List<PolicyTargetResult>> ListTargetsForPolicyAsync(
        string policyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<PolicyTargetResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListTargetsForPolicyRequest
                {
                    PolicyId = policyId
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListTargetsForPolicyAsync(request);
                foreach (var t in resp.Targets)
                {
                    results.Add(new PolicyTargetResult(
                        TargetId: t.TargetId, Arn: t.Arn,
                        Name: t.Name, Type: t.Type?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list targets for policy '{policyId}'");
        }
    }

    /// <summary>
    /// List policies attached to a target.
    /// </summary>
    public static async Task<List<OrgPolicySummaryResult>> ListPoliciesForTargetAsync(
        string targetId,
        string filter,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgPolicySummaryResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListPoliciesForTargetRequest
                {
                    TargetId = targetId,
                    Filter = new PolicyType(filter)
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListPoliciesForTargetAsync(request);
                foreach (var p in resp.Policies)
                {
                    results.Add(new OrgPolicySummaryResult(
                        Id: p.Id, Arn: p.Arn, Name: p.Name,
                        Description: p.Description,
                        Type: p.Type?.Value,
                        AwsManaged: p.AwsManaged));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list policies for target '{targetId}'");
        }
    }

    /// <summary>
    /// Enable a policy type in a root.
    /// </summary>
    public static async Task<OrgRootResult> EnablePolicyTypeAsync(
        string rootId,
        string policyType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.EnablePolicyTypeAsync(
                new EnablePolicyTypeRequest
                {
                    RootId = rootId,
                    PolicyType = new PolicyType(policyType)
                });
            var r = resp.Root;
            return new OrgRootResult(
                Id: r.Id, Arn: r.Arn, Name: r.Name,
                PolicyTypes: r.PolicyTypes?
                    .Select(pt => pt.Type?.Value ?? "")
                    .ToList());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable policy type '{policyType}' on root '{rootId}'");
        }
    }

    /// <summary>
    /// Disable a policy type in a root.
    /// </summary>
    public static async Task<OrgRootResult> DisablePolicyTypeAsync(
        string rootId,
        string policyType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisablePolicyTypeAsync(
                new DisablePolicyTypeRequest
                {
                    RootId = rootId,
                    PolicyType = new PolicyType(policyType)
                });
            var r = resp.Root;
            return new OrgRootResult(
                Id: r.Id, Arn: r.Arn, Name: r.Name,
                PolicyTypes: r.PolicyTypes?
                    .Select(pt => pt.Type?.Value ?? "")
                    .ToList());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable policy type '{policyType}' on root '{rootId}'");
        }
    }

    // ── Service access ──────────────────────────────────────────────

    /// <summary>
    /// Enable an AWS service to access the organization.
    /// </summary>
    public static async Task EnableAWSServiceAccessAsync(
        string servicePrincipal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableAWSServiceAccessAsync(
                new EnableAWSServiceAccessRequest
                {
                    ServicePrincipal = servicePrincipal
                });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable service access for '{servicePrincipal}'");
        }
    }

    /// <summary>
    /// Disable an AWS service from accessing the organization.
    /// </summary>
    public static async Task DisableAWSServiceAccessAsync(
        string servicePrincipal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableAWSServiceAccessAsync(
                new DisableAWSServiceAccessRequest
                {
                    ServicePrincipal = servicePrincipal
                });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable service access for '{servicePrincipal}'");
        }
    }

    /// <summary>
    /// List AWS services enabled for the organization.
    /// </summary>
    public static async Task<List<EnabledServiceResult>> ListAWSServiceAccessForOrganizationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<EnabledServiceResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAWSServiceAccessForOrganizationRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAWSServiceAccessForOrganizationAsync(request);
                foreach (var s in resp.EnabledServicePrincipals)
                {
                    results.Add(new EnabledServiceResult(
                        ServicePrincipal: s.ServicePrincipal,
                        DateEnabled: s.DateEnabled?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list AWS service access for organization");
        }
    }

    // ── Handshakes ──────────────────────────────────────────────────

    /// <summary>
    /// Invite an account to the organization.
    /// </summary>
    public static async Task<OrgHandshakeResult> InviteAccountToOrganizationAsync(
        string targetId,
        string targetType,
        string? notes = null,
        List<Amazon.Organizations.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InviteAccountToOrganizationRequest
        {
            Target = new HandshakeParty
            {
                Id = targetId,
                Type = new HandshakePartyType(targetType)
            }
        };
        if (notes != null) request.Notes = notes;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.InviteAccountToOrganizationAsync(request);
            var h = resp.Handshake;
            return new OrgHandshakeResult(
                Id: h.Id, Arn: h.Arn,
                State: h.State?.Value,
                Action: h.Action?.Value,
                RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                ExpirationTimestamp: h.ExpirationTimestamp?.ToString());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invite account '{targetId}'");
        }
    }

    /// <summary>
    /// Accept a handshake.
    /// </summary>
    public static async Task<OrgHandshakeResult> AcceptHandshakeAsync(
        string handshakeId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AcceptHandshakeAsync(
                new AcceptHandshakeRequest { HandshakeId = handshakeId });
            var h = resp.Handshake;
            return new OrgHandshakeResult(
                Id: h.Id, Arn: h.Arn,
                State: h.State?.Value,
                Action: h.Action?.Value,
                RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                ExpirationTimestamp: h.ExpirationTimestamp?.ToString());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to accept handshake '{handshakeId}'");
        }
    }

    /// <summary>
    /// Decline a handshake.
    /// </summary>
    public static async Task<OrgHandshakeResult> DeclineHandshakeAsync(
        string handshakeId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeclineHandshakeAsync(
                new DeclineHandshakeRequest { HandshakeId = handshakeId });
            var h = resp.Handshake;
            return new OrgHandshakeResult(
                Id: h.Id, Arn: h.Arn,
                State: h.State?.Value,
                Action: h.Action?.Value,
                RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                ExpirationTimestamp: h.ExpirationTimestamp?.ToString());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to decline handshake '{handshakeId}'");
        }
    }

    /// <summary>
    /// Cancel a handshake.
    /// </summary>
    public static async Task<OrgHandshakeResult> CancelHandshakeAsync(
        string handshakeId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelHandshakeAsync(
                new CancelHandshakeRequest { HandshakeId = handshakeId });
            var h = resp.Handshake;
            return new OrgHandshakeResult(
                Id: h.Id, Arn: h.Arn,
                State: h.State?.Value,
                Action: h.Action?.Value,
                RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                ExpirationTimestamp: h.ExpirationTimestamp?.ToString());
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel handshake '{handshakeId}'");
        }
    }

    /// <summary>
    /// List handshakes for the account.
    /// </summary>
    public static async Task<List<OrgHandshakeResult>> ListHandshakesForAccountAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgHandshakeResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListHandshakesForAccountRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListHandshakesForAccountAsync(request);
                foreach (var h in resp.Handshakes)
                {
                    results.Add(new OrgHandshakeResult(
                        Id: h.Id, Arn: h.Arn,
                        State: h.State?.Value,
                        Action: h.Action?.Value,
                        RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                        ExpirationTimestamp: h.ExpirationTimestamp?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list handshakes for account");
        }
    }

    /// <summary>
    /// List handshakes for the organization.
    /// </summary>
    public static async Task<List<OrgHandshakeResult>> ListHandshakesForOrganizationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgHandshakeResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListHandshakesForOrganizationRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListHandshakesForOrganizationAsync(request);
                foreach (var h in resp.Handshakes)
                {
                    results.Add(new OrgHandshakeResult(
                        Id: h.Id, Arn: h.Arn,
                        State: h.State?.Value,
                        Action: h.Action?.Value,
                        RequestedTimestamp: h.RequestedTimestamp?.ToString(),
                        ExpirationTimestamp: h.ExpirationTimestamp?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list handshakes for organization");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────

    /// <summary>
    /// Tag a resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceId,
        List<Amazon.Organizations.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceId = resourceId,
                Tags = tags
            });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceId}'");
        }
    }

    /// <summary>
    /// Remove tags from a resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceId,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceId = resourceId,
                TagKeys = tagKeys
            });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceId}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<OrgTagResult>> ListTagsForResourceAsync(
        string resourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<OrgTagResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListTagsForResourceRequest
                {
                    ResourceId = resourceId
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListTagsForResourceAsync(request);
                foreach (var t in resp.Tags)
                {
                    results.Add(new OrgTagResult(Key: t.Key, Value: t.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceId}'");
        }
    }

    // ── Delegated administrators ────────────────────────────────────

    /// <summary>
    /// Register a delegated administrator.
    /// </summary>
    public static async Task RegisterDelegatedAdministratorAsync(
        string accountId,
        string servicePrincipal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RegisterDelegatedAdministratorAsync(
                new RegisterDelegatedAdministratorRequest
                {
                    AccountId = accountId,
                    ServicePrincipal = servicePrincipal
                });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to register delegated admin '{accountId}'");
        }
    }

    /// <summary>
    /// Deregister a delegated administrator.
    /// </summary>
    public static async Task DeregisterDelegatedAdministratorAsync(
        string accountId,
        string servicePrincipal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeregisterDelegatedAdministratorAsync(
                new DeregisterDelegatedAdministratorRequest
                {
                    AccountId = accountId,
                    ServicePrincipal = servicePrincipal
                });
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister delegated admin '{accountId}'");
        }
    }

    /// <summary>
    /// List delegated administrators.
    /// </summary>
    public static async Task<List<DelegatedAdminResult>> ListDelegatedAdministratorsAsync(
        string? servicePrincipal = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DelegatedAdminResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListDelegatedAdministratorsRequest();
                if (servicePrincipal != null) request.ServicePrincipal = servicePrincipal;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListDelegatedAdministratorsAsync(request);
                foreach (var d in resp.DelegatedAdministrators)
                {
                    results.Add(new DelegatedAdminResult(
                        Id: d.Id, Arn: d.Arn, Name: d.Name,
                        Email: d.Email, Status: d.Status?.Value,
                        JoinedMethod: d.JoinedMethod?.Value,
                        DelegationEnabledDate: d.DelegationEnabledDate?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonOrganizationsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list delegated administrators");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateOrganizationAsync"/>.</summary>
    public static OrganizationResult CreateOrganization(string? featureSet = null, RegionEndpoint? region = null)
        => CreateOrganizationAsync(featureSet, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteOrganizationAsync"/>.</summary>
    public static void DeleteOrganization(RegionEndpoint? region = null)
        => DeleteOrganizationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeOrganizationAsync"/>.</summary>
    public static OrganizationResult DescribeOrganization(RegionEndpoint? region = null)
        => DescribeOrganizationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccountsAsync"/>.</summary>
    public static List<OrgAccountResult> ListAccounts(RegionEndpoint? region = null)
        => ListAccountsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAccountAsync"/>.</summary>
    public static OrgAccountResult DescribeAccount(string accountId, RegionEndpoint? region = null)
        => DescribeAccountAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAccountAsync"/>.</summary>
    public static CreateAccountStatusResult CreateAccount(string email, string accountName, string? iamUserAccessToBilling = null, string? roleName = null, List<Amazon.Organizations.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateAccountAsync(email, accountName, iamUserAccessToBilling, roleName, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CloseAccountAsync"/>.</summary>
    public static void CloseAccount(string accountId, RegionEndpoint? region = null)
        => CloseAccountAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MoveAccountAsync"/>.</summary>
    public static void MoveAccount(string accountId, string sourceParentId, string destinationParentId, RegionEndpoint? region = null)
        => MoveAccountAsync(accountId, sourceParentId, destinationParentId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateOrganizationalUnitAsync"/>.</summary>
    public static OrgUnitResult CreateOrganizationalUnit(string parentId, string name, List<Amazon.Organizations.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateOrganizationalUnitAsync(parentId, name, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteOrganizationalUnitAsync"/>.</summary>
    public static void DeleteOrganizationalUnit(string organizationalUnitId, RegionEndpoint? region = null)
        => DeleteOrganizationalUnitAsync(organizationalUnitId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeOrganizationalUnitAsync"/>.</summary>
    public static OrgUnitResult DescribeOrganizationalUnit(string organizationalUnitId, RegionEndpoint? region = null)
        => DescribeOrganizationalUnitAsync(organizationalUnitId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListOrganizationalUnitsForParentAsync"/>.</summary>
    public static List<OrgUnitResult> ListOrganizationalUnitsForParent(string parentId, RegionEndpoint? region = null)
        => ListOrganizationalUnitsForParentAsync(parentId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListChildrenAsync"/>.</summary>
    public static List<OrgChildResult> ListChildren(string parentId, string childType, RegionEndpoint? region = null)
        => ListChildrenAsync(parentId, childType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListParentsAsync"/>.</summary>
    public static List<OrgParentResult> ListParents(string childId, RegionEndpoint? region = null)
        => ListParentsAsync(childId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRootsAsync"/>.</summary>
    public static List<OrgRootResult> ListRoots(RegionEndpoint? region = null)
        => ListRootsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePolicyAsync"/>.</summary>
    public static OrgPolicyResult CreatePolicy(string content, string description, string name, string type, List<Amazon.Organizations.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreatePolicyAsync(content, description, name, type, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePolicyAsync"/>.</summary>
    public static void DeletePolicy(string policyId, RegionEndpoint? region = null)
        => DeletePolicyAsync(policyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribePolicyAsync"/>.</summary>
    public static OrgPolicyResult DescribePolicy(string policyId, RegionEndpoint? region = null)
        => DescribePolicyAsync(policyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPoliciesAsync"/>.</summary>
    public static List<OrgPolicySummaryResult> ListPolicies(string filter, RegionEndpoint? region = null)
        => ListPoliciesAsync(filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachPolicyAsync"/>.</summary>
    public static void AttachPolicy(string policyId, string targetId, RegionEndpoint? region = null)
        => AttachPolicyAsync(policyId, targetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachPolicyAsync"/>.</summary>
    public static void DetachPolicy(string policyId, string targetId, RegionEndpoint? region = null)
        => DetachPolicyAsync(policyId, targetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTargetsForPolicyAsync"/>.</summary>
    public static List<PolicyTargetResult> ListTargetsForPolicy(string policyId, RegionEndpoint? region = null)
        => ListTargetsForPolicyAsync(policyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPoliciesForTargetAsync"/>.</summary>
    public static List<OrgPolicySummaryResult> ListPoliciesForTarget(string targetId, string filter, RegionEndpoint? region = null)
        => ListPoliciesForTargetAsync(targetId, filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnablePolicyTypeAsync"/>.</summary>
    public static OrgRootResult EnablePolicyType(string rootId, string policyType, RegionEndpoint? region = null)
        => EnablePolicyTypeAsync(rootId, policyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisablePolicyTypeAsync"/>.</summary>
    public static OrgRootResult DisablePolicyType(string rootId, string policyType, RegionEndpoint? region = null)
        => DisablePolicyTypeAsync(rootId, policyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableAWSServiceAccessAsync"/>.</summary>
    public static void EnableAWSServiceAccess(string servicePrincipal, RegionEndpoint? region = null)
        => EnableAWSServiceAccessAsync(servicePrincipal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableAWSServiceAccessAsync"/>.</summary>
    public static void DisableAWSServiceAccess(string servicePrincipal, RegionEndpoint? region = null)
        => DisableAWSServiceAccessAsync(servicePrincipal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAWSServiceAccessForOrganizationAsync"/>.</summary>
    public static List<EnabledServiceResult> ListAWSServiceAccessForOrganization(RegionEndpoint? region = null)
        => ListAWSServiceAccessForOrganizationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="InviteAccountToOrganizationAsync"/>.</summary>
    public static OrgHandshakeResult InviteAccountToOrganization(string targetId, string targetType, string? notes = null, List<Amazon.Organizations.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => InviteAccountToOrganizationAsync(targetId, targetType, notes, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AcceptHandshakeAsync"/>.</summary>
    public static OrgHandshakeResult AcceptHandshake(string handshakeId, RegionEndpoint? region = null)
        => AcceptHandshakeAsync(handshakeId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeclineHandshakeAsync"/>.</summary>
    public static OrgHandshakeResult DeclineHandshake(string handshakeId, RegionEndpoint? region = null)
        => DeclineHandshakeAsync(handshakeId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelHandshakeAsync"/>.</summary>
    public static OrgHandshakeResult CancelHandshake(string handshakeId, RegionEndpoint? region = null)
        => CancelHandshakeAsync(handshakeId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListHandshakesForAccountAsync"/>.</summary>
    public static List<OrgHandshakeResult> ListHandshakesForAccount(RegionEndpoint? region = null)
        => ListHandshakesForAccountAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListHandshakesForOrganizationAsync"/>.</summary>
    public static List<OrgHandshakeResult> ListHandshakesForOrganization(RegionEndpoint? region = null)
        => ListHandshakesForOrganizationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceId, List<Amazon.Organizations.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceId, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceId, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static List<OrgTagResult> ListTagsForResource(string resourceId, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterDelegatedAdministratorAsync"/>.</summary>
    public static void RegisterDelegatedAdministrator(string accountId, string servicePrincipal, RegionEndpoint? region = null)
        => RegisterDelegatedAdministratorAsync(accountId, servicePrincipal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterDelegatedAdministratorAsync"/>.</summary>
    public static void DeregisterDelegatedAdministrator(string accountId, string servicePrincipal, RegionEndpoint? region = null)
        => DeregisterDelegatedAdministratorAsync(accountId, servicePrincipal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDelegatedAdministratorsAsync"/>.</summary>
    public static List<DelegatedAdminResult> ListDelegatedAdministrators(string? servicePrincipal = null, RegionEndpoint? region = null)
        => ListDelegatedAdministratorsAsync(servicePrincipal, region).GetAwaiter().GetResult();

}
