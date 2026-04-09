using Amazon;
using Amazon.Detective;
using Amazon.Detective.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record DetectiveGraphResult(
    string? Arn = null, string? CreatedTime = null);

public sealed record DetectiveGraphListResult(
    string? Arn = null, string? CreatedTime = null);

public sealed record DetectiveMemberResult(
    string? AccountId = null, string? EmailAddress = null,
    string? GraphArn = null, string? Status = null,
    string? InvitedTime = null, string? UpdatedTime = null,
    string? DisabledReason = null);

public sealed record DetectiveInvitationResult(
    string? AccountId = null, string? GraphArn = null,
    string? EmailAddress = null, string? Status = null);

public sealed record DetectiveDatasourceResult(
    string? DatasourcePackage = null, string? Status = null,
    string? LastIngestDate = null);

public sealed record DetectiveAdminAccountResult(
    string? AccountId = null, string? GraphArn = null,
    string? DelegationStatus = null);

public sealed record DetectiveMemberDatasourceResult(
    string? AccountId = null, string? GraphArn = null,
    Dictionary<string, string>? DatasourcePackageIngestState = null);

public sealed record DetectiveInvestigationResult(
    string? InvestigationId = null, string? Severity = null,
    string? Status = null, string? State = null,
    string? EntityArn = null, string? EntityType = null,
    string? CreatedTime = null);

public sealed record DetectiveIndicatorResult(
    string? IndicatorType = null,
    Dictionary<string, string>? IndicatorDetail = null);

public sealed record DetectiveTagResult(
    string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for Amazon Detective.
/// </summary>
public static class DetectiveService
{
    private static AmazonDetectiveClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonDetectiveClient>(region);

    // ── Graphs ──────────────────────────────────────────────────────

    /// <summary>
    /// Create a behavior graph.
    /// </summary>
    public static async Task<DetectiveGraphResult> CreateGraphAsync(
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGraphRequest();
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateGraphAsync(request);
            return new DetectiveGraphResult(Arn: resp.GraphArn);
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create graph");
        }
    }

    /// <summary>
    /// Delete a behavior graph.
    /// </summary>
    public static async Task DeleteGraphAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteGraphAsync(
                new DeleteGraphRequest { GraphArn = graphArn });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete graph '{graphArn}'");
        }
    }

    /// <summary>
    /// List behavior graphs.
    /// </summary>
    public static async Task<List<DetectiveGraphListResult>> ListGraphsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveGraphListResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListGraphsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListGraphsAsync(request);
                foreach (var g in resp.GraphList)
                {
                    results.Add(new DetectiveGraphListResult(
                        Arn: g.Arn,
                        CreatedTime: g.CreatedTime?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list graphs");
        }
    }

    // ── Members ─────────────────────────────────────────────────────

    /// <summary>
    /// Create member accounts in a graph.
    /// </summary>
    public static async Task<List<DetectiveMemberResult>> CreateMembersAsync(
        string graphArn,
        List<Account> accounts,
        string? message = null,
        bool? disableEmailNotification = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateMembersRequest
        {
            GraphArn = graphArn,
            Accounts = accounts
        };
        if (message != null) request.Message = message;
        if (disableEmailNotification.HasValue)
            request.DisableEmailNotification = disableEmailNotification.Value;

        try
        {
            var resp = await client.CreateMembersAsync(request);
            return resp.Members.Select(m => new DetectiveMemberResult(
                AccountId: m.AccountId,
                EmailAddress: m.EmailAddress,
                GraphArn: m.GraphArn,
                Status: m.Status?.Value,
                InvitedTime: m.InvitedTime?.ToString(),
                UpdatedTime: m.UpdatedTime?.ToString(),
                DisabledReason: m.DisabledReason?.Value)).ToList();
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create members for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Delete member accounts from a graph.
    /// </summary>
    public static async Task DeleteMembersAsync(
        string graphArn,
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteMembersAsync(
                new DeleteMembersRequest
                {
                    GraphArn = graphArn,
                    AccountIds = accountIds
                });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete members from graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Get member account details.
    /// </summary>
    public static async Task<List<DetectiveMemberResult>> GetMembersAsync(
        string graphArn,
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMembersAsync(
                new GetMembersRequest
                {
                    GraphArn = graphArn,
                    AccountIds = accountIds
                });
            return resp.MemberDetails.Select(m => new DetectiveMemberResult(
                AccountId: m.AccountId,
                EmailAddress: m.EmailAddress,
                GraphArn: m.GraphArn,
                Status: m.Status?.Value,
                InvitedTime: m.InvitedTime?.ToString(),
                UpdatedTime: m.UpdatedTime?.ToString(),
                DisabledReason: m.DisabledReason?.Value)).ToList();
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get members for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// List member accounts in a graph.
    /// </summary>
    public static async Task<List<DetectiveMemberResult>> ListMembersAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveMemberResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListMembersRequest { GraphArn = graphArn };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListMembersAsync(request);
                foreach (var m in resp.MemberDetails)
                {
                    results.Add(new DetectiveMemberResult(
                        AccountId: m.AccountId,
                        EmailAddress: m.EmailAddress,
                        GraphArn: m.GraphArn,
                        Status: m.Status?.Value,
                        InvitedTime: m.InvitedTime?.ToString(),
                        UpdatedTime: m.UpdatedTime?.ToString(),
                        DisabledReason: m.DisabledReason?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list members for graph '{graphArn}'");
        }
    }

    // ── Invitations ─────────────────────────────────────────────────

    /// <summary>
    /// Accept an invitation to a behavior graph.
    /// </summary>
    public static async Task AcceptInvitationAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AcceptInvitationAsync(
                new AcceptInvitationRequest { GraphArn = graphArn });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to accept invitation for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Reject an invitation to a behavior graph.
    /// </summary>
    public static async Task RejectInvitationAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RejectInvitationAsync(
                new RejectInvitationRequest { GraphArn = graphArn });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reject invitation for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Disassociate membership from a graph.
    /// </summary>
    public static async Task DisassociateMembershipAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateMembershipAsync(
                new DisassociateMembershipRequest { GraphArn = graphArn });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate membership from graph '{graphArn}'");
        }
    }

    /// <summary>
    /// List invitations.
    /// </summary>
    public static async Task<List<DetectiveInvitationResult>> ListInvitationsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveInvitationResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListInvitationsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListInvitationsAsync(request);
                foreach (var m in resp.Invitations)
                {
                    results.Add(new DetectiveInvitationResult(
                        AccountId: m.AccountId,
                        GraphArn: m.GraphArn,
                        EmailAddress: m.EmailAddress,
                        Status: m.Status?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list invitations");
        }
    }

    /// <summary>
    /// Start monitoring a member account.
    /// </summary>
    public static async Task StartMonitoringMemberAsync(
        string graphArn,
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartMonitoringMemberAsync(
                new StartMonitoringMemberRequest
                {
                    GraphArn = graphArn,
                    AccountId = accountId
                });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start monitoring member '{accountId}'");
        }
    }

    // ── Datasource packages ─────────────────────────────────────────

    /// <summary>
    /// List datasource packages for a graph.
    /// </summary>
    public static async Task<List<DetectiveDatasourceResult>> ListDatasourcePackagesAsync(
        string graphArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveDatasourceResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListDatasourcePackagesRequest
                {
                    GraphArn = graphArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListDatasourcePackagesAsync(request);
                foreach (var kvp in resp.DatasourcePackages)
                {
                    results.Add(new DetectiveDatasourceResult(
                        DatasourcePackage: kvp.Key,
                        Status: kvp.Value?.DatasourcePackageIngestState?.Value,
                        LastIngestDate: kvp.Value?.LastIngestStateChange?
                            .Values.FirstOrDefault()?.Timestamp?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list datasource packages for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Update datasource packages for a graph.
    /// </summary>
    public static async Task UpdateDatasourcePackagesAsync(
        string graphArn,
        List<string> datasourcePackages,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateDatasourcePackagesAsync(
                new UpdateDatasourcePackagesRequest
                {
                    GraphArn = graphArn,
                    DatasourcePackages = datasourcePackages
                });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update datasource packages for graph '{graphArn}'");
        }
    }

    // ── Organization admin ──────────────────────────────────────────

    /// <summary>
    /// Enable the organization admin account for Detective.
    /// </summary>
    public static async Task EnableOrganizationAdminAccountAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableOrganizationAdminAccountAsync(
                new EnableOrganizationAdminAccountRequest
                {
                    AccountId = accountId
                });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable organization admin '{accountId}'");
        }
    }

    /// <summary>
    /// Disable the organization admin account for Detective.
    /// </summary>
    public static async Task DisableOrganizationAdminAccountAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableOrganizationAdminAccountAsync(
                new DisableOrganizationAdminAccountRequest());
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disable organization admin account");
        }
    }

    /// <summary>
    /// List organization admin accounts.
    /// </summary>
    public static async Task<List<DetectiveAdminAccountResult>> ListOrganizationAdminAccountsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveAdminAccountResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListOrganizationAdminAccountsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListOrganizationAdminAccountsAsync(request);
                foreach (var a in resp.Administrators)
                {
                    results.Add(new DetectiveAdminAccountResult(
                        AccountId: a.AccountId,
                        GraphArn: a.GraphArn,
                        DelegationStatus: null));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list organization admin accounts");
        }
    }

    // ── Batch datasource operations ─────────────────────────────────

    /// <summary>
    /// Batch get graph member datasources.
    /// </summary>
    public static async Task<List<DetectiveMemberDatasourceResult>> BatchGetGraphMemberDatasourcesAsync(
        string graphArn,
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetGraphMemberDatasourcesAsync(
                new BatchGetGraphMemberDatasourcesRequest
                {
                    GraphArn = graphArn,
                    AccountIds = accountIds
                });
            return resp.MemberDatasources.Select(m =>
                new DetectiveMemberDatasourceResult(
                    AccountId: m.AccountId,
                    GraphArn: m.GraphArn,
                    DatasourcePackageIngestState: m.DatasourcePackageIngestHistory?
                        .ToDictionary(
                            kvp => kvp.Key ?? "",
                            kvp => kvp.Value?.Values.FirstOrDefault()?.Timestamp?.ToString() ?? "")
                )).ToList();
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to batch get graph member datasources for '{graphArn}'");
        }
    }

    /// <summary>
    /// Batch get membership datasources.
    /// </summary>
    public static async Task<List<DetectiveMemberDatasourceResult>> BatchGetMembershipDatasourcesAsync(
        List<string> graphArns,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetMembershipDatasourcesAsync(
                new BatchGetMembershipDatasourcesRequest
                {
                    GraphArns = graphArns
                });
            return resp.MembershipDatasources.Select(m =>
                new DetectiveMemberDatasourceResult(
                    AccountId: m.AccountId,
                    GraphArn: m.GraphArn,
                    DatasourcePackageIngestState: m.DatasourcePackageIngestHistory?
                        .ToDictionary(
                            kvp => kvp.Key ?? "",
                            kvp => kvp.Value?.Values.FirstOrDefault()?.Timestamp?.ToString() ?? "")
                )).ToList();
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get membership datasources");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────

    /// <summary>
    /// Tag a resource.
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
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a resource.
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
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<DetectiveTagResult>> ListTagsForResourceAsync(
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
            return resp.Tags
                .Select(t => new DetectiveTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }

    // ── Investigations ──────────────────────────────────────────────

    /// <summary>
    /// Start an investigation.
    /// </summary>
    public static async Task<DetectiveInvestigationResult> StartInvestigationAsync(
        string graphArn,
        string entityArn,
        DateTime scopeStartTime,
        DateTime scopeEndTime,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartInvestigationAsync(
                new StartInvestigationRequest
                {
                    GraphArn = graphArn,
                    EntityArn = entityArn,
                    ScopeStartTime = scopeStartTime,
                    ScopeEndTime = scopeEndTime
                });
            return new DetectiveInvestigationResult(
                InvestigationId: resp.InvestigationId);
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start investigation for entity '{entityArn}'");
        }
    }

    /// <summary>
    /// Get investigation details.
    /// </summary>
    public static async Task<DetectiveInvestigationResult> GetInvestigationAsync(
        string graphArn,
        string investigationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInvestigationAsync(
                new GetInvestigationRequest
                {
                    GraphArn = graphArn,
                    InvestigationId = investigationId
                });
            return new DetectiveInvestigationResult(
                InvestigationId: resp.InvestigationId,
                Severity: resp.Severity?.Value,
                Status: resp.Status?.Value,
                State: resp.State?.Value,
                EntityArn: resp.EntityArn,
                EntityType: resp.EntityType?.Value,
                CreatedTime: resp.CreatedTime?.ToString());
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get investigation '{investigationId}'");
        }
    }

    /// <summary>
    /// List investigations for a graph.
    /// </summary>
    public static async Task<List<DetectiveInvestigationResult>> ListInvestigationsAsync(
        string graphArn,
        FilterCriteria? filterCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveInvestigationResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListInvestigationsRequest
                {
                    GraphArn = graphArn
                };
                if (filterCriteria != null) request.FilterCriteria = filterCriteria;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListInvestigationsAsync(request);
                foreach (var i in resp.InvestigationDetails)
                {
                    results.Add(new DetectiveInvestigationResult(
                        InvestigationId: i.InvestigationId,
                        Severity: i.Severity?.Value,
                        Status: i.Status?.Value,
                        State: i.State?.Value,
                        EntityArn: i.EntityArn,
                        EntityType: i.EntityType?.Value,
                        CreatedTime: i.CreatedTime?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list investigations for graph '{graphArn}'");
        }
    }

    /// <summary>
    /// Update the state of an investigation.
    /// </summary>
    public static async Task UpdateInvestigationStateAsync(
        string graphArn,
        string investigationId,
        string state,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateInvestigationStateAsync(
                new UpdateInvestigationStateRequest
                {
                    GraphArn = graphArn,
                    InvestigationId = investigationId,
                    State = new State(state)
                });
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update investigation state '{investigationId}'");
        }
    }

    /// <summary>
    /// List indicators for an investigation.
    /// </summary>
    public static async Task<List<DetectiveIndicatorResult>> ListIndicatorsAsync(
        string graphArn,
        string investigationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<DetectiveIndicatorResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListIndicatorsRequest
                {
                    GraphArn = graphArn,
                    InvestigationId = investigationId
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListIndicatorsAsync(request);
                foreach (var ind in resp.Indicators)
                {
                    results.Add(new DetectiveIndicatorResult(
                        IndicatorType: ind.IndicatorType?.Value,
                        IndicatorDetail: new Dictionary<string, string>
                        {
                            ["Type"] = ind.IndicatorType?.Value ?? ""
                        }));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonDetectiveException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list indicators for investigation '{investigationId}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateGraphAsync"/>.</summary>
    public static DetectiveGraphResult CreateGraph(Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateGraphAsync(tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGraphAsync"/>.</summary>
    public static void DeleteGraph(string graphArn, RegionEndpoint? region = null)
        => DeleteGraphAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGraphsAsync"/>.</summary>
    public static List<DetectiveGraphListResult> ListGraphs(RegionEndpoint? region = null)
        => ListGraphsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateMembersAsync"/>.</summary>
    public static List<DetectiveMemberResult> CreateMembers(string graphArn, List<Account> accounts, string? message = null, bool? disableEmailNotification = null, RegionEndpoint? region = null)
        => CreateMembersAsync(graphArn, accounts, message, disableEmailNotification, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteMembersAsync"/>.</summary>
    public static void DeleteMembers(string graphArn, List<string> accountIds, RegionEndpoint? region = null)
        => DeleteMembersAsync(graphArn, accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMembersAsync"/>.</summary>
    public static List<DetectiveMemberResult> GetMembers(string graphArn, List<string> accountIds, RegionEndpoint? region = null)
        => GetMembersAsync(graphArn, accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListMembersAsync"/>.</summary>
    public static List<DetectiveMemberResult> ListMembers(string graphArn, RegionEndpoint? region = null)
        => ListMembersAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AcceptInvitationAsync"/>.</summary>
    public static void AcceptInvitation(string graphArn, RegionEndpoint? region = null)
        => AcceptInvitationAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RejectInvitationAsync"/>.</summary>
    public static void RejectInvitation(string graphArn, RegionEndpoint? region = null)
        => RejectInvitationAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateMembershipAsync"/>.</summary>
    public static void DisassociateMembership(string graphArn, RegionEndpoint? region = null)
        => DisassociateMembershipAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInvitationsAsync"/>.</summary>
    public static List<DetectiveInvitationResult> ListInvitations(RegionEndpoint? region = null)
        => ListInvitationsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartMonitoringMemberAsync"/>.</summary>
    public static void StartMonitoringMember(string graphArn, string accountId, RegionEndpoint? region = null)
        => StartMonitoringMemberAsync(graphArn, accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDatasourcePackagesAsync"/>.</summary>
    public static List<DetectiveDatasourceResult> ListDatasourcePackages(string graphArn, RegionEndpoint? region = null)
        => ListDatasourcePackagesAsync(graphArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDatasourcePackagesAsync"/>.</summary>
    public static void UpdateDatasourcePackages(string graphArn, List<string> datasourcePackages, RegionEndpoint? region = null)
        => UpdateDatasourcePackagesAsync(graphArn, datasourcePackages, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableOrganizationAdminAccountAsync"/>.</summary>
    public static void EnableOrganizationAdminAccount(string accountId, RegionEndpoint? region = null)
        => EnableOrganizationAdminAccountAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableOrganizationAdminAccountAsync"/>.</summary>
    public static void DisableOrganizationAdminAccount(RegionEndpoint? region = null)
        => DisableOrganizationAdminAccountAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListOrganizationAdminAccountsAsync"/>.</summary>
    public static List<DetectiveAdminAccountResult> ListOrganizationAdminAccounts(RegionEndpoint? region = null)
        => ListOrganizationAdminAccountsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetGraphMemberDatasourcesAsync"/>.</summary>
    public static List<DetectiveMemberDatasourceResult> BatchGetGraphMemberDatasources(string graphArn, List<string> accountIds, RegionEndpoint? region = null)
        => BatchGetGraphMemberDatasourcesAsync(graphArn, accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetMembershipDatasourcesAsync"/>.</summary>
    public static List<DetectiveMemberDatasourceResult> BatchGetMembershipDatasources(List<string> graphArns, RegionEndpoint? region = null)
        => BatchGetMembershipDatasourcesAsync(graphArns, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static List<DetectiveTagResult> ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartInvestigationAsync"/>.</summary>
    public static DetectiveInvestigationResult StartInvestigation(string graphArn, string entityArn, DateTime scopeStartTime, DateTime scopeEndTime, RegionEndpoint? region = null)
        => StartInvestigationAsync(graphArn, entityArn, scopeStartTime, scopeEndTime, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetInvestigationAsync"/>.</summary>
    public static DetectiveInvestigationResult GetInvestigation(string graphArn, string investigationId, RegionEndpoint? region = null)
        => GetInvestigationAsync(graphArn, investigationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInvestigationsAsync"/>.</summary>
    public static List<DetectiveInvestigationResult> ListInvestigations(string graphArn, FilterCriteria? filterCriteria = null, RegionEndpoint? region = null)
        => ListInvestigationsAsync(graphArn, filterCriteria, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateInvestigationStateAsync"/>.</summary>
    public static void UpdateInvestigationState(string graphArn, string investigationId, string state, RegionEndpoint? region = null)
        => UpdateInvestigationStateAsync(graphArn, investigationId, state, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListIndicatorsAsync"/>.</summary>
    public static List<DetectiveIndicatorResult> ListIndicators(string graphArn, string investigationId, RegionEndpoint? region = null)
        => ListIndicatorsAsync(graphArn, investigationId, region).GetAwaiter().GetResult();

}
