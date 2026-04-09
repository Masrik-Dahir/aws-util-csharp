using Amazon;
using Amazon.SecurityHub;
using Amazon.SecurityHub.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record SecurityHubResult(
    string? HubArn = null, string? SubscribedAt = null,
    bool? AutoEnableControls = null);

public sealed record SecurityHubFindingResult(
    string? Id = null, string? ProductArn = null,
    string? GeneratorId = null, string? AwsAccountId = null,
    string? Title = null, string? Description = null,
    string? SeverityLabel = null, string? WorkflowStatus = null,
    string? RecordState = null, string? CreatedAt = null,
    string? UpdatedAt = null);

public sealed record SecurityHubInsightResult(
    string? InsightArn = null, string? Name = null,
    string? GroupByAttribute = null);

public sealed record SecurityHubInsightResultsResult(
    string? InsightArn = null,
    List<Dictionary<string, string>>? ResultValues = null);

public sealed record SecurityHubBatchImportResult(
    int? FailedCount = null, int? SuccessCount = null,
    List<string>? FailedFindings = null);

public sealed record SecurityHubBatchUpdateResult(
    List<string>? ProcessedFindings = null,
    List<string>? UnprocessedFindings = null);

public sealed record SecurityHubProductResult(
    string? ProductArn = null, string? ProductName = null,
    string? CompanyName = null, string? Description = null,
    string? MarketplaceUrl = null);

public sealed record SecurityHubProductSubscriptionResult(
    string? ProductSubscriptionArn = null);

public sealed record SecurityHubStandardResult(
    string? StandardsArn = null, string? Name = null,
    string? Description = null, bool? EnabledByDefault = null);

public sealed record SecurityHubStandardsSubscriptionResult(
    string? StandardsSubscriptionArn = null,
    string? StandardsArn = null, string? StandardsStatus = null);

public sealed record SecurityHubStandardsControlResult(
    string? StandardsControlArn = null, string? ControlId = null,
    string? ControlStatus = null, string? Title = null,
    string? Description = null, string? SeverityRating = null,
    string? RemediationUrl = null);

public sealed record SecurityHubMemberResult(
    string? AccountId = null, string? Email = null,
    string? MasterId = null, string? MemberStatus = null,
    string? InvitedAt = null, string? UpdatedAt = null);

public sealed record SecurityHubActionTargetResult(
    string? ActionTargetArn = null, string? Name = null,
    string? Description = null);

public sealed record SecurityHubTagResult(
    string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for AWS Security Hub.
/// </summary>
public static class SecurityHubService
{
    private static AmazonSecurityHubClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSecurityHubClient>(region);

    // ── Hub ─────────────────────────────────────────────────────────

    /// <summary>
    /// Enable Security Hub.
    /// </summary>
    public static async Task<string> EnableSecurityHubAsync(
        bool? enableDefaultStandards = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableSecurityHubRequest();
        if (enableDefaultStandards.HasValue)
            request.EnableDefaultStandards = enableDefaultStandards.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            await client.EnableSecurityHubAsync(request);
            return "Security Hub enabled";
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to enable Security Hub");
        }
    }

    /// <summary>
    /// Disable Security Hub.
    /// </summary>
    public static async Task DisableSecurityHubAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableSecurityHubAsync(
                new DisableSecurityHubRequest());
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disable Security Hub");
        }
    }

    /// <summary>
    /// Describe the Security Hub configuration.
    /// </summary>
    public static async Task<SecurityHubResult> DescribeHubAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeHubAsync(
                new DescribeHubRequest());
            return new SecurityHubResult(
                HubArn: resp.HubArn,
                SubscribedAt: resp.SubscribedAt,
                AutoEnableControls: resp.AutoEnableControls);
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Security Hub");
        }
    }

    // ── Findings ────────────────────────────────────────────────────

    /// <summary>
    /// Get findings matching filter criteria.
    /// </summary>
    public static async Task<List<SecurityHubFindingResult>> GetFindingsAsync(
        AwsSecurityFindingFilters? filters = null,
        List<SortCriterion>? sortCriteria = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubFindingResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new GetFindingsRequest();
                if (filters != null) request.Filters = filters;
                if (sortCriteria != null) request.SortCriteria = sortCriteria;
                if (maxResults.HasValue) request.MaxResults = maxResults.Value;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.GetFindingsAsync(request);
                foreach (var f in resp.Findings)
                {
                    results.Add(new SecurityHubFindingResult(
                        Id: f.Id, ProductArn: f.ProductArn,
                        GeneratorId: f.GeneratorId,
                        AwsAccountId: f.AwsAccountId,
                        Title: f.Title, Description: f.Description,
                        SeverityLabel: f.Severity?.Label?.Value,
                        WorkflowStatus: f.Workflow?.Status?.Value,
                        RecordState: f.RecordState?.Value,
                        CreatedAt: f.CreatedAt,
                        UpdatedAt: f.UpdatedAt));
                }
                nextToken = resp.NextToken;
                if (maxResults.HasValue) break;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get findings");
        }
    }

    /// <summary>
    /// Batch import findings.
    /// </summary>
    public static async Task<SecurityHubBatchImportResult> BatchImportFindingsAsync(
        List<AwsSecurityFinding> findings,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchImportFindingsAsync(
                new BatchImportFindingsRequest { Findings = findings });
            return new SecurityHubBatchImportResult(
                FailedCount: resp.FailedCount,
                SuccessCount: resp.SuccessCount,
                FailedFindings: resp.FailedFindings?
                    .Select(f => f.ErrorMessage).ToList());
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch import findings");
        }
    }

    /// <summary>
    /// Batch update findings.
    /// </summary>
    public static async Task<SecurityHubBatchUpdateResult> BatchUpdateFindingsAsync(
        List<AwsSecurityFindingIdentifier> findingIdentifiers,
        NoteUpdate? note = null,
        SeverityUpdate? severity = null,
        WorkflowUpdate? workflow = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchUpdateFindingsRequest
        {
            FindingIdentifiers = findingIdentifiers
        };
        if (note != null) request.Note = note;
        if (severity != null) request.Severity = severity;
        if (workflow != null) request.Workflow = workflow;

        try
        {
            var resp = await client.BatchUpdateFindingsAsync(request);
            return new SecurityHubBatchUpdateResult(
                ProcessedFindings: resp.ProcessedFindings?
                    .Select(f => f.Id).ToList(),
                UnprocessedFindings: resp.UnprocessedFindings?
                    .Select(f => f.FindingIdentifier?.Id ?? "").ToList());
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch update findings");
        }
    }

    // ── Insights ────────────────────────────────────────────────────

    /// <summary>
    /// Get insights.
    /// </summary>
    public static async Task<List<SecurityHubInsightResult>> GetInsightsAsync(
        List<string>? insightArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubInsightResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new GetInsightsRequest();
                if (insightArns != null) request.InsightArns = insightArns;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.GetInsightsAsync(request);
                foreach (var i in resp.Insights)
                {
                    results.Add(new SecurityHubInsightResult(
                        InsightArn: i.InsightArn, Name: i.Name,
                        GroupByAttribute: i.GroupByAttribute));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get insights");
        }
    }

    /// <summary>
    /// Get insight results.
    /// </summary>
    public static async Task<SecurityHubInsightResultsResult> GetInsightResultsAsync(
        string insightArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInsightResultsAsync(
                new GetInsightResultsRequest { InsightArn = insightArn });
            var ir = resp.InsightResults;
            return new SecurityHubInsightResultsResult(
                InsightArn: ir.InsightArn,
                ResultValues: ir.ResultValues?.Select(r =>
                    new Dictionary<string, string>
                    {
                        ["GroupByAttributeValue"] = r.GroupByAttributeValue ?? "",
                        ["Count"] = r.Count.ToString()
                    }).ToList());
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get insight results for '{insightArn}'");
        }
    }

    /// <summary>
    /// Create a custom insight.
    /// </summary>
    public static async Task<string> CreateInsightAsync(
        string name,
        AwsSecurityFindingFilters filters,
        string groupByAttribute,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateInsightAsync(
                new CreateInsightRequest
                {
                    Name = name,
                    Filters = filters,
                    GroupByAttribute = groupByAttribute
                });
            return resp.InsightArn;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create insight '{name}'");
        }
    }

    /// <summary>
    /// Delete a custom insight.
    /// </summary>
    public static async Task<string> DeleteInsightAsync(
        string insightArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteInsightAsync(
                new DeleteInsightRequest { InsightArn = insightArn });
            return resp.InsightArn;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete insight '{insightArn}'");
        }
    }

    /// <summary>
    /// Update a custom insight.
    /// </summary>
    public static async Task UpdateInsightAsync(
        string insightArn,
        string? name = null,
        AwsSecurityFindingFilters? filters = null,
        string? groupByAttribute = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateInsightRequest { InsightArn = insightArn };
        if (name != null) request.Name = name;
        if (filters != null) request.Filters = filters;
        if (groupByAttribute != null) request.GroupByAttribute = groupByAttribute;

        try
        {
            await client.UpdateInsightAsync(request);
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update insight '{insightArn}'");
        }
    }

    // ── Product subscriptions ───────────────────────────────────────

    /// <summary>
    /// Enable findings import for a product.
    /// </summary>
    public static async Task<SecurityHubProductSubscriptionResult> EnableImportFindingsForProductAsync(
        string productArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.EnableImportFindingsForProductAsync(
                new EnableImportFindingsForProductRequest
                {
                    ProductArn = productArn
                });
            return new SecurityHubProductSubscriptionResult(
                ProductSubscriptionArn: resp.ProductSubscriptionArn);
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable import for product '{productArn}'");
        }
    }

    /// <summary>
    /// Disable findings import for a product.
    /// </summary>
    public static async Task DisableImportFindingsForProductAsync(
        string productSubscriptionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableImportFindingsForProductAsync(
                new DisableImportFindingsForProductRequest
                {
                    ProductSubscriptionArn = productSubscriptionArn
                });
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable import for product '{productSubscriptionArn}'");
        }
    }

    /// <summary>
    /// List products enabled for import.
    /// </summary>
    public static async Task<List<string>> ListEnabledProductsForImportAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<string>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListEnabledProductsForImportRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListEnabledProductsForImportAsync(request);
                results.AddRange(resp.ProductSubscriptions);
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list enabled products for import");
        }
    }

    /// <summary>
    /// Describe available products.
    /// </summary>
    public static async Task<List<SecurityHubProductResult>> DescribeProductsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubProductResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new DescribeProductsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.DescribeProductsAsync(request);
                foreach (var p in resp.Products)
                {
                    results.Add(new SecurityHubProductResult(
                        ProductArn: p.ProductArn,
                        ProductName: p.ProductName,
                        CompanyName: p.CompanyName,
                        Description: p.Description,
                        MarketplaceUrl: p.MarketplaceUrl));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe products");
        }
    }

    // ── Standards ────────────────────────────────────────────────────

    /// <summary>
    /// Batch enable standards.
    /// </summary>
    public static async Task<List<SecurityHubStandardsSubscriptionResult>> BatchEnableStandardsAsync(
        List<StandardsSubscriptionRequest> standardsSubscriptionRequests,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchEnableStandardsAsync(
                new BatchEnableStandardsRequest
                {
                    StandardsSubscriptionRequests = standardsSubscriptionRequests
                });
            return resp.StandardsSubscriptions.Select(s =>
                new SecurityHubStandardsSubscriptionResult(
                    StandardsSubscriptionArn: s.StandardsSubscriptionArn,
                    StandardsArn: s.StandardsArn,
                    StandardsStatus: s.StandardsStatus?.Value)).ToList();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch enable standards");
        }
    }

    /// <summary>
    /// Batch disable standards.
    /// </summary>
    public static async Task<List<SecurityHubStandardsSubscriptionResult>> BatchDisableStandardsAsync(
        List<string> standardsSubscriptionArns,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchDisableStandardsAsync(
                new BatchDisableStandardsRequest
                {
                    StandardsSubscriptionArns = standardsSubscriptionArns
                });
            return resp.StandardsSubscriptions.Select(s =>
                new SecurityHubStandardsSubscriptionResult(
                    StandardsSubscriptionArn: s.StandardsSubscriptionArn,
                    StandardsArn: s.StandardsArn,
                    StandardsStatus: s.StandardsStatus?.Value)).ToList();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch disable standards");
        }
    }

    /// <summary>
    /// Get enabled standards subscriptions.
    /// </summary>
    public static async Task<List<SecurityHubStandardsSubscriptionResult>> GetEnabledStandardsAsync(
        List<string>? standardsSubscriptionArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubStandardsSubscriptionResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new GetEnabledStandardsRequest();
                if (standardsSubscriptionArns != null)
                    request.StandardsSubscriptionArns = standardsSubscriptionArns;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.GetEnabledStandardsAsync(request);
                foreach (var s in resp.StandardsSubscriptions)
                {
                    results.Add(new SecurityHubStandardsSubscriptionResult(
                        StandardsSubscriptionArn: s.StandardsSubscriptionArn,
                        StandardsArn: s.StandardsArn,
                        StandardsStatus: s.StandardsStatus?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get enabled standards");
        }
    }

    /// <summary>
    /// Describe available standards.
    /// </summary>
    public static async Task<List<SecurityHubStandardResult>> DescribeStandardsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubStandardResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new DescribeStandardsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.DescribeStandardsAsync(request);
                foreach (var s in resp.Standards)
                {
                    results.Add(new SecurityHubStandardResult(
                        StandardsArn: s.StandardsArn,
                        Name: s.Name,
                        Description: s.Description,
                        EnabledByDefault: s.EnabledByDefault));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe standards");
        }
    }

    /// <summary>
    /// Describe controls for a standard.
    /// </summary>
    public static async Task<List<SecurityHubStandardsControlResult>> DescribeStandardsControlsAsync(
        string standardsSubscriptionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubStandardsControlResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new DescribeStandardsControlsRequest
                {
                    StandardsSubscriptionArn = standardsSubscriptionArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.DescribeStandardsControlsAsync(request);
                foreach (var c in resp.Controls)
                {
                    results.Add(new SecurityHubStandardsControlResult(
                        StandardsControlArn: c.StandardsControlArn,
                        ControlId: c.ControlId,
                        ControlStatus: c.ControlStatus?.Value,
                        Title: c.Title, Description: c.Description,
                        SeverityRating: c.SeverityRating?.Value,
                        RemediationUrl: c.RemediationUrl));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe standards controls for '{standardsSubscriptionArn}'");
        }
    }

    /// <summary>
    /// Update a standards control.
    /// </summary>
    public static async Task UpdateStandardsControlAsync(
        string standardsControlArn,
        string? controlStatus = null,
        string? disabledReason = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateStandardsControlRequest
        {
            StandardsControlArn = standardsControlArn
        };
        if (controlStatus != null)
            request.ControlStatus = new ControlStatus(controlStatus);
        if (disabledReason != null) request.DisabledReason = disabledReason;

        try
        {
            await client.UpdateStandardsControlAsync(request);
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update standards control '{standardsControlArn}'");
        }
    }

    // ── Members ─────────────────────────────────────────────────────

    /// <summary>
    /// Create member accounts.
    /// </summary>
    public static async Task<List<string>> CreateMembersAsync(
        List<AccountDetails> accountDetails,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateMembersAsync(
                new CreateMembersRequest { AccountDetails = accountDetails });
            return resp.UnprocessedAccounts?
                .Select(u => u.AccountId).ToList() ?? new List<string>();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create members");
        }
    }

    /// <summary>
    /// Delete member accounts.
    /// </summary>
    public static async Task<List<string>> DeleteMembersAsync(
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteMembersAsync(
                new DeleteMembersRequest { AccountIds = accountIds });
            return resp.UnprocessedAccounts?
                .Select(u => u.AccountId).ToList() ?? new List<string>();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete members");
        }
    }

    /// <summary>
    /// Get member account details.
    /// </summary>
    public static async Task<List<SecurityHubMemberResult>> GetMembersAsync(
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMembersAsync(
                new GetMembersRequest { AccountIds = accountIds });
            return resp.Members.Select(m => new SecurityHubMemberResult(
                AccountId: m.AccountId, Email: m.Email,
                MasterId: m.MasterId,
                MemberStatus: m.MemberStatus,
                InvitedAt: m.InvitedAt?.ToString(),
                UpdatedAt: m.UpdatedAt?.ToString())).ToList();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get members");
        }
    }

    /// <summary>
    /// List member accounts.
    /// </summary>
    public static async Task<List<SecurityHubMemberResult>> ListMembersAsync(
        bool? onlyAssociated = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubMemberResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListMembersRequest();
                if (onlyAssociated.HasValue)
                    request.OnlyAssociated = onlyAssociated.Value;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListMembersAsync(request);
                foreach (var m in resp.Members)
                {
                    results.Add(new SecurityHubMemberResult(
                        AccountId: m.AccountId, Email: m.Email,
                        MasterId: m.MasterId,
                        MemberStatus: m.MemberStatus,
                        InvitedAt: m.InvitedAt?.ToString(),
                        UpdatedAt: m.UpdatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list members");
        }
    }

    /// <summary>
    /// Invite member accounts.
    /// </summary>
    public static async Task<List<string>> InviteMembersAsync(
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.InviteMembersAsync(
                new InviteMembersRequest { AccountIds = accountIds });
            return resp.UnprocessedAccounts?
                .Select(u => u.AccountId).ToList() ?? new List<string>();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to invite members");
        }
    }

    /// <summary>
    /// Accept an invitation from the administrator account.
    /// </summary>
    public static async Task AcceptInvitationAsync(
        string masterId,
        string invitationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AcceptInvitationAsync(
                new AcceptInvitationRequest
                {
                    MasterId = masterId,
                    InvitationId = invitationId
                });
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to accept invitation");
        }
    }

    /// <summary>
    /// Decline invitations.
    /// </summary>
    public static async Task<List<string>> DeclineInvitationsAsync(
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeclineInvitationsAsync(
                new DeclineInvitationsRequest { AccountIds = accountIds });
            return resp.UnprocessedAccounts?
                .Select(u => u.AccountId).ToList() ?? new List<string>();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to decline invitations");
        }
    }

    // ── Action targets ──────────────────────────────────────────────

    /// <summary>
    /// Create a custom action target.
    /// </summary>
    public static async Task<string> CreateActionTargetAsync(
        string name,
        string description,
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateActionTargetAsync(
                new CreateActionTargetRequest
                {
                    Name = name,
                    Description = description,
                    Id = id
                });
            return resp.ActionTargetArn;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create action target '{name}'");
        }
    }

    /// <summary>
    /// Delete a custom action target.
    /// </summary>
    public static async Task<string> DeleteActionTargetAsync(
        string actionTargetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteActionTargetAsync(
                new DeleteActionTargetRequest
                {
                    ActionTargetArn = actionTargetArn
                });
            return resp.ActionTargetArn;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete action target '{actionTargetArn}'");
        }
    }

    /// <summary>
    /// Describe action targets.
    /// </summary>
    public static async Task<List<SecurityHubActionTargetResult>> DescribeActionTargetsAsync(
        List<string>? actionTargetArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<SecurityHubActionTargetResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new DescribeActionTargetsRequest();
                if (actionTargetArns != null)
                    request.ActionTargetArns = actionTargetArns;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.DescribeActionTargetsAsync(request);
                foreach (var a in resp.ActionTargets)
                {
                    results.Add(new SecurityHubActionTargetResult(
                        ActionTargetArn: a.ActionTargetArn,
                        Name: a.Name,
                        Description: a.Description));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe action targets");
        }
    }

    /// <summary>
    /// Update a custom action target.
    /// </summary>
    public static async Task UpdateActionTargetAsync(
        string actionTargetArn,
        string? name = null,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateActionTargetRequest
        {
            ActionTargetArn = actionTargetArn
        };
        if (name != null) request.Name = name;
        if (description != null) request.Description = description;

        try
        {
            await client.UpdateActionTargetAsync(request);
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update action target '{actionTargetArn}'");
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
        catch (AmazonSecurityHubException exc)
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
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<SecurityHubTagResult>> ListTagsForResourceAsync(
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
                .Select(t => new SecurityHubTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonSecurityHubException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}
