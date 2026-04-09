using Amazon;
using Amazon.Inspector2;
using Amazon.Inspector2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record InspectorAccountStatusResult(
    string? AccountId = null, string? ResourceState = null,
    string? Status = null);

public sealed record InspectorFindingResult(
    string? FindingArn = null, string? Type = null,
    string? Severity = null, string? Status = null,
    string? Title = null, string? Description = null,
    string? FirstObservedAt = null, string? LastObservedAt = null,
    string? UpdatedAt = null, double? InspectorScore = null);

public sealed record InspectorMemberResult(
    string? AccountId = null, string? DelegatedAdminAccountId = null,
    string? RelationshipStatus = null, string? UpdatedAt = null);

public sealed record InspectorFreeTrialResult(
    string? AccountId = null,
    List<Dictionary<string, string>>? FreeTrialInfo = null);

public sealed record InspectorCoverageResult(
    string? AccountId = null, string? ResourceId = null,
    string? ResourceType = null, string? ScanType = null,
    string? ScanStatus = null);

public sealed record InspectorCoverageStatisticsResult(
    long? TotalCounts = null,
    Dictionary<string, int>? CountsByGroup = null);

public sealed record InspectorFilterResult(
    string? Arn = null, string? Name = null,
    string? Description = null, string? Action = null,
    string? OwnerId = null, string? Reason = null,
    string? CreatedAt = null, string? UpdatedAt = null);

public sealed record InspectorUsageResult(
    string? AccountId = null, string? Type = null,
    double? Total = null, string? Currency = null);

public sealed record InspectorDelegatedAdminResult(
    string? AccountId = null, string? Status = null);

public sealed record InspectorOrgConfigResult(
    bool? AutoEnable = null);

public sealed record InspectorSbomExportResult(
    string? ReportId = null, string? Status = null,
    string? S3Destination = null, string? ErrorMessage = null);

public sealed record InspectorAccountPermissionResult(
    string? Service = null, string? Feature = null,
    string? Status = null);

public sealed record InspectorConfigurationResult(
    string? EcrConfiguration = null);

public sealed record InspectorFindingsReportResult(
    string? ReportId = null, string? Status = null,
    string? ErrorMessage = null, string? Destination = null);

public sealed record InspectorTagResult(string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for Amazon Inspector v2.
/// </summary>
public static class InspectorService
{
    private static AmazonInspector2Client GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonInspector2Client>(region);

    // ── Enable / Disable ────────────────────────────────────────────

    /// <summary>
    /// Enable Amazon Inspector for the specified resource types.
    /// </summary>
    public static async Task<List<InspectorAccountStatusResult>> EnableAsync(
        List<string> resourceTypes,
        List<string>? accountIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableRequest
        {
            ResourceTypes = resourceTypes
        };
        if (accountIds != null) request.AccountIds = accountIds;

        try
        {
            var resp = await client.EnableAsync(request);
            return resp.Accounts.Select(a => new InspectorAccountStatusResult(
                AccountId: a.AccountId,
                ResourceState: a.ResourceStatus?.ToString(),
                Status: a.Status?.Value)).ToList();
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to enable Inspector");
        }
    }

    /// <summary>
    /// Disable Amazon Inspector for the specified resource types.
    /// </summary>
    public static async Task<List<InspectorAccountStatusResult>> DisableAsync(
        List<string>? resourceTypes = null,
        List<string>? accountIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DisableRequest();
        if (resourceTypes != null)
            request.ResourceTypes = resourceTypes;
        if (accountIds != null) request.AccountIds = accountIds;

        try
        {
            var resp = await client.DisableAsync(request);
            return resp.Accounts.Select(a => new InspectorAccountStatusResult(
                AccountId: a.AccountId,
                ResourceState: a.ResourceStatus?.ToString(),
                Status: a.Status?.Value)).ToList();
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to disable Inspector");
        }
    }

    /// <summary>
    /// Batch get account status.
    /// </summary>
    public static async Task<List<InspectorAccountStatusResult>> BatchGetAccountStatusAsync(
        List<string>? accountIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchGetAccountStatusRequest();
        if (accountIds != null) request.AccountIds = accountIds;

        try
        {
            var resp = await client.BatchGetAccountStatusAsync(request);
            return resp.Accounts.Select(a => new InspectorAccountStatusResult(
                AccountId: a.AccountId,
                ResourceState: a.ResourceState?.ToString(),
                Status: a.State?.Status?.Value)).ToList();
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get account status");
        }
    }

    /// <summary>
    /// Get the status of a findings report.
    /// </summary>
    public static async Task<InspectorFindingsReportResult> GetFindingsReportStatusAsync(
        string? reportId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFindingsReportStatusRequest();
        if (reportId != null) request.ReportId = reportId;

        try
        {
            var resp = await client.GetFindingsReportStatusAsync(request);
            return new InspectorFindingsReportResult(
                ReportId: resp.ReportId,
                Status: resp.Status?.Value,
                ErrorMessage: resp.ErrorCode?.Value,
                Destination: resp.Destination?.BucketName);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get findings report status");
        }
    }

    // ── Findings ────────────────────────────────────────────────────

    /// <summary>
    /// List findings.
    /// </summary>
    public static async Task<List<InspectorFindingResult>> ListFindingsAsync(
        FilterCriteria? filterCriteria = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorFindingResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListFindingsRequest();
                if (filterCriteria != null) request.FilterCriteria = filterCriteria;
                if (maxResults.HasValue) request.MaxResults = maxResults.Value;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListFindingsAsync(request);
                foreach (var f in resp.Findings)
                {
                    results.Add(new InspectorFindingResult(
                        FindingArn: f.FindingArn, Type: f.Type?.Value,
                        Severity: f.Severity?.Value, Status: f.Status?.Value,
                        Title: f.Title, Description: f.Description,
                        FirstObservedAt: f.FirstObservedAt?.ToString(),
                        LastObservedAt: f.LastObservedAt?.ToString(),
                        UpdatedAt: f.UpdatedAt?.ToString(),
                        InspectorScore: f.InspectorScore));
                }
                nextToken = resp.NextToken;
                if (maxResults.HasValue) break;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list findings");
        }
    }

    // ── Members ─────────────────────────────────────────────────────

    /// <summary>
    /// Get a member account.
    /// </summary>
    public static async Task<InspectorMemberResult> GetMemberAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMemberAsync(
                new GetMemberRequest { AccountId = accountId });
            var m = resp.Member;
            return new InspectorMemberResult(
                AccountId: m.AccountId,
                DelegatedAdminAccountId: m.DelegatedAdminAccountId,
                RelationshipStatus: m.RelationshipStatus?.Value,
                UpdatedAt: m.UpdatedAt?.ToString());
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get member '{accountId}'");
        }
    }

    /// <summary>
    /// List member accounts.
    /// </summary>
    public static async Task<List<InspectorMemberResult>> ListMembersAsync(
        bool? onlyAssociated = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorMemberResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListMembersRequest();
                if (onlyAssociated.HasValue) request.OnlyAssociated = onlyAssociated.Value;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListMembersAsync(request);
                foreach (var m in resp.Members)
                {
                    results.Add(new InspectorMemberResult(
                        AccountId: m.AccountId,
                        DelegatedAdminAccountId: m.DelegatedAdminAccountId,
                        RelationshipStatus: m.RelationshipStatus?.Value,
                        UpdatedAt: m.UpdatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list members");
        }
    }

    /// <summary>
    /// Associate a member account.
    /// </summary>
    public static async Task<InspectorMemberResult> AssociateMemberAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AssociateMemberAsync(
                new AssociateMemberRequest { AccountId = accountId });
            return new InspectorMemberResult(AccountId: resp.AccountId);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate member '{accountId}'");
        }
    }

    /// <summary>
    /// Disassociate a member account.
    /// </summary>
    public static async Task<InspectorMemberResult> DisassociateMemberAsync(
        string accountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisassociateMemberAsync(
                new DisassociateMemberRequest { AccountId = accountId });
            return new InspectorMemberResult(AccountId: resp.AccountId);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate member '{accountId}'");
        }
    }

    // ── Free trial ──────────────────────────────────────────────────

    /// <summary>
    /// Batch get free trial info for accounts.
    /// </summary>
    public static async Task<List<InspectorFreeTrialResult>> BatchGetFreeTrialInfoAsync(
        List<string> accountIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetFreeTrialInfoAsync(
                new BatchGetFreeTrialInfoRequest { AccountIds = accountIds });
            return resp.Accounts.Select(a => new InspectorFreeTrialResult(
                AccountId: a.AccountId,
                FreeTrialInfo: a.FreeTrialInfo?.Select(f =>
                    new Dictionary<string, string>
                    {
                        ["Type"] = f.Type?.Value ?? "",
                        ["Start"] = f.Start?.ToString() ?? "",
                        ["End"] = f.End?.ToString() ?? "",
                        ["Status"] = f.Status?.Value ?? ""
                    }).ToList())).ToList();
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get free trial info");
        }
    }

    // ── Coverage ────────────────────────────────────────────────────

    /// <summary>
    /// List coverage for resources.
    /// </summary>
    public static async Task<List<InspectorCoverageResult>> ListCoverageAsync(
        CoverageFilterCriteria? filterCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorCoverageResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListCoverageRequest();
                if (filterCriteria != null) request.FilterCriteria = filterCriteria;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListCoverageAsync(request);
                foreach (var c in resp.CoveredResources)
                {
                    results.Add(new InspectorCoverageResult(
                        AccountId: c.AccountId,
                        ResourceId: c.ResourceId,
                        ResourceType: c.ResourceType?.Value,
                        ScanType: c.ScanType?.Value,
                        ScanStatus: c.ScanStatus?.StatusCode?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list coverage");
        }
    }

    /// <summary>
    /// List coverage statistics.
    /// </summary>
    public static async Task<InspectorCoverageStatisticsResult> ListCoverageStatisticsAsync(
        CoverageFilterCriteria? filterCriteria = null,
        string? groupBy = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCoverageStatisticsRequest();
        if (filterCriteria != null) request.FilterCriteria = filterCriteria;
        if (groupBy != null) request.GroupBy = new GroupKey(groupBy);

        try
        {
            var resp = await client.ListCoverageStatisticsAsync(request);
            var countsByGroup = resp.CountsByGroup?
                .ToDictionary(
                    c => c.GroupKey?.Value ?? "",
                    c => (int)(c.Count ?? 0));
            return new InspectorCoverageStatisticsResult(
                TotalCounts: resp.TotalCounts,
                CountsByGroup: countsByGroup);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list coverage statistics");
        }
    }

    // ── Filters ─────────────────────────────────────────────────────

    /// <summary>
    /// Create a filter.
    /// </summary>
    public static async Task<string> CreateFilterAsync(
        string name,
        string action,
        FilterCriteria filterCriteria,
        string? description = null,
        string? reason = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFilterRequest
        {
            Name = name,
            Action = new FilterAction(action),
            FilterCriteria = filterCriteria
        };
        if (description != null) request.Description = description;
        if (reason != null) request.Reason = reason;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateFilterAsync(request);
            return resp.Arn;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create filter '{name}'");
        }
    }

    /// <summary>
    /// Delete a filter.
    /// </summary>
    public static async Task DeleteFilterAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFilterAsync(
                new DeleteFilterRequest { Arn = arn });
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete filter '{arn}'");
        }
    }

    /// <summary>
    /// Get a filter by ARN.
    /// </summary>
    public static async Task<InspectorFilterResult> GetFilterAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListFiltersAsync(
                new ListFiltersRequest { Arns = new List<string> { arn } });
            var f = resp.Filters.FirstOrDefault();
            if (f == null)
                throw new AwsNotFoundException($"Filter '{arn}' not found");
            return new InspectorFilterResult(
                Arn: f.Arn, Name: f.Name,
                Description: f.Description,
                Action: f.Action?.Value,
                OwnerId: f.OwnerId, Reason: f.Reason,
                CreatedAt: f.CreatedAt?.ToString(),
                UpdatedAt: f.UpdatedAt?.ToString());
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get filter '{arn}'");
        }
    }

    /// <summary>
    /// List filters.
    /// </summary>
    public static async Task<List<InspectorFilterResult>> ListFiltersAsync(
        string? action = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorFilterResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListFiltersRequest();
                if (action != null) request.Action = new FilterAction(action);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListFiltersAsync(request);
                foreach (var f in resp.Filters)
                {
                    results.Add(new InspectorFilterResult(
                        Arn: f.Arn, Name: f.Name,
                        Description: f.Description,
                        Action: f.Action?.Value,
                        OwnerId: f.OwnerId, Reason: f.Reason,
                        CreatedAt: f.CreatedAt?.ToString(),
                        UpdatedAt: f.UpdatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list filters");
        }
    }

    /// <summary>
    /// Update a filter.
    /// </summary>
    public static async Task<string> UpdateFilterAsync(
        string filterArn,
        string? name = null,
        string? action = null,
        FilterCriteria? filterCriteria = null,
        string? description = null,
        string? reason = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFilterRequest { FilterArn = filterArn };
        if (name != null) request.Name = name;
        if (action != null) request.Action = new FilterAction(action);
        if (filterCriteria != null) request.FilterCriteria = filterCriteria;
        if (description != null) request.Description = description;
        if (reason != null) request.Reason = reason;

        try
        {
            var resp = await client.UpdateFilterAsync(request);
            return resp.Arn;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update filter '{filterArn}'");
        }
    }

    // ── Usage ───────────────────────────────────────────────────────

    /// <summary>
    /// List usage totals.
    /// </summary>
    public static async Task<List<InspectorUsageResult>> ListUsageTotalsAsync(
        List<string>? accountIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsageTotalsRequest();
        if (accountIds != null) request.AccountIds = accountIds;
        var results = new List<InspectorUsageResult>();
        string? nextToken = null;

        try
        {
            do
            {
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListUsageTotalsAsync(request);
                foreach (var u in resp.Totals)
                {
                    foreach (var usage in u.Usage)
                    {
                        results.Add(new InspectorUsageResult(
                            AccountId: u.AccountId,
                            Type: usage.Type?.Value,
                            Total: usage.Total,
                            Currency: usage.Currency?.Value));
                    }
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list usage totals");
        }
    }

    // ── Delegated admin ─────────────────────────────────────────────

    /// <summary>
    /// Enable a delegated admin account.
    /// </summary>
    public static async Task<InspectorDelegatedAdminResult> EnableDelegatedAdminAccountAsync(
        string delegatedAdminAccountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.EnableDelegatedAdminAccountAsync(
                new EnableDelegatedAdminAccountRequest
                {
                    DelegatedAdminAccountId = delegatedAdminAccountId
                });
            return new InspectorDelegatedAdminResult(
                AccountId: resp.DelegatedAdminAccountId);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable delegated admin '{delegatedAdminAccountId}'");
        }
    }

    /// <summary>
    /// Disable a delegated admin account.
    /// </summary>
    public static async Task<InspectorDelegatedAdminResult> DisableDelegatedAdminAccountAsync(
        string delegatedAdminAccountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisableDelegatedAdminAccountAsync(
                new DisableDelegatedAdminAccountRequest
                {
                    DelegatedAdminAccountId = delegatedAdminAccountId
                });
            return new InspectorDelegatedAdminResult(
                AccountId: resp.DelegatedAdminAccountId);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable delegated admin '{delegatedAdminAccountId}'");
        }
    }

    /// <summary>
    /// List delegated admin accounts.
    /// </summary>
    public static async Task<List<InspectorDelegatedAdminResult>> ListDelegatedAdminAccountsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorDelegatedAdminResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListDelegatedAdminAccountsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListDelegatedAdminAccountsAsync(request);
                foreach (var d in resp.DelegatedAdminAccounts)
                {
                    results.Add(new InspectorDelegatedAdminResult(
                        AccountId: d.AccountId,
                        Status: d.Status?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list delegated admin accounts");
        }
    }

    // ── Organization configuration ──────────────────────────────────

    /// <summary>
    /// Describe the organization configuration.
    /// </summary>
    public static async Task<InspectorOrgConfigResult> DescribeOrganizationConfigurationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeOrganizationConfigurationAsync(
                new DescribeOrganizationConfigurationRequest());
            return new InspectorOrgConfigResult(
                AutoEnable: resp.AutoEnable?.Ec2);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe organization configuration");
        }
    }

    /// <summary>
    /// Update the organization configuration.
    /// </summary>
    public static async Task UpdateOrganizationConfigurationAsync(
        AutoEnable autoEnable,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateOrganizationConfigurationAsync(
                new UpdateOrganizationConfigurationRequest
                {
                    AutoEnable = autoEnable
                });
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update organization configuration");
        }
    }

    // ── SBOM export ─────────────────────────────────────────────────

    /// <summary>
    /// Create an SBOM export.
    /// </summary>
    public static async Task<InspectorSbomExportResult> CreateSbomExportAsync(
        string reportFormat,
        Destination s3Destination,
        ResourceFilterCriteria? resourceFilterCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSbomExportRequest
        {
            ReportFormat = new SbomReportFormat(reportFormat),
            S3Destination = s3Destination
        };
        if (resourceFilterCriteria != null)
            request.ResourceFilterCriteria = resourceFilterCriteria;

        try
        {
            var resp = await client.CreateSbomExportAsync(request);
            return new InspectorSbomExportResult(ReportId: resp.ReportId);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create SBOM export");
        }
    }

    /// <summary>
    /// Get the status of an SBOM export.
    /// </summary>
    public static async Task<InspectorSbomExportResult> GetSbomExportAsync(
        string reportId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSbomExportAsync(
                new GetSbomExportRequest { ReportId = reportId });
            return new InspectorSbomExportResult(
                ReportId: resp.ReportId,
                Status: resp.Status?.Value,
                S3Destination: resp.S3Destination?.BucketName,
                ErrorMessage: resp.ErrorCode?.Value);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get SBOM export '{reportId}'");
        }
    }

    // ── Permissions ─────────────────────────────────────────────────

    /// <summary>
    /// List account permissions.
    /// </summary>
    public static async Task<List<InspectorAccountPermissionResult>> ListAccountPermissionsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<InspectorAccountPermissionResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccountPermissionsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccountPermissionsAsync(request);
                foreach (var p in resp.Permissions)
                {
                    results.Add(new InspectorAccountPermissionResult(
                        Service: p.Service?.Value,
                        Feature: p.Operation?.Value,
                        Status: null));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list account permissions");
        }
    }

    // ── Configuration ───────────────────────────────────────────────

    /// <summary>
    /// Update the Inspector configuration.
    /// </summary>
    public static async Task UpdateConfigurationAsync(
        EcrConfiguration ecrConfiguration,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateConfigurationAsync(
                new UpdateConfigurationRequest
                {
                    EcrConfiguration = ecrConfiguration
                });
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update configuration");
        }
    }

    /// <summary>
    /// Get the Inspector configuration.
    /// </summary>
    public static async Task<InspectorConfigurationResult> GetConfigurationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetConfigurationAsync(
                new GetConfigurationRequest());
            return new InspectorConfigurationResult(
                EcrConfiguration: resp.EcrConfiguration?.RescanDurationState?.RescanDuration?.Value);
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get configuration");
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
        catch (AmazonInspector2Exception exc)
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
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<InspectorTagResult>> ListTagsForResourceAsync(
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
                .Select(t => new InspectorTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonInspector2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="EnableAsync"/>.</summary>
    public static List<InspectorAccountStatusResult> Enable(List<string> resourceTypes, List<string>? accountIds = null, RegionEndpoint? region = null)
        => EnableAsync(resourceTypes, accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableAsync"/>.</summary>
    public static List<InspectorAccountStatusResult> Disable(List<string>? resourceTypes = null, List<string>? accountIds = null, RegionEndpoint? region = null)
        => DisableAsync(resourceTypes, accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetAccountStatusAsync"/>.</summary>
    public static List<InspectorAccountStatusResult> BatchGetAccountStatus(List<string>? accountIds = null, RegionEndpoint? region = null)
        => BatchGetAccountStatusAsync(accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFindingsReportStatusAsync"/>.</summary>
    public static InspectorFindingsReportResult GetFindingsReportStatus(string? reportId = null, RegionEndpoint? region = null)
        => GetFindingsReportStatusAsync(reportId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFindingsAsync"/>.</summary>
    public static List<InspectorFindingResult> ListFindings(FilterCriteria? filterCriteria = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListFindingsAsync(filterCriteria, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMemberAsync"/>.</summary>
    public static InspectorMemberResult GetMember(string accountId, RegionEndpoint? region = null)
        => GetMemberAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListMembersAsync"/>.</summary>
    public static List<InspectorMemberResult> ListMembers(bool? onlyAssociated = null, RegionEndpoint? region = null)
        => ListMembersAsync(onlyAssociated, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateMemberAsync"/>.</summary>
    public static InspectorMemberResult AssociateMember(string accountId, RegionEndpoint? region = null)
        => AssociateMemberAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateMemberAsync"/>.</summary>
    public static InspectorMemberResult DisassociateMember(string accountId, RegionEndpoint? region = null)
        => DisassociateMemberAsync(accountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetFreeTrialInfoAsync"/>.</summary>
    public static List<InspectorFreeTrialResult> BatchGetFreeTrialInfo(List<string> accountIds, RegionEndpoint? region = null)
        => BatchGetFreeTrialInfoAsync(accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCoverageAsync"/>.</summary>
    public static List<InspectorCoverageResult> ListCoverage(CoverageFilterCriteria? filterCriteria = null, RegionEndpoint? region = null)
        => ListCoverageAsync(filterCriteria, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCoverageStatisticsAsync"/>.</summary>
    public static InspectorCoverageStatisticsResult ListCoverageStatistics(CoverageFilterCriteria? filterCriteria = null, string? groupBy = null, RegionEndpoint? region = null)
        => ListCoverageStatisticsAsync(filterCriteria, groupBy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateFilterAsync"/>.</summary>
    public static string CreateFilter(string name, string action, FilterCriteria filterCriteria, string? description = null, string? reason = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateFilterAsync(name, action, filterCriteria, description, reason, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFilterAsync"/>.</summary>
    public static void DeleteFilter(string arn, RegionEndpoint? region = null)
        => DeleteFilterAsync(arn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFilterAsync"/>.</summary>
    public static InspectorFilterResult GetFilter(string arn, RegionEndpoint? region = null)
        => GetFilterAsync(arn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFiltersAsync"/>.</summary>
    public static List<InspectorFilterResult> ListFilters(string? action = null, RegionEndpoint? region = null)
        => ListFiltersAsync(action, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateFilterAsync"/>.</summary>
    public static string UpdateFilter(string filterArn, string? name = null, string? action = null, FilterCriteria? filterCriteria = null, string? description = null, string? reason = null, RegionEndpoint? region = null)
        => UpdateFilterAsync(filterArn, name, action, filterCriteria, description, reason, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUsageTotalsAsync"/>.</summary>
    public static List<InspectorUsageResult> ListUsageTotals(List<string>? accountIds = null, RegionEndpoint? region = null)
        => ListUsageTotalsAsync(accountIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableDelegatedAdminAccountAsync"/>.</summary>
    public static InspectorDelegatedAdminResult EnableDelegatedAdminAccount(string delegatedAdminAccountId, RegionEndpoint? region = null)
        => EnableDelegatedAdminAccountAsync(delegatedAdminAccountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableDelegatedAdminAccountAsync"/>.</summary>
    public static InspectorDelegatedAdminResult DisableDelegatedAdminAccount(string delegatedAdminAccountId, RegionEndpoint? region = null)
        => DisableDelegatedAdminAccountAsync(delegatedAdminAccountId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDelegatedAdminAccountsAsync"/>.</summary>
    public static List<InspectorDelegatedAdminResult> ListDelegatedAdminAccounts(RegionEndpoint? region = null)
        => ListDelegatedAdminAccountsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeOrganizationConfigurationAsync"/>.</summary>
    public static InspectorOrgConfigResult DescribeOrganizationConfiguration(RegionEndpoint? region = null)
        => DescribeOrganizationConfigurationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateOrganizationConfigurationAsync"/>.</summary>
    public static void UpdateOrganizationConfiguration(AutoEnable autoEnable, RegionEndpoint? region = null)
        => UpdateOrganizationConfigurationAsync(autoEnable, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSbomExportAsync"/>.</summary>
    public static InspectorSbomExportResult CreateSbomExport(string reportFormat, Destination s3Destination, ResourceFilterCriteria? resourceFilterCriteria = null, RegionEndpoint? region = null)
        => CreateSbomExportAsync(reportFormat, s3Destination, resourceFilterCriteria, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSbomExportAsync"/>.</summary>
    public static InspectorSbomExportResult GetSbomExport(string reportId, RegionEndpoint? region = null)
        => GetSbomExportAsync(reportId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccountPermissionsAsync"/>.</summary>
    public static List<InspectorAccountPermissionResult> ListAccountPermissions(RegionEndpoint? region = null)
        => ListAccountPermissionsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateConfigurationAsync"/>.</summary>
    public static void UpdateConfiguration(EcrConfiguration ecrConfiguration, RegionEndpoint? region = null)
        => UpdateConfigurationAsync(ecrConfiguration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetConfigurationAsync"/>.</summary>
    public static InspectorConfigurationResult GetConfiguration(RegionEndpoint? region = null)
        => GetConfigurationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static List<InspectorTagResult> ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
