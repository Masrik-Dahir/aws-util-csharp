using Amazon;
using Amazon.Macie2;
using Amazon.Macie2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record MacieSessionResult(
    string? CreatedAt = null, string? FindingPublishingFrequency = null,
    string? ServiceRole = null, string? Status = null,
    string? UpdatedAt = null);

public sealed record MacieClassificationJobResult(
    string? JobId = null, string? Name = null,
    string? JobType = null, string? JobStatus = null,
    string? CreatedAt = null, string? LastRunTime = null);

public sealed record MacieFindingResult(
    string? Id = null, string? Type = null,
    string? Severity = null, string? Title = null,
    string? Description = null, string? AccountId = null,
    string? Region = null, string? CreatedAt = null,
    string? UpdatedAt = null, bool? Archived = null);

public sealed record MacieFindingStatisticsResult(
    Dictionary<string, int>? CountBySeverity = null);

public sealed record MacieFindingsFilterResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Action = null,
    string? Description = null, int? Position = null);

public sealed record MacieCustomDataIdentifierResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Description = null,
    string? CreatedAt = null);

public sealed record MacieAllowListResult(
    string? Id = null, string? Arn = null,
    string? Name = null, string? Description = null,
    string? Status = null, string? CreatedAt = null,
    string? UpdatedAt = null);

public sealed record MacieMemberResult(
    string? AccountId = null, string? Email = null,
    string? MasterId = null, string? RelationshipStatus = null,
    string? InvitedAt = null, string? UpdatedAt = null,
    string? Arn = null);

public sealed record MacieBucketStatisticsResult(
    long? BucketCount = null, long? ClassifiableObjectCount = null,
    long? ClassifiableSizeInBytes = null,
    long? UnclassifiableObjectCount = null);

public sealed record MacieBucketResult(
    string? AccountId = null, string? BucketName = null,
    string? BucketArn = null, string? Region = null,
    long? ObjectCount = null, long? SizeInBytes = null,
    string? BucketCreatedAt = null);

public sealed record MacieTagResult(
    string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for Amazon Macie v2.
/// </summary>
public static class MacieService
{
    private static AmazonMacie2Client GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonMacie2Client>(region);

    // ── Session ─────────────────────────────────────────────────────

    /// <summary>
    /// Enable Amazon Macie.
    /// </summary>
    public static async Task EnableMacieAsync(
        string? findingPublishingFrequency = null,
        string? clientToken = null,
        string? status = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableMacieRequest();
        if (findingPublishingFrequency != null)
            request.FindingPublishingFrequency =
                new FindingPublishingFrequency(findingPublishingFrequency);
        if (clientToken != null) request.ClientToken = clientToken;
        if (status != null) request.Status = new MacieStatus(status);

        try
        {
            await client.EnableMacieAsync(request);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to enable Macie");
        }
    }

    /// <summary>
    /// Disable Amazon Macie.
    /// </summary>
    public static async Task DisableMacieAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableMacieAsync(new DisableMacieRequest());
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to disable Macie");
        }
    }

    /// <summary>
    /// Get the Macie session.
    /// </summary>
    public static async Task<MacieSessionResult> GetMacieSessionAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMacieSessionAsync(
                new GetMacieSessionRequest());
            return new MacieSessionResult(
                CreatedAt: resp.CreatedAt?.ToString(),
                FindingPublishingFrequency: resp.FindingPublishingFrequency?.Value,
                ServiceRole: resp.ServiceRole,
                Status: resp.Status?.Value,
                UpdatedAt: resp.UpdatedAt?.ToString());
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get Macie session");
        }
    }

    /// <summary>
    /// Update the Macie session.
    /// </summary>
    public static async Task UpdateMacieSessionAsync(
        string? findingPublishingFrequency = null,
        string? status = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateMacieSessionRequest();
        if (findingPublishingFrequency != null)
            request.FindingPublishingFrequency =
                new FindingPublishingFrequency(findingPublishingFrequency);
        if (status != null) request.Status = new MacieStatus(status);

        try
        {
            await client.UpdateMacieSessionAsync(request);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Macie session");
        }
    }

    // ── Classification jobs ─────────────────────────────────────────

    /// <summary>
    /// Create a classification job.
    /// </summary>
    public static async Task<MacieClassificationJobResult> CreateClassificationJobAsync(
        string name,
        string jobType,
        S3JobDefinition s3JobDefinition,
        string? clientToken = null,
        string? description = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateClassificationJobRequest
        {
            Name = name,
            JobType = new JobType(jobType),
            S3JobDefinition = s3JobDefinition
        };
        if (clientToken != null) request.ClientToken = clientToken;
        else request.ClientToken = Guid.NewGuid().ToString();
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateClassificationJobAsync(request);
            return new MacieClassificationJobResult(
                JobId: resp.JobId);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create classification job '{name}'");
        }
    }

    /// <summary>
    /// Describe a classification job.
    /// </summary>
    public static async Task<MacieClassificationJobResult> DescribeClassificationJobAsync(
        string jobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClassificationJobAsync(
                new DescribeClassificationJobRequest { JobId = jobId });
            return new MacieClassificationJobResult(
                JobId: resp.JobId, Name: resp.Name,
                JobType: resp.JobType?.Value,
                JobStatus: resp.JobStatus?.Value,
                CreatedAt: resp.CreatedAt?.ToString(),
                LastRunTime: resp.LastRunTime?.ToString());
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe classification job '{jobId}'");
        }
    }

    /// <summary>
    /// List classification jobs.
    /// </summary>
    public static async Task<List<MacieClassificationJobResult>> ListClassificationJobsAsync(
        ListJobsFilterCriteria? filterCriteria = null,
        ListJobsSortCriteria? sortCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieClassificationJobResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListClassificationJobsRequest();
                if (filterCriteria != null) request.FilterCriteria = filterCriteria;
                if (sortCriteria != null) request.SortCriteria = sortCriteria;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListClassificationJobsAsync(request);
                foreach (var j in resp.Items)
                {
                    results.Add(new MacieClassificationJobResult(
                        JobId: j.JobId, Name: j.Name,
                        JobType: j.JobType?.Value,
                        JobStatus: j.JobStatus?.Value,
                        CreatedAt: j.CreatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list classification jobs");
        }
    }

    /// <summary>
    /// Update a classification job.
    /// </summary>
    public static async Task UpdateClassificationJobAsync(
        string jobId,
        string jobStatus,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateClassificationJobAsync(
                new UpdateClassificationJobRequest
                {
                    JobId = jobId,
                    JobStatus = new JobStatus(jobStatus)
                });
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update classification job '{jobId}'");
        }
    }

    // ── Findings ────────────────────────────────────────────────────

    /// <summary>
    /// Get findings by IDs.
    /// </summary>
    public static async Task<List<MacieFindingResult>> GetFindingsAsync(
        List<string> findingIds,
        SortCriteria? sortCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFindingsRequest { FindingIds = findingIds };
        if (sortCriteria != null) request.SortCriteria = sortCriteria;

        try
        {
            var resp = await client.GetFindingsAsync(request);
            return resp.Findings.Select(f => new MacieFindingResult(
                Id: f.Id, Type: f.Type?.Value,
                Severity: f.Severity?.Description?.Value,
                Title: f.Title, Description: f.Description,
                AccountId: f.AccountId, Region: f.Region,
                CreatedAt: f.CreatedAt?.ToString(),
                UpdatedAt: f.UpdatedAt?.ToString(),
                Archived: f.Archived)).ToList();
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get findings");
        }
    }

    /// <summary>
    /// Get finding statistics.
    /// </summary>
    public static async Task<MacieFindingStatisticsResult> GetFindingStatisticsAsync(
        string groupBy,
        FindingCriteria? findingCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFindingStatisticsRequest
        {
            GroupBy = new GroupBy(groupBy)
        };
        if (findingCriteria != null) request.FindingCriteria = findingCriteria;

        try
        {
            var resp = await client.GetFindingStatisticsAsync(request);
            var counts = resp.CountsByGroup?
                .ToDictionary(c => c.GroupKey ?? "", c => (int)(c.Count));
            return new MacieFindingStatisticsResult(CountBySeverity: counts);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get finding statistics");
        }
    }

    /// <summary>
    /// List finding IDs.
    /// </summary>
    public static async Task<List<string>> ListFindingsAsync(
        FindingCriteria? findingCriteria = null,
        SortCriteria? sortCriteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<string>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListFindingsRequest();
                if (findingCriteria != null) request.FindingCriteria = findingCriteria;
                if (sortCriteria != null) request.SortCriteria = sortCriteria;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListFindingsAsync(request);
                results.AddRange(resp.FindingIds);
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list findings");
        }
    }

    // ── Findings filters ────────────────────────────────────────────

    /// <summary>
    /// Create a findings filter.
    /// </summary>
    public static async Task<MacieFindingsFilterResult> CreateFindingsFilterAsync(
        string name,
        string action,
        FindingCriteria findingCriteria,
        string? clientToken = null,
        string? description = null,
        int? position = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFindingsFilterRequest
        {
            Name = name,
            Action = new FindingsFilterAction(action),
            FindingCriteria = findingCriteria
        };
        if (clientToken != null) request.ClientToken = clientToken;
        if (description != null) request.Description = description;
        if (position.HasValue) request.Position = position.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateFindingsFilterAsync(request);
            return new MacieFindingsFilterResult(
                Id: resp.Id, Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create findings filter '{name}'");
        }
    }

    /// <summary>
    /// Delete a findings filter.
    /// </summary>
    public static async Task DeleteFindingsFilterAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFindingsFilterAsync(
                new DeleteFindingsFilterRequest { Id = id });
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete findings filter '{id}'");
        }
    }

    /// <summary>
    /// Get a findings filter.
    /// </summary>
    public static async Task<MacieFindingsFilterResult> GetFindingsFilterAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFindingsFilterAsync(
                new GetFindingsFilterRequest { Id = id });
            return new MacieFindingsFilterResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Action: resp.Action?.Value,
                Description: resp.Description,
                Position: resp.Position);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get findings filter '{id}'");
        }
    }

    /// <summary>
    /// List findings filters.
    /// </summary>
    public static async Task<List<MacieFindingsFilterResult>> ListFindingsFiltersAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieFindingsFilterResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListFindingsFiltersRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListFindingsFiltersAsync(request);
                foreach (var f in resp.FindingsFilterListItems)
                {
                    results.Add(new MacieFindingsFilterResult(
                        Id: f.Id, Arn: f.Arn, Name: f.Name,
                        Action: f.Action?.Value));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list findings filters");
        }
    }

    /// <summary>
    /// Update a findings filter.
    /// </summary>
    public static async Task<MacieFindingsFilterResult> UpdateFindingsFilterAsync(
        string id,
        string? name = null,
        string? action = null,
        FindingCriteria? findingCriteria = null,
        string? description = null,
        int? position = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFindingsFilterRequest { Id = id };
        if (name != null) request.Name = name;
        if (action != null) request.Action = new FindingsFilterAction(action);
        if (findingCriteria != null) request.FindingCriteria = findingCriteria;
        if (description != null) request.Description = description;
        if (position.HasValue) request.Position = position.Value;

        try
        {
            var resp = await client.UpdateFindingsFilterAsync(request);
            return new MacieFindingsFilterResult(
                Id: resp.Id, Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update findings filter '{id}'");
        }
    }

    // ── Custom data identifiers ─────────────────────────────────────

    /// <summary>
    /// Create a custom data identifier.
    /// </summary>
    public static async Task<MacieCustomDataIdentifierResult> CreateCustomDataIdentifierAsync(
        string name,
        string regex,
        string? clientToken = null,
        string? description = null,
        List<string>? keywords = null,
        int? maximumMatchDistance = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCustomDataIdentifierRequest
        {
            Name = name,
            Regex = regex
        };
        if (clientToken != null) request.ClientToken = clientToken;
        if (description != null) request.Description = description;
        if (keywords != null) request.Keywords = keywords;
        if (maximumMatchDistance.HasValue)
            request.MaximumMatchDistance = maximumMatchDistance.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCustomDataIdentifierAsync(request);
            return new MacieCustomDataIdentifierResult(
                Id: resp.CustomDataIdentifierId);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create custom data identifier '{name}'");
        }
    }

    /// <summary>
    /// Delete a custom data identifier.
    /// </summary>
    public static async Task DeleteCustomDataIdentifierAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCustomDataIdentifierAsync(
                new DeleteCustomDataIdentifierRequest { Id = id });
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete custom data identifier '{id}'");
        }
    }

    /// <summary>
    /// Get a custom data identifier.
    /// </summary>
    public static async Task<MacieCustomDataIdentifierResult> GetCustomDataIdentifierAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCustomDataIdentifierAsync(
                new GetCustomDataIdentifierRequest { Id = id });
            return new MacieCustomDataIdentifierResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Description: resp.Description,
                CreatedAt: resp.CreatedAt?.ToString());
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get custom data identifier '{id}'");
        }
    }

    /// <summary>
    /// List custom data identifiers.
    /// </summary>
    public static async Task<List<MacieCustomDataIdentifierResult>> ListCustomDataIdentifiersAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieCustomDataIdentifierResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListCustomDataIdentifiersRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListCustomDataIdentifiersAsync(request);
                foreach (var c in resp.Items)
                {
                    results.Add(new MacieCustomDataIdentifierResult(
                        Id: c.Id, Arn: c.Arn, Name: c.Name,
                        Description: c.Description,
                        CreatedAt: c.CreatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list custom data identifiers");
        }
    }

    // ── Allow lists ─────────────────────────────────────────────────

    /// <summary>
    /// Create an allow list.
    /// </summary>
    public static async Task<MacieAllowListResult> CreateAllowListAsync(
        string name,
        AllowListCriteria criteria,
        string? clientToken = null,
        string? description = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAllowListRequest
        {
            Name = name,
            Criteria = criteria
        };
        if (clientToken != null) request.ClientToken = clientToken;
        else request.ClientToken = Guid.NewGuid().ToString();
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAllowListAsync(request);
            return new MacieAllowListResult(
                Id: resp.Id, Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create allow list '{name}'");
        }
    }

    /// <summary>
    /// Delete an allow list.
    /// </summary>
    public static async Task DeleteAllowListAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAllowListAsync(
                new DeleteAllowListRequest { Id = id });
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete allow list '{id}'");
        }
    }

    /// <summary>
    /// Get an allow list.
    /// </summary>
    public static async Task<MacieAllowListResult> GetAllowListAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAllowListAsync(
                new GetAllowListRequest { Id = id });
            return new MacieAllowListResult(
                Id: resp.Id, Arn: resp.Arn, Name: resp.Name,
                Description: resp.Description,
                Status: resp.Status?.Code?.Value,
                CreatedAt: resp.CreatedAt?.ToString(),
                UpdatedAt: resp.UpdatedAt?.ToString());
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get allow list '{id}'");
        }
    }

    /// <summary>
    /// List allow lists.
    /// </summary>
    public static async Task<List<MacieAllowListResult>> ListAllowListsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieAllowListResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAllowListsRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAllowListsAsync(request);
                foreach (var a in resp.AllowLists)
                {
                    results.Add(new MacieAllowListResult(
                        Id: a.Id, Arn: a.Arn, Name: a.Name,
                        Description: a.Description,
                        CreatedAt: a.CreatedAt?.ToString(),
                        UpdatedAt: a.UpdatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list allow lists");
        }
    }

    /// <summary>
    /// Update an allow list.
    /// </summary>
    public static async Task<MacieAllowListResult> UpdateAllowListAsync(
        string id,
        string name,
        AllowListCriteria criteria,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAllowListRequest
        {
            Id = id,
            Name = name,
            Criteria = criteria
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateAllowListAsync(request);
            return new MacieAllowListResult(
                Id: resp.Id, Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update allow list '{id}'");
        }
    }

    // ── Members ─────────────────────────────────────────────────────

    /// <summary>
    /// Create a member association.
    /// </summary>
    public static async Task<MacieMemberResult> CreateMemberAsync(
        string accountId,
        string email,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateMemberRequest
        {
            Account = new AccountDetail
            {
                AccountId = accountId,
                Email = email
            }
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateMemberAsync(request);
            return new MacieMemberResult(Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create member '{accountId}'");
        }
    }

    /// <summary>
    /// Delete a member association.
    /// </summary>
    public static async Task DeleteMemberAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteMemberAsync(
                new DeleteMemberRequest { Id = id });
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete member '{id}'");
        }
    }

    /// <summary>
    /// Get a member.
    /// </summary>
    public static async Task<MacieMemberResult> GetMemberAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMemberAsync(
                new GetMemberRequest { Id = id });
            return new MacieMemberResult(
                AccountId: resp.AccountId,
                Email: resp.Email,
                MasterId: resp.AdministratorAccountId,
                RelationshipStatus: resp.RelationshipStatus?.Value,
                InvitedAt: resp.InvitedAt?.ToString(),
                UpdatedAt: resp.UpdatedAt?.ToString(),
                Arn: resp.Arn);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get member '{id}'");
        }
    }

    /// <summary>
    /// List members.
    /// </summary>
    public static async Task<List<MacieMemberResult>> ListMembersAsync(
        string? onlyAssociated = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieMemberResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListMembersRequest();
                if (onlyAssociated != null) request.OnlyAssociated = onlyAssociated;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListMembersAsync(request);
                foreach (var m in resp.Members)
                {
                    results.Add(new MacieMemberResult(
                        AccountId: m.AccountId,
                        Email: m.Email,
                        MasterId: m.AdministratorAccountId,
                        RelationshipStatus: m.RelationshipStatus?.Value,
                        InvitedAt: m.InvitedAt?.ToString(),
                        UpdatedAt: m.UpdatedAt?.ToString(),
                        Arn: m.Arn));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list members");
        }
    }

    // ── Invitations ─────────────────────────────────────────────────

    /// <summary>
    /// Create invitations.
    /// </summary>
    public static async Task<List<string>> CreateInvitationsAsync(
        List<string> accountIds,
        bool? disableEmailNotification = null,
        string? message = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateInvitationsRequest
        {
            AccountIds = accountIds
        };
        if (disableEmailNotification.HasValue)
            request.DisableEmailNotification = disableEmailNotification.Value;
        if (message != null) request.Message = message;

        try
        {
            var resp = await client.CreateInvitationsAsync(request);
            return resp.UnprocessedAccounts?
                .Select(u => u.AccountId).ToList() ?? new List<string>();
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create invitations");
        }
    }

    /// <summary>
    /// Accept an invitation.
    /// </summary>
    public static async Task AcceptInvitationAsync(
        string invitationId,
        string administratorAccountId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AcceptInvitationAsync(
                new AcceptInvitationRequest
                {
                    InvitationId = invitationId,
                    AdministratorAccountId = administratorAccountId
                });
        }
        catch (AmazonMacie2Exception exc)
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
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to decline invitations");
        }
    }

    // ── Bucket statistics ───────────────────────────────────────────

    /// <summary>
    /// Get bucket statistics.
    /// </summary>
    public static async Task<MacieBucketStatisticsResult> GetBucketStatisticsAsync(
        string? accountId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBucketStatisticsRequest();
        if (accountId != null) request.AccountId = accountId;

        try
        {
            var resp = await client.GetBucketStatisticsAsync(request);
            return new MacieBucketStatisticsResult(
                BucketCount: resp.BucketCount,
                ClassifiableObjectCount: resp.ClassifiableObjectCount,
                ClassifiableSizeInBytes: resp.ClassifiableSizeInBytes,
                UnclassifiableObjectCount: resp.UnclassifiableObjectCount?.Total);
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get bucket statistics");
        }
    }

    /// <summary>
    /// Describe S3 buckets.
    /// </summary>
    public static async Task<List<MacieBucketResult>> DescribeBucketsAsync(
        Dictionary<string, BucketCriteriaAdditionalProperties>? criteria = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<MacieBucketResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new DescribeBucketsRequest();
                if (criteria != null) request.Criteria = criteria;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.DescribeBucketsAsync(request);
                foreach (var b in resp.Buckets)
                {
                    results.Add(new MacieBucketResult(
                        AccountId: b.AccountId,
                        BucketName: b.BucketName,
                        BucketArn: b.BucketArn,
                        Region: b.Region,
                        ObjectCount: b.ObjectCount,
                        SizeInBytes: b.SizeInBytes,
                        BucketCreatedAt: b.BucketCreatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe buckets");
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
        catch (AmazonMacie2Exception exc)
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
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<MacieTagResult>> ListTagsForResourceAsync(
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
                .Select(t => new MacieTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonMacie2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}
