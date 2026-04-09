using Amazon;
using Amazon.AccessAnalyzer;
using Amazon.AccessAnalyzer.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record AnalyzerResult(
    string? Arn = null, string? Name = null,
    string? Type = null, string? Status = null,
    string? CreatedAt = null, string? LastResourceAnalyzed = null,
    string? LastResourceAnalyzedAt = null);

public sealed record AnalyzerFindingResult(
    string? Id = null, string? ResourceType = null,
    string? Resource = null, string? Status = null,
    string? Principal = null, string? Action = null,
    string? Condition = null, string? IsPublic = null,
    string? CreatedAt = null, string? UpdatedAt = null,
    string? AnalyzedAt = null);

public sealed record AnalyzedResourceResult(
    string? ResourceArn = null, string? ResourceType = null,
    string? ResourceOwnerAccount = null, string? Status = null,
    string? IsPublic = null, string? AnalyzedAt = null,
    string? CreatedAt = null, string? UpdatedAt = null);

public sealed record ArchiveRuleResult(
    string? RuleName = null, string? CreatedAt = null,
    string? UpdatedAt = null);

public sealed record PolicyGenerationResult(
    string? JobId = null, string? Status = null,
    string? GeneratedPolicy = null);

public sealed record ValidatePolicyFindingResult(
    string? FindingType = null, string? IssueCode = null,
    string? Message = null, string? LearnMoreLink = null);

public sealed record AccessPreviewResult(
    string? Id = null, string? AnalyzerArn = null,
    string? Status = null, string? CreatedAt = null);

public sealed record AccessPreviewFindingResult(
    string? Id = null, string? ResourceType = null,
    string? Resource = null, string? Status = null,
    string? ChangeType = null, string? IsPublic = null,
    string? CreatedAt = null);

public sealed record CheckResultResult(
    string? Result = null, string? Message = null,
    List<string>? Reasons = null);

public sealed record AccessAnalyzerTagResult(
    string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for AWS IAM Access Analyzer.
/// </summary>
public static class AccessAnalyzerService
{
    private static AmazonAccessAnalyzerClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAccessAnalyzerClient>(region);

    // ── Analyzers ───────────────────────────────────────────────────

    /// <summary>
    /// Create an analyzer.
    /// </summary>
    public static async Task<AnalyzerResult> CreateAnalyzerAsync(
        string analyzerName,
        string type,
        List<InlineArchiveRule>? archiveRules = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAnalyzerRequest
        {
            AnalyzerName = analyzerName,
            Type = new Amazon.AccessAnalyzer.Type(type)
        };
        if (archiveRules != null) request.ArchiveRules = archiveRules;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAnalyzerAsync(request);
            return new AnalyzerResult(Arn: resp.Arn);
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create analyzer '{analyzerName}'");
        }
    }

    /// <summary>
    /// Delete an analyzer.
    /// </summary>
    public static async Task DeleteAnalyzerAsync(
        string analyzerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAnalyzerAsync(
                new DeleteAnalyzerRequest { AnalyzerName = analyzerName });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete analyzer '{analyzerName}'");
        }
    }

    /// <summary>
    /// Get analyzer details.
    /// </summary>
    public static async Task<AnalyzerResult> GetAnalyzerAsync(
        string analyzerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAnalyzerAsync(
                new GetAnalyzerRequest { AnalyzerName = analyzerName });
            var a = resp.Analyzer;
            return new AnalyzerResult(
                Arn: a.Arn, Name: a.Name,
                Type: a.Type?.Value,
                Status: a.Status?.Value,
                CreatedAt: a.CreatedAt?.ToString(),
                LastResourceAnalyzed: a.LastResourceAnalyzed,
                LastResourceAnalyzedAt: a.LastResourceAnalyzedAt?.ToString());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get analyzer '{analyzerName}'");
        }
    }

    /// <summary>
    /// List analyzers.
    /// </summary>
    public static async Task<List<AnalyzerResult>> ListAnalyzersAsync(
        string? type = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<AnalyzerResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAnalyzersRequest();
                if (type != null)
                    request.Type = new Amazon.AccessAnalyzer.Type(type);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAnalyzersAsync(request);
                foreach (var a in resp.Analyzers)
                {
                    results.Add(new AnalyzerResult(
                        Arn: a.Arn, Name: a.Name,
                        Type: a.Type?.Value,
                        Status: a.Status?.Value,
                        CreatedAt: a.CreatedAt?.ToString(),
                        LastResourceAnalyzed: a.LastResourceAnalyzed,
                        LastResourceAnalyzedAt: a.LastResourceAnalyzedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list analyzers");
        }
    }

    // ── Findings ────────────────────────────────────────────────────

    /// <summary>
    /// Update finding status.
    /// </summary>
    public static async Task UpdateFindingsAsync(
        string analyzerArn,
        string status,
        List<string>? ids = null,
        string? resourceArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFindingsRequest
        {
            AnalyzerArn = analyzerArn,
            Status = new FindingStatusUpdate(status)
        };
        if (ids != null) request.Ids = ids;
        if (resourceArn != null) request.ResourceArn = resourceArn;

        try
        {
            await client.UpdateFindingsAsync(request);
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update findings");
        }
    }

    /// <summary>
    /// Get a finding.
    /// </summary>
    public static async Task<AnalyzerFindingResult> GetFindingAsync(
        string analyzerArn,
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFindingAsync(
                new GetFindingRequest
                {
                    AnalyzerArn = analyzerArn,
                    Id = id
                });
            var f = resp.Finding;
            return new AnalyzerFindingResult(
                Id: f.Id,
                ResourceType: f.ResourceType?.Value,
                Resource: f.Resource,
                Status: f.Status?.Value,
                Principal: f.Principal?.Count > 0
                    ? string.Join(", ", f.Principal.Select(kv => $"{kv.Key}={kv.Value}"))
                    : null,
                Action: f.Action?.Count > 0
                    ? string.Join(", ", f.Action) : null,
                Condition: f.Condition?.Count > 0
                    ? string.Join(", ", f.Condition.Select(kv => $"{kv.Key}={kv.Value}"))
                    : null,
                IsPublic: f.IsPublic?.ToString(),
                CreatedAt: f.CreatedAt?.ToString(),
                UpdatedAt: f.UpdatedAt?.ToString(),
                AnalyzedAt: f.AnalyzedAt?.ToString());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get finding '{id}'");
        }
    }

    /// <summary>
    /// List findings for an analyzer.
    /// </summary>
    public static async Task<List<AnalyzerFindingResult>> ListFindingsAsync(
        string analyzerArn,
        Dictionary<string, Criterion>? filter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<AnalyzerFindingResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListFindingsRequest
                {
                    AnalyzerArn = analyzerArn
                };
                if (filter != null) request.Filter = filter;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListFindingsAsync(request);
                foreach (var f in resp.Findings)
                {
                    results.Add(new AnalyzerFindingResult(
                        Id: f.Id,
                        ResourceType: f.ResourceType?.Value,
                        Resource: f.Resource,
                        Status: f.Status?.Value,
                        Principal: f.Principal?.Count > 0
                            ? string.Join(", ", f.Principal.Select(kv => $"{kv.Key}={kv.Value}"))
                            : null,
                        Action: f.Action?.Count > 0
                            ? string.Join(", ", f.Action) : null,
                        Condition: f.Condition?.Count > 0
                            ? string.Join(", ", f.Condition.Select(kv => $"{kv.Key}={kv.Value}"))
                            : null,
                        IsPublic: f.IsPublic?.ToString(),
                        CreatedAt: f.CreatedAt?.ToString(),
                        UpdatedAt: f.UpdatedAt?.ToString(),
                        AnalyzedAt: f.AnalyzedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list findings for analyzer '{analyzerArn}'");
        }
    }

    // ── Resource scanning ───────────────────────────────────────────

    /// <summary>
    /// Start a resource scan.
    /// </summary>
    public static async Task StartResourceScanAsync(
        string analyzerArn,
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartResourceScanAsync(
                new StartResourceScanRequest
                {
                    AnalyzerArn = analyzerArn,
                    ResourceArn = resourceArn
                });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start resource scan for '{resourceArn}'");
        }
    }

    /// <summary>
    /// Get an analyzed resource.
    /// </summary>
    public static async Task<AnalyzedResourceResult> GetAnalyzedResourceAsync(
        string analyzerArn,
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAnalyzedResourceAsync(
                new GetAnalyzedResourceRequest
                {
                    AnalyzerArn = analyzerArn,
                    ResourceArn = resourceArn
                });
            var r = resp.Resource;
            return new AnalyzedResourceResult(
                ResourceArn: r.ResourceArn,
                ResourceType: r.ResourceType?.Value,
                ResourceOwnerAccount: r.ResourceOwnerAccount,
                Status: r.Status?.Value,
                IsPublic: r.IsPublic?.ToString(),
                AnalyzedAt: r.AnalyzedAt?.ToString(),
                CreatedAt: r.CreatedAt?.ToString(),
                UpdatedAt: r.UpdatedAt?.ToString());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get analyzed resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List analyzed resources.
    /// </summary>
    public static async Task<List<AnalyzedResourceResult>> ListAnalyzedResourcesAsync(
        string analyzerArn,
        string? resourceType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<AnalyzedResourceResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAnalyzedResourcesRequest
                {
                    AnalyzerArn = analyzerArn
                };
                if (resourceType != null)
                    request.ResourceType = new ResourceType(resourceType);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAnalyzedResourcesAsync(request);
                foreach (var r in resp.AnalyzedResources)
                {
                    results.Add(new AnalyzedResourceResult(
                        ResourceArn: r.ResourceArn,
                        ResourceType: r.ResourceType?.Value,
                        ResourceOwnerAccount: r.ResourceOwnerAccount,
                        IsPublic: null));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list analyzed resources for '{analyzerArn}'");
        }
    }

    // ── Archive rules ───────────────────────────────────────────────

    /// <summary>
    /// Create an archive rule.
    /// </summary>
    public static async Task CreateArchiveRuleAsync(
        string analyzerName,
        string ruleName,
        Dictionary<string, Criterion> filter,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateArchiveRuleAsync(
                new CreateArchiveRuleRequest
                {
                    AnalyzerName = analyzerName,
                    RuleName = ruleName,
                    Filter = filter
                });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create archive rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Delete an archive rule.
    /// </summary>
    public static async Task DeleteArchiveRuleAsync(
        string analyzerName,
        string ruleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteArchiveRuleAsync(
                new DeleteArchiveRuleRequest
                {
                    AnalyzerName = analyzerName,
                    RuleName = ruleName
                });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete archive rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Get an archive rule.
    /// </summary>
    public static async Task<ArchiveRuleResult> GetArchiveRuleAsync(
        string analyzerName,
        string ruleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetArchiveRuleAsync(
                new GetArchiveRuleRequest
                {
                    AnalyzerName = analyzerName,
                    RuleName = ruleName
                });
            var r = resp.ArchiveRule;
            return new ArchiveRuleResult(
                RuleName: r.RuleName,
                CreatedAt: r.CreatedAt?.ToString(),
                UpdatedAt: r.UpdatedAt?.ToString());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get archive rule '{ruleName}'");
        }
    }

    /// <summary>
    /// List archive rules for an analyzer.
    /// </summary>
    public static async Task<List<ArchiveRuleResult>> ListArchiveRulesAsync(
        string analyzerName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<ArchiveRuleResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListArchiveRulesRequest
                {
                    AnalyzerName = analyzerName
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListArchiveRulesAsync(request);
                foreach (var r in resp.ArchiveRules)
                {
                    results.Add(new ArchiveRuleResult(
                        RuleName: r.RuleName,
                        CreatedAt: r.CreatedAt?.ToString(),
                        UpdatedAt: r.UpdatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list archive rules for '{analyzerName}'");
        }
    }

    /// <summary>
    /// Update an archive rule.
    /// </summary>
    public static async Task UpdateArchiveRuleAsync(
        string analyzerName,
        string ruleName,
        Dictionary<string, Criterion> filter,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateArchiveRuleAsync(
                new UpdateArchiveRuleRequest
                {
                    AnalyzerName = analyzerName,
                    RuleName = ruleName,
                    Filter = filter
                });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update archive rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Apply an archive rule retroactively.
    /// </summary>
    public static async Task ApplyArchiveRuleAsync(
        string analyzerArn,
        string ruleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ApplyArchiveRuleAsync(
                new ApplyArchiveRuleRequest
                {
                    AnalyzerArn = analyzerArn,
                    RuleName = ruleName
                });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to apply archive rule '{ruleName}'");
        }
    }

    // ── Policy generation ───────────────────────────────────────────

    /// <summary>
    /// Start policy generation.
    /// </summary>
    public static async Task<string> StartPolicyGenerationAsync(
        PolicyGenerationDetails policyGenerationDetails,
        string? clientToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartPolicyGenerationRequest
        {
            PolicyGenerationDetails = policyGenerationDetails
        };
        if (clientToken != null) request.ClientToken = clientToken;

        try
        {
            var resp = await client.StartPolicyGenerationAsync(request);
            return resp.JobId;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start policy generation");
        }
    }

    /// <summary>
    /// Get generated policy.
    /// </summary>
    public static async Task<PolicyGenerationResult> GetGeneratedPolicyAsync(
        string jobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGeneratedPolicyAsync(
                new GetGeneratedPolicyRequest { JobId = jobId });
            var policy = resp.GeneratedPolicyResult?.GeneratedPolicies?
                .FirstOrDefault()?.Policy;
            return new PolicyGenerationResult(
                JobId: resp.JobDetails?.JobId,
                Status: resp.JobDetails?.Status?.Value,
                GeneratedPolicy: policy);
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get generated policy for job '{jobId}'");
        }
    }

    /// <summary>
    /// Cancel policy generation.
    /// </summary>
    public static async Task CancelPolicyGenerationAsync(
        string jobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CancelPolicyGenerationAsync(
                new CancelPolicyGenerationRequest { JobId = jobId });
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel policy generation for job '{jobId}'");
        }
    }

    // ── Policy validation ───────────────────────────────────────────

    /// <summary>
    /// Validate a policy.
    /// </summary>
    public static async Task<List<ValidatePolicyFindingResult>> ValidatePolicyAsync(
        string policyDocument,
        string policyType,
        string? locale = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<ValidatePolicyFindingResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ValidatePolicyRequest
                {
                    PolicyDocument = policyDocument,
                    PolicyType = new PolicyType(policyType)
                };
                if (locale != null) request.Locale = new Locale(locale);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ValidatePolicyAsync(request);
                foreach (var f in resp.Findings)
                {
                    results.Add(new ValidatePolicyFindingResult(
                        FindingType: f.FindingType?.Value,
                        IssueCode: f.IssueCode,
                        Message: f.FindingDetails,
                        LearnMoreLink: f.LearnMoreLink));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to validate policy");
        }
    }

    // ── Access previews ─────────────────────────────────────────────

    /// <summary>
    /// Create an access preview.
    /// </summary>
    public static async Task<AccessPreviewResult> CreateAccessPreviewAsync(
        string analyzerArn,
        Dictionary<string, Configuration> configurations,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAccessPreviewAsync(
                new CreateAccessPreviewRequest
                {
                    AnalyzerArn = analyzerArn,
                    Configurations = configurations
                });
            return new AccessPreviewResult(Id: resp.Id);
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create access preview");
        }
    }

    /// <summary>
    /// Get an access preview.
    /// </summary>
    public static async Task<AccessPreviewResult> GetAccessPreviewAsync(
        string analyzerArn,
        string accessPreviewId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccessPreviewAsync(
                new GetAccessPreviewRequest
                {
                    AnalyzerArn = analyzerArn,
                    AccessPreviewId = accessPreviewId
                });
            var ap = resp.AccessPreview;
            return new AccessPreviewResult(
                Id: ap.Id, AnalyzerArn: ap.AnalyzerArn,
                Status: ap.Status?.Value,
                CreatedAt: ap.CreatedAt?.ToString());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get access preview '{accessPreviewId}'");
        }
    }

    /// <summary>
    /// List access previews.
    /// </summary>
    public static async Task<List<AccessPreviewResult>> ListAccessPreviewsAsync(
        string analyzerArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<AccessPreviewResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccessPreviewsRequest
                {
                    AnalyzerArn = analyzerArn
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccessPreviewsAsync(request);
                foreach (var ap in resp.AccessPreviews)
                {
                    results.Add(new AccessPreviewResult(
                        Id: ap.Id, AnalyzerArn: ap.AnalyzerArn,
                        Status: ap.Status?.Value,
                        CreatedAt: ap.CreatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list access previews for '{analyzerArn}'");
        }
    }

    /// <summary>
    /// List access preview findings.
    /// </summary>
    public static async Task<List<AccessPreviewFindingResult>> ListAccessPreviewFindingsAsync(
        string analyzerArn,
        string accessPreviewId,
        Dictionary<string, Criterion>? filter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<AccessPreviewFindingResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAccessPreviewFindingsRequest
                {
                    AnalyzerArn = analyzerArn,
                    AccessPreviewId = accessPreviewId
                };
                if (filter != null) request.Filter = filter;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAccessPreviewFindingsAsync(request);
                foreach (var f in resp.Findings)
                {
                    results.Add(new AccessPreviewFindingResult(
                        Id: f.Id,
                        ResourceType: f.ResourceType?.Value,
                        Resource: f.Resource,
                        Status: f.Status?.Value,
                        ChangeType: f.ChangeType?.Value,
                        IsPublic: f.IsPublic?.ToString(),
                        CreatedAt: f.CreatedAt?.ToString()));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list access preview findings for '{accessPreviewId}'");
        }
    }

    // ── Policy checks ───────────────────────────────────────────────

    /// <summary>
    /// Check that a policy does not grant new access compared to a reference policy.
    /// </summary>
    public static async Task<CheckResultResult> CheckNoNewAccessAsync(
        string newPolicyDocument,
        string existingPolicyDocument,
        string policyType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CheckNoNewAccessAsync(
                new CheckNoNewAccessRequest
                {
                    NewPolicyDocument = newPolicyDocument,
                    ExistingPolicyDocument = existingPolicyDocument,
                    PolicyType = new AccessCheckPolicyType(policyType)
                });
            return new CheckResultResult(
                Result: resp.Result?.Value,
                Message: resp.Message,
                Reasons: resp.Reasons?
                    .Select(r => r.Description).ToList());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to check no new access");
        }
    }

    /// <summary>
    /// Check that a policy does not grant access to specific actions/resources.
    /// </summary>
    public static async Task<CheckResultResult> CheckAccessNotGrantedAsync(
        string policyDocument,
        List<Access> access,
        string policyType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CheckAccessNotGrantedAsync(
                new CheckAccessNotGrantedRequest
                {
                    PolicyDocument = policyDocument,
                    Access = access,
                    PolicyType = new AccessCheckPolicyType(policyType)
                });
            return new CheckResultResult(
                Result: resp.Result?.Value,
                Message: resp.Message,
                Reasons: resp.Reasons?
                    .Select(r => r.Description).ToList());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to check access not granted");
        }
    }

    /// <summary>
    /// Check that a policy does not grant public access.
    /// </summary>
    public static async Task<CheckResultResult> CheckNoPublicAccessAsync(
        string policyDocument,
        string resourceType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CheckNoPublicAccessAsync(
                new CheckNoPublicAccessRequest
                {
                    PolicyDocument = policyDocument,
                    ResourceType = new AccessCheckResourceType(resourceType)
                });
            return new CheckResultResult(
                Result: resp.Result?.Value,
                Message: resp.Message,
                Reasons: resp.Reasons?
                    .Select(r => r.Description).ToList());
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to check no public access");
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
        catch (AmazonAccessAnalyzerException exc)
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
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<AccessAnalyzerTagResult>> ListTagsForResourceAsync(
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
                .Select(t => new AccessAnalyzerTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonAccessAnalyzerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateAnalyzerAsync"/>.</summary>
    public static AnalyzerResult CreateAnalyzer(string analyzerName, string type, List<InlineArchiveRule>? archiveRules = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAnalyzerAsync(analyzerName, type, archiveRules, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAnalyzerAsync"/>.</summary>
    public static void DeleteAnalyzer(string analyzerName, RegionEndpoint? region = null)
        => DeleteAnalyzerAsync(analyzerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAnalyzerAsync"/>.</summary>
    public static AnalyzerResult GetAnalyzer(string analyzerName, RegionEndpoint? region = null)
        => GetAnalyzerAsync(analyzerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAnalyzersAsync"/>.</summary>
    public static List<AnalyzerResult> ListAnalyzers(string? type = null, RegionEndpoint? region = null)
        => ListAnalyzersAsync(type, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateFindingsAsync"/>.</summary>
    public static void UpdateFindings(string analyzerArn, string status, List<string>? ids = null, string? resourceArn = null, RegionEndpoint? region = null)
        => UpdateFindingsAsync(analyzerArn, status, ids, resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFindingAsync"/>.</summary>
    public static AnalyzerFindingResult GetFinding(string analyzerArn, string id, RegionEndpoint? region = null)
        => GetFindingAsync(analyzerArn, id, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFindingsAsync"/>.</summary>
    public static List<AnalyzerFindingResult> ListFindings(string analyzerArn, Dictionary<string, Criterion>? filter = null, RegionEndpoint? region = null)
        => ListFindingsAsync(analyzerArn, filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartResourceScanAsync"/>.</summary>
    public static void StartResourceScan(string analyzerArn, string resourceArn, RegionEndpoint? region = null)
        => StartResourceScanAsync(analyzerArn, resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAnalyzedResourceAsync"/>.</summary>
    public static AnalyzedResourceResult GetAnalyzedResource(string analyzerArn, string resourceArn, RegionEndpoint? region = null)
        => GetAnalyzedResourceAsync(analyzerArn, resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAnalyzedResourcesAsync"/>.</summary>
    public static List<AnalyzedResourceResult> ListAnalyzedResources(string analyzerArn, string? resourceType = null, RegionEndpoint? region = null)
        => ListAnalyzedResourcesAsync(analyzerArn, resourceType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateArchiveRuleAsync"/>.</summary>
    public static void CreateArchiveRule(string analyzerName, string ruleName, Dictionary<string, Criterion> filter, RegionEndpoint? region = null)
        => CreateArchiveRuleAsync(analyzerName, ruleName, filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteArchiveRuleAsync"/>.</summary>
    public static void DeleteArchiveRule(string analyzerName, string ruleName, RegionEndpoint? region = null)
        => DeleteArchiveRuleAsync(analyzerName, ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetArchiveRuleAsync"/>.</summary>
    public static ArchiveRuleResult GetArchiveRule(string analyzerName, string ruleName, RegionEndpoint? region = null)
        => GetArchiveRuleAsync(analyzerName, ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListArchiveRulesAsync"/>.</summary>
    public static List<ArchiveRuleResult> ListArchiveRules(string analyzerName, RegionEndpoint? region = null)
        => ListArchiveRulesAsync(analyzerName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateArchiveRuleAsync"/>.</summary>
    public static void UpdateArchiveRule(string analyzerName, string ruleName, Dictionary<string, Criterion> filter, RegionEndpoint? region = null)
        => UpdateArchiveRuleAsync(analyzerName, ruleName, filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ApplyArchiveRuleAsync"/>.</summary>
    public static void ApplyArchiveRule(string analyzerArn, string ruleName, RegionEndpoint? region = null)
        => ApplyArchiveRuleAsync(analyzerArn, ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartPolicyGenerationAsync"/>.</summary>
    public static string StartPolicyGeneration(PolicyGenerationDetails policyGenerationDetails, string? clientToken = null, RegionEndpoint? region = null)
        => StartPolicyGenerationAsync(policyGenerationDetails, clientToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetGeneratedPolicyAsync"/>.</summary>
    public static PolicyGenerationResult GetGeneratedPolicy(string jobId, RegionEndpoint? region = null)
        => GetGeneratedPolicyAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelPolicyGenerationAsync"/>.</summary>
    public static void CancelPolicyGeneration(string jobId, RegionEndpoint? region = null)
        => CancelPolicyGenerationAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ValidatePolicyAsync"/>.</summary>
    public static List<ValidatePolicyFindingResult> ValidatePolicy(string policyDocument, string policyType, string? locale = null, RegionEndpoint? region = null)
        => ValidatePolicyAsync(policyDocument, policyType, locale, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAccessPreviewAsync"/>.</summary>
    public static AccessPreviewResult CreateAccessPreview(string analyzerArn, Dictionary<string, Configuration> configurations, RegionEndpoint? region = null)
        => CreateAccessPreviewAsync(analyzerArn, configurations, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAccessPreviewAsync"/>.</summary>
    public static AccessPreviewResult GetAccessPreview(string analyzerArn, string accessPreviewId, RegionEndpoint? region = null)
        => GetAccessPreviewAsync(analyzerArn, accessPreviewId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccessPreviewsAsync"/>.</summary>
    public static List<AccessPreviewResult> ListAccessPreviews(string analyzerArn, RegionEndpoint? region = null)
        => ListAccessPreviewsAsync(analyzerArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccessPreviewFindingsAsync"/>.</summary>
    public static List<AccessPreviewFindingResult> ListAccessPreviewFindings(string analyzerArn, string accessPreviewId, Dictionary<string, Criterion>? filter = null, RegionEndpoint? region = null)
        => ListAccessPreviewFindingsAsync(analyzerArn, accessPreviewId, filter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CheckNoNewAccessAsync"/>.</summary>
    public static CheckResultResult CheckNoNewAccess(string newPolicyDocument, string existingPolicyDocument, string policyType, RegionEndpoint? region = null)
        => CheckNoNewAccessAsync(newPolicyDocument, existingPolicyDocument, policyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CheckAccessNotGrantedAsync"/>.</summary>
    public static CheckResultResult CheckAccessNotGranted(string policyDocument, List<Access> access, string policyType, RegionEndpoint? region = null)
        => CheckAccessNotGrantedAsync(policyDocument, access, policyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CheckNoPublicAccessAsync"/>.</summary>
    public static CheckResultResult CheckNoPublicAccess(string policyDocument, string resourceType, RegionEndpoint? region = null)
        => CheckNoPublicAccessAsync(policyDocument, resourceType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static List<AccessAnalyzerTagResult> ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
