using Amazon;
using Amazon.CodeBuild;
using Amazon.CodeBuild.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CbCreateProjectResult(
    Project? Project = null);

public sealed record CbDeleteProjectResult(bool Success = true);

public sealed record CbBatchGetProjectsResult(
    List<Project>? Projects = null,
    List<string>? ProjectsNotFound = null);

public sealed record CbListProjectsResult(
    List<string>? Projects = null,
    string? NextToken = null);

public sealed record CbUpdateProjectResult(
    Project? Project = null);

public sealed record CbStartBuildResult(
    Build? Build = null);

public sealed record CbStopBuildResult(
    Build? Build = null);

public sealed record CbBatchGetBuildsResult(
    List<Build>? Builds = null,
    List<string>? BuildsNotFound = null);

public sealed record CbListBuildsForProjectResult(
    List<string>? Ids = null,
    string? NextToken = null);

public sealed record CbListBuildsResult(
    List<string>? Ids = null,
    string? NextToken = null);

public sealed record CbRetryBuildResult(
    Build? Build = null);

public sealed record CbCreateReportGroupResult(
    ReportGroup? ReportGroup = null);

public sealed record CbDeleteReportGroupResult(bool Success = true);

public sealed record CbBatchGetReportGroupsResult(
    List<ReportGroup>? ReportGroups = null,
    List<string>? ReportGroupsNotFound = null);

public sealed record CbListReportGroupsResult(
    List<string>? ReportGroups = null,
    string? NextToken = null);

public sealed record CbBatchGetReportsResult(
    List<Report>? Reports = null,
    List<string>? ReportsNotFound = null);

public sealed record CbListReportsForReportGroupResult(
    List<string>? Reports = null,
    string? NextToken = null);

public sealed record CbCreateWebhookResult(
    Webhook? Webhook = null);

public sealed record CbDeleteWebhookResult(bool Success = true);

public sealed record CbUpdateWebhookResult(
    Webhook? Webhook = null);

public sealed record CbImportSourceCredentialsResult(
    string? Arn = null);

public sealed record CbListSourceCredentialsResult(
    List<SourceCredentialsInfo>? SourceCredentialsInfos = null);

public sealed record CbDeleteSourceCredentialsResult(
    string? Arn = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodeBuild.
/// </summary>
public static class CodeBuildService
{
    private static AmazonCodeBuildClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodeBuildClient>(region);

    /// <summary>
    /// Create a new CodeBuild project.
    /// </summary>
    public static async Task<CbCreateProjectResult> CreateProjectAsync(
        CreateProjectRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProjectAsync(request);
            return new CbCreateProjectResult(Project: resp.Project);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create CodeBuild project");
        }
    }

    /// <summary>
    /// Delete a CodeBuild project.
    /// </summary>
    public static async Task<CbDeleteProjectResult> DeleteProjectAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProjectAsync(new DeleteProjectRequest { Name = name });
            return new CbDeleteProjectResult();
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeBuild project '{name}'");
        }
    }

    /// <summary>
    /// Get details for one or more CodeBuild projects.
    /// </summary>
    public static async Task<CbBatchGetProjectsResult> BatchGetProjectsAsync(
        List<string> names,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetProjectsAsync(
                new BatchGetProjectsRequest { Names = names });
            return new CbBatchGetProjectsResult(
                Projects: resp.Projects,
                ProjectsNotFound: resp.ProjectsNotFound);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get CodeBuild projects");
        }
    }

    /// <summary>
    /// List CodeBuild project names.
    /// </summary>
    public static async Task<CbListProjectsResult> ListProjectsAsync(
        string? sortBy = null,
        string? sortOrder = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListProjectsRequest();
        if (sortBy != null) request.SortBy = new ProjectSortByType(sortBy);
        if (sortOrder != null) request.SortOrder = new SortOrderType(sortOrder);
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListProjectsAsync(request);
            return new CbListProjectsResult(
                Projects: resp.Projects,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list CodeBuild projects");
        }
    }

    /// <summary>
    /// Update a CodeBuild project.
    /// </summary>
    public static async Task<CbUpdateProjectResult> UpdateProjectAsync(
        UpdateProjectRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateProjectAsync(request);
            return new CbUpdateProjectResult(Project: resp.Project);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update CodeBuild project");
        }
    }

    /// <summary>
    /// Start a new build for a CodeBuild project.
    /// </summary>
    public static async Task<CbStartBuildResult> StartBuildAsync(
        StartBuildRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartBuildAsync(request);
            return new CbStartBuildResult(Build: resp.Build);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start CodeBuild build");
        }
    }

    /// <summary>
    /// Stop a running CodeBuild build.
    /// </summary>
    public static async Task<CbStopBuildResult> StopBuildAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopBuildAsync(
                new StopBuildRequest { Id = id });
            return new CbStopBuildResult(Build: resp.Build);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop CodeBuild build '{id}'");
        }
    }

    /// <summary>
    /// Get details for one or more builds.
    /// </summary>
    public static async Task<CbBatchGetBuildsResult> BatchGetBuildsAsync(
        List<string> ids,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetBuildsAsync(
                new BatchGetBuildsRequest { Ids = ids });
            return new CbBatchGetBuildsResult(
                Builds: resp.Builds,
                BuildsNotFound: resp.BuildsNotFound);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get CodeBuild builds");
        }
    }

    /// <summary>
    /// List build IDs for a specific CodeBuild project.
    /// </summary>
    public static async Task<CbListBuildsForProjectResult>
        ListBuildsForProjectAsync(
            string projectName,
            string? sortOrder = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBuildsForProjectRequest
        {
            ProjectName = projectName
        };
        if (sortOrder != null) request.SortOrder = new SortOrderType(sortOrder);
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListBuildsForProjectAsync(request);
            return new CbListBuildsForProjectResult(
                Ids: resp.Ids,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list builds for project '{projectName}'");
        }
    }

    /// <summary>
    /// List all build IDs.
    /// </summary>
    public static async Task<CbListBuildsResult> ListBuildsAsync(
        string? sortOrder = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBuildsRequest();
        if (sortOrder != null) request.SortOrder = new SortOrderType(sortOrder);
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListBuildsAsync(request);
            return new CbListBuildsResult(
                Ids: resp.Ids,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list CodeBuild builds");
        }
    }

    /// <summary>
    /// Retry a failed or stopped build.
    /// </summary>
    public static async Task<CbRetryBuildResult> RetryBuildAsync(
        string id,
        string? idempotencyToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RetryBuildRequest { Id = id };
        if (idempotencyToken != null) request.IdempotencyToken = idempotencyToken;

        try
        {
            var resp = await client.RetryBuildAsync(request);
            return new CbRetryBuildResult(Build: resp.Build);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to retry CodeBuild build '{id}'");
        }
    }

    /// <summary>
    /// Create a report group.
    /// </summary>
    public static async Task<CbCreateReportGroupResult> CreateReportGroupAsync(
        CreateReportGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateReportGroupAsync(request);
            return new CbCreateReportGroupResult(ReportGroup: resp.ReportGroup);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeBuild report group");
        }
    }

    /// <summary>
    /// Delete a report group.
    /// </summary>
    public static async Task<CbDeleteReportGroupResult> DeleteReportGroupAsync(
        string arn,
        bool deleteReports = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteReportGroupAsync(new DeleteReportGroupRequest
            {
                Arn = arn,
                DeleteReports = deleteReports
            });
            return new CbDeleteReportGroupResult();
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete CodeBuild report group");
        }
    }

    /// <summary>
    /// Get details for one or more report groups.
    /// </summary>
    public static async Task<CbBatchGetReportGroupsResult>
        BatchGetReportGroupsAsync(
            List<string> reportGroupArns,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetReportGroupsAsync(
                new BatchGetReportGroupsRequest
                {
                    ReportGroupArns = reportGroupArns
                });
            return new CbBatchGetReportGroupsResult(
                ReportGroups: resp.ReportGroups,
                ReportGroupsNotFound: resp.ReportGroupsNotFound);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get CodeBuild report groups");
        }
    }

    /// <summary>
    /// List report group ARNs.
    /// </summary>
    public static async Task<CbListReportGroupsResult> ListReportGroupsAsync(
        string? sortBy = null,
        string? sortOrder = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListReportGroupsRequest();
        if (sortBy != null) request.SortBy = new ReportGroupSortByType(sortBy);
        if (sortOrder != null) request.SortOrder = new SortOrderType(sortOrder);
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListReportGroupsAsync(request);
            return new CbListReportGroupsResult(
                ReportGroups: resp.ReportGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list CodeBuild report groups");
        }
    }

    /// <summary>
    /// Get details for one or more reports.
    /// </summary>
    public static async Task<CbBatchGetReportsResult> BatchGetReportsAsync(
        List<string> reportArns,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetReportsAsync(
                new BatchGetReportsRequest { ReportArns = reportArns });
            return new CbBatchGetReportsResult(
                Reports: resp.Reports,
                ReportsNotFound: resp.ReportsNotFound);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get CodeBuild reports");
        }
    }

    /// <summary>
    /// List report ARNs for a report group.
    /// </summary>
    public static async Task<CbListReportsForReportGroupResult>
        ListReportsForReportGroupAsync(
            string reportGroupArn,
            string? sortOrder = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListReportsForReportGroupRequest
        {
            ReportGroupArn = reportGroupArn
        };
        if (sortOrder != null) request.SortOrder = new SortOrderType(sortOrder);
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListReportsForReportGroupAsync(request);
            return new CbListReportsForReportGroupResult(
                Reports: resp.Reports,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list reports for report group");
        }
    }

    /// <summary>
    /// Create a webhook for a CodeBuild project.
    /// </summary>
    public static async Task<CbCreateWebhookResult> CreateWebhookAsync(
        CreateWebhookRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateWebhookAsync(request);
            return new CbCreateWebhookResult(Webhook: resp.Webhook);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeBuild webhook");
        }
    }

    /// <summary>
    /// Delete a webhook for a CodeBuild project.
    /// </summary>
    public static async Task<CbDeleteWebhookResult> DeleteWebhookAsync(
        string projectName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWebhookAsync(
                new DeleteWebhookRequest { ProjectName = projectName });
            return new CbDeleteWebhookResult();
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeBuild webhook for '{projectName}'");
        }
    }

    /// <summary>
    /// Update a webhook for a CodeBuild project.
    /// </summary>
    public static async Task<CbUpdateWebhookResult> UpdateWebhookAsync(
        UpdateWebhookRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateWebhookAsync(request);
            return new CbUpdateWebhookResult(Webhook: resp.Webhook);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update CodeBuild webhook");
        }
    }

    /// <summary>
    /// Import source credentials for CodeBuild.
    /// </summary>
    public static async Task<CbImportSourceCredentialsResult>
        ImportSourceCredentialsAsync(
            ImportSourceCredentialsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ImportSourceCredentialsAsync(request);
            return new CbImportSourceCredentialsResult(Arn: resp.Arn);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to import source credentials");
        }
    }

    /// <summary>
    /// List source credentials for CodeBuild.
    /// </summary>
    public static async Task<CbListSourceCredentialsResult>
        ListSourceCredentialsAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListSourceCredentialsAsync(
                new ListSourceCredentialsRequest());
            return new CbListSourceCredentialsResult(
                SourceCredentialsInfos: resp.SourceCredentialsInfos);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list source credentials");
        }
    }

    /// <summary>
    /// Delete source credentials from CodeBuild.
    /// </summary>
    public static async Task<CbDeleteSourceCredentialsResult>
        DeleteSourceCredentialsAsync(
            string arn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSourceCredentialsAsync(
                new DeleteSourceCredentialsRequest { Arn = arn });
            return new CbDeleteSourceCredentialsResult(Arn: resp.Arn);
        }
        catch (AmazonCodeBuildException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete source credentials");
        }
    }
}
