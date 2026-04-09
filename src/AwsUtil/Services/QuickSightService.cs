using Amazon;
using Amazon.QuickSight;
using Amazon.QuickSight.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record QsDashboardResult(
    string? DashboardId = null,
    string? Arn = null,
    string? Name = null,
    string? VersionArn = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListDashboardsResult(
    List<DashboardSummary>? DashboardSummaryList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsDataSetResult(
    string? DataSetId = null,
    string? Arn = null,
    string? Name = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListDataSetsResult(
    List<DataSetSummary>? DataSetSummaries = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsDataSourceResult(
    string? DataSourceId = null,
    string? Arn = null,
    string? Name = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListDataSourcesResult(
    List<DataSource>? DataSources = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsAnalysisResult(
    string? AnalysisId = null,
    string? Arn = null,
    string? Name = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListAnalysesResult(
    List<AnalysisSummary>? AnalysisSummaryList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsTemplateResult(
    string? TemplateId = null,
    string? Arn = null,
    string? VersionArn = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListTemplatesResult(
    List<TemplateSummary>? TemplateSummaryList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsThemeResult(
    string? ThemeId = null,
    string? Arn = null,
    string? VersionArn = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsListThemesResult(
    List<ThemeSummary>? ThemeSummaryList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsGroupResult(
    string? GroupName = null,
    string? Arn = null,
    string? Description = null,
    string? PrincipalId = null,
    string? RequestId = null);

public sealed record QsListGroupsResult(
    List<Group>? GroupList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsGroupMembershipResult(
    GroupMember? GroupMember = null,
    string? RequestId = null);

public sealed record QsListGroupMembershipsResult(
    List<GroupMember>? GroupMemberList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsUserResult(
    string? UserName = null,
    string? Arn = null,
    string? Email = null,
    string? Role = null,
    string? RequestId = null);

public sealed record QsListUsersResult(
    List<User>? UserList = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsSearchResult(
    List<DashboardSummary>? DashboardSummaryList = null,
    List<AnalysisSummary>? AnalysisSummaryList = null,
    List<DataSetSummary>? DataSetSummaries = null,
    List<DataSource>? DataSources = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record QsTagResult(string? RequestId = null);

public sealed record QsListTagsResult(
    List<Tag>? Tags = null,
    string? RequestId = null);

public sealed record QsDeleteResult(
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeDashboardResult(
    Dashboard? Dashboard = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeDataSetResult(
    DataSet? DataSet = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeDataSourceResult(
    DataSource? DataSource = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeAnalysisResult(
    Analysis? Analysis = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeTemplateResult(
    Template? Template = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeThemeResult(
    Theme? Theme = null,
    int? Status = null,
    string? RequestId = null);

public sealed record QsDescribeGroupResult(
    Group? Group = null,
    string? RequestId = null);

public sealed record QsDescribeUserResult(
    User? User = null,
    string? RequestId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon QuickSight.
/// </summary>
public static class QuickSightService
{
    private static AmazonQuickSightClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonQuickSightClient>(region);

    // ── Dashboard ────────────────────────────────────────────────────

    /// <summary>Create a QuickSight dashboard.</summary>
    public static async Task<QsDashboardResult> CreateDashboardAsync(
        string awsAccountId,
        string dashboardId,
        string name,
        DashboardSourceEntity sourceEntity,
        DashboardPublishOptions? publishOptions = null,
        List<ResourcePermission>? permissions = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDashboardRequest
        {
            AwsAccountId = awsAccountId,
            DashboardId = dashboardId,
            Name = name,
            SourceEntity = sourceEntity
        };
        if (publishOptions != null) request.DashboardPublishOptions = publishOptions;
        if (permissions != null) request.Permissions = permissions;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDashboardAsync(request);
            return new QsDashboardResult(
                DashboardId: resp.DashboardId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: resp.CreationStatus?.Value != null
                    ? (int)resp.HttpStatusCode : null,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dashboard '{dashboardId}'");
        }
    }

    /// <summary>Delete a QuickSight dashboard.</summary>
    public static async Task<QsDeleteResult> DeleteDashboardAsync(
        string awsAccountId,
        string dashboardId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDashboardAsync(new DeleteDashboardRequest
            {
                AwsAccountId = awsAccountId,
                DashboardId = dashboardId
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dashboard '{dashboardId}'");
        }
    }

    /// <summary>Describe a QuickSight dashboard.</summary>
    public static async Task<QsDescribeDashboardResult> DescribeDashboardAsync(
        string awsAccountId,
        string dashboardId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDashboardAsync(
                new DescribeDashboardRequest
                {
                    AwsAccountId = awsAccountId,
                    DashboardId = dashboardId
                });
            return new QsDescribeDashboardResult(
                Dashboard: resp.Dashboard,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dashboard '{dashboardId}'");
        }
    }

    /// <summary>List QuickSight dashboards.</summary>
    public static async Task<QsListDashboardsResult> ListDashboardsAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDashboardsRequest { AwsAccountId = awsAccountId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDashboardsAsync(request);
            return new QsListDashboardsResult(
                DashboardSummaryList: resp.DashboardSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dashboards");
        }
    }

    /// <summary>Update a QuickSight dashboard.</summary>
    public static async Task<QsDashboardResult> UpdateDashboardAsync(
        string awsAccountId,
        string dashboardId,
        string name,
        DashboardSourceEntity sourceEntity,
        DashboardPublishOptions? publishOptions = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDashboardRequest
        {
            AwsAccountId = awsAccountId,
            DashboardId = dashboardId,
            Name = name,
            SourceEntity = sourceEntity
        };
        if (publishOptions != null) request.DashboardPublishOptions = publishOptions;

        try
        {
            var resp = await client.UpdateDashboardAsync(request);
            return new QsDashboardResult(
                DashboardId: resp.DashboardId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update dashboard '{dashboardId}'");
        }
    }

    // ── DataSet ──────────────────────────────────────────────────────

    /// <summary>Create a QuickSight data set.</summary>
    public static async Task<QsDataSetResult> CreateDataSetAsync(
        string awsAccountId,
        string dataSetId,
        string name,
        Dictionary<string, PhysicalTable> physicalTableMap,
        DataSetImportMode importMode,
        Dictionary<string, LogicalTable>? logicalTableMap = null,
        List<ResourcePermission>? permissions = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDataSetRequest
        {
            AwsAccountId = awsAccountId,
            DataSetId = dataSetId,
            Name = name,
            PhysicalTableMap = physicalTableMap,
            ImportMode = importMode
        };
        if (logicalTableMap != null) request.LogicalTableMap = logicalTableMap;
        if (permissions != null) request.Permissions = permissions;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDataSetAsync(request);
            return new QsDataSetResult(
                DataSetId: resp.DataSetId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create data set '{dataSetId}'");
        }
    }

    /// <summary>Delete a QuickSight data set.</summary>
    public static async Task<QsDeleteResult> DeleteDataSetAsync(
        string awsAccountId,
        string dataSetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDataSetAsync(new DeleteDataSetRequest
            {
                AwsAccountId = awsAccountId,
                DataSetId = dataSetId
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete data set '{dataSetId}'");
        }
    }

    /// <summary>Describe a QuickSight data set.</summary>
    public static async Task<QsDescribeDataSetResult> DescribeDataSetAsync(
        string awsAccountId,
        string dataSetId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDataSetAsync(
                new DescribeDataSetRequest
                {
                    AwsAccountId = awsAccountId,
                    DataSetId = dataSetId
                });
            return new QsDescribeDataSetResult(
                DataSet: resp.DataSet,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe data set '{dataSetId}'");
        }
    }

    /// <summary>List QuickSight data sets.</summary>
    public static async Task<QsListDataSetsResult> ListDataSetsAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDataSetsRequest { AwsAccountId = awsAccountId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDataSetsAsync(request);
            return new QsListDataSetsResult(
                DataSetSummaries: resp.DataSetSummaries,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list data sets");
        }
    }

    /// <summary>Update a QuickSight data set.</summary>
    public static async Task<QsDataSetResult> UpdateDataSetAsync(
        string awsAccountId,
        string dataSetId,
        string name,
        Dictionary<string, PhysicalTable> physicalTableMap,
        DataSetImportMode importMode,
        Dictionary<string, LogicalTable>? logicalTableMap = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDataSetRequest
        {
            AwsAccountId = awsAccountId,
            DataSetId = dataSetId,
            Name = name,
            PhysicalTableMap = physicalTableMap,
            ImportMode = importMode
        };
        if (logicalTableMap != null) request.LogicalTableMap = logicalTableMap;

        try
        {
            var resp = await client.UpdateDataSetAsync(request);
            return new QsDataSetResult(
                DataSetId: resp.DataSetId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update data set '{dataSetId}'");
        }
    }

    // ── DataSource ───────────────────────────────────────────────────

    /// <summary>Create a QuickSight data source.</summary>
    public static async Task<QsDataSourceResult> CreateDataSourceAsync(
        string awsAccountId,
        string dataSourceId,
        string name,
        DataSourceType type,
        DataSourceParameters? dataSourceParameters = null,
        DataSourceCredentials? credentials = null,
        List<ResourcePermission>? permissions = null,
        SslProperties? sslProperties = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDataSourceRequest
        {
            AwsAccountId = awsAccountId,
            DataSourceId = dataSourceId,
            Name = name,
            Type = type
        };
        if (dataSourceParameters != null)
            request.DataSourceParameters = dataSourceParameters;
        if (credentials != null) request.Credentials = credentials;
        if (permissions != null) request.Permissions = permissions;
        if (sslProperties != null) request.SslProperties = sslProperties;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDataSourceAsync(request);
            return new QsDataSourceResult(
                DataSourceId: resp.DataSourceId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create data source '{dataSourceId}'");
        }
    }

    /// <summary>Delete a QuickSight data source.</summary>
    public static async Task<QsDeleteResult> DeleteDataSourceAsync(
        string awsAccountId,
        string dataSourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteDataSourceAsync(
                new DeleteDataSourceRequest
                {
                    AwsAccountId = awsAccountId,
                    DataSourceId = dataSourceId
                });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete data source '{dataSourceId}'");
        }
    }

    /// <summary>Describe a QuickSight data source.</summary>
    public static async Task<QsDescribeDataSourceResult> DescribeDataSourceAsync(
        string awsAccountId,
        string dataSourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDataSourceAsync(
                new DescribeDataSourceRequest
                {
                    AwsAccountId = awsAccountId,
                    DataSourceId = dataSourceId
                });
            return new QsDescribeDataSourceResult(
                DataSource: resp.DataSource,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe data source '{dataSourceId}'");
        }
    }

    /// <summary>List QuickSight data sources.</summary>
    public static async Task<QsListDataSourcesResult> ListDataSourcesAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDataSourcesRequest
        {
            AwsAccountId = awsAccountId
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDataSourcesAsync(request);
            return new QsListDataSourcesResult(
                DataSources: resp.DataSources,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list data sources");
        }
    }

    /// <summary>Update a QuickSight data source.</summary>
    public static async Task<QsDataSourceResult> UpdateDataSourceAsync(
        string awsAccountId,
        string dataSourceId,
        string name,
        DataSourceParameters? dataSourceParameters = null,
        DataSourceCredentials? credentials = null,
        SslProperties? sslProperties = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDataSourceRequest
        {
            AwsAccountId = awsAccountId,
            DataSourceId = dataSourceId,
            Name = name
        };
        if (dataSourceParameters != null)
            request.DataSourceParameters = dataSourceParameters;
        if (credentials != null) request.Credentials = credentials;
        if (sslProperties != null) request.SslProperties = sslProperties;

        try
        {
            var resp = await client.UpdateDataSourceAsync(request);
            return new QsDataSourceResult(
                DataSourceId: resp.DataSourceId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update data source '{dataSourceId}'");
        }
    }

    // ── Analysis ─────────────────────────────────────────────────────

    /// <summary>Create a QuickSight analysis.</summary>
    public static async Task<QsAnalysisResult> CreateAnalysisAsync(
        string awsAccountId,
        string analysisId,
        string name,
        AnalysisSourceEntity sourceEntity,
        List<ResourcePermission>? permissions = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAnalysisRequest
        {
            AwsAccountId = awsAccountId,
            AnalysisId = analysisId,
            Name = name,
            SourceEntity = sourceEntity
        };
        if (permissions != null) request.Permissions = permissions;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAnalysisAsync(request);
            return new QsAnalysisResult(
                AnalysisId: resp.AnalysisId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create analysis '{analysisId}'");
        }
    }

    /// <summary>Delete a QuickSight analysis.</summary>
    public static async Task<QsDeleteResult> DeleteAnalysisAsync(
        string awsAccountId,
        string analysisId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteAnalysisAsync(new DeleteAnalysisRequest
            {
                AwsAccountId = awsAccountId,
                AnalysisId = analysisId
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete analysis '{analysisId}'");
        }
    }

    /// <summary>Describe a QuickSight analysis.</summary>
    public static async Task<QsDescribeAnalysisResult> DescribeAnalysisAsync(
        string awsAccountId,
        string analysisId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAnalysisAsync(
                new DescribeAnalysisRequest
                {
                    AwsAccountId = awsAccountId,
                    AnalysisId = analysisId
                });
            return new QsDescribeAnalysisResult(
                Analysis: resp.Analysis,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe analysis '{analysisId}'");
        }
    }

    /// <summary>List QuickSight analyses.</summary>
    public static async Task<QsListAnalysesResult> ListAnalysesAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAnalysesRequest { AwsAccountId = awsAccountId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListAnalysesAsync(request);
            return new QsListAnalysesResult(
                AnalysisSummaryList: resp.AnalysisSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list analyses");
        }
    }

    /// <summary>Update a QuickSight analysis.</summary>
    public static async Task<QsAnalysisResult> UpdateAnalysisAsync(
        string awsAccountId,
        string analysisId,
        string name,
        AnalysisSourceEntity sourceEntity,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAnalysisRequest
        {
            AwsAccountId = awsAccountId,
            AnalysisId = analysisId,
            Name = name,
            SourceEntity = sourceEntity
        };

        try
        {
            var resp = await client.UpdateAnalysisAsync(request);
            return new QsAnalysisResult(
                AnalysisId: resp.AnalysisId,
                Arn: resp.Arn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update analysis '{analysisId}'");
        }
    }

    // ── Template ─────────────────────────────────────────────────────

    /// <summary>Create a QuickSight template.</summary>
    public static async Task<QsTemplateResult> CreateTemplateAsync(
        string awsAccountId,
        string templateId,
        string name,
        TemplateSourceEntity sourceEntity,
        List<ResourcePermission>? permissions = null,
        List<Tag>? tags = null,
        string? versionDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTemplateRequest
        {
            AwsAccountId = awsAccountId,
            TemplateId = templateId,
            Name = name,
            SourceEntity = sourceEntity
        };
        if (permissions != null) request.Permissions = permissions;
        if (tags != null) request.Tags = tags;
        if (versionDescription != null)
            request.VersionDescription = versionDescription;

        try
        {
            var resp = await client.CreateTemplateAsync(request);
            return new QsTemplateResult(
                TemplateId: resp.TemplateId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create template '{templateId}'");
        }
    }

    /// <summary>Delete a QuickSight template.</summary>
    public static async Task<QsDeleteResult> DeleteTemplateAsync(
        string awsAccountId,
        string templateId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteTemplateAsync(new DeleteTemplateRequest
            {
                AwsAccountId = awsAccountId,
                TemplateId = templateId
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete template '{templateId}'");
        }
    }

    /// <summary>Describe a QuickSight template.</summary>
    public static async Task<QsDescribeTemplateResult> DescribeTemplateAsync(
        string awsAccountId,
        string templateId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTemplateAsync(
                new DescribeTemplateRequest
                {
                    AwsAccountId = awsAccountId,
                    TemplateId = templateId
                });
            return new QsDescribeTemplateResult(
                Template: resp.Template,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe template '{templateId}'");
        }
    }

    /// <summary>List QuickSight templates.</summary>
    public static async Task<QsListTemplatesResult> ListTemplatesAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTemplatesRequest { AwsAccountId = awsAccountId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTemplatesAsync(request);
            return new QsListTemplatesResult(
                TemplateSummaryList: resp.TemplateSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list templates");
        }
    }

    /// <summary>Update a QuickSight template.</summary>
    public static async Task<QsTemplateResult> UpdateTemplateAsync(
        string awsAccountId,
        string templateId,
        TemplateSourceEntity sourceEntity,
        string? versionDescription = null,
        string? name = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateTemplateRequest
        {
            AwsAccountId = awsAccountId,
            TemplateId = templateId,
            SourceEntity = sourceEntity
        };
        if (versionDescription != null)
            request.VersionDescription = versionDescription;
        if (name != null) request.Name = name;

        try
        {
            var resp = await client.UpdateTemplateAsync(request);
            return new QsTemplateResult(
                TemplateId: resp.TemplateId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update template '{templateId}'");
        }
    }

    // ── Theme ────────────────────────────────────────────────────────

    /// <summary>Create a QuickSight theme.</summary>
    public static async Task<QsThemeResult> CreateThemeAsync(
        string awsAccountId,
        string themeId,
        string name,
        string baseThemeId,
        ThemeConfiguration configuration,
        List<ResourcePermission>? permissions = null,
        List<Tag>? tags = null,
        string? versionDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateThemeRequest
        {
            AwsAccountId = awsAccountId,
            ThemeId = themeId,
            Name = name,
            BaseThemeId = baseThemeId,
            Configuration = configuration
        };
        if (permissions != null) request.Permissions = permissions;
        if (tags != null) request.Tags = tags;
        if (versionDescription != null)
            request.VersionDescription = versionDescription;

        try
        {
            var resp = await client.CreateThemeAsync(request);
            return new QsThemeResult(
                ThemeId: resp.ThemeId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create theme '{themeId}'");
        }
    }

    /// <summary>Delete a QuickSight theme.</summary>
    public static async Task<QsDeleteResult> DeleteThemeAsync(
        string awsAccountId,
        string themeId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteThemeAsync(new DeleteThemeRequest
            {
                AwsAccountId = awsAccountId,
                ThemeId = themeId
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete theme '{themeId}'");
        }
    }

    /// <summary>Describe a QuickSight theme.</summary>
    public static async Task<QsDescribeThemeResult> DescribeThemeAsync(
        string awsAccountId,
        string themeId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeThemeAsync(
                new DescribeThemeRequest
                {
                    AwsAccountId = awsAccountId,
                    ThemeId = themeId
                });
            return new QsDescribeThemeResult(
                Theme: resp.Theme,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe theme '{themeId}'");
        }
    }

    /// <summary>List QuickSight themes.</summary>
    public static async Task<QsListThemesResult> ListThemesAsync(
        string awsAccountId,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThemesRequest { AwsAccountId = awsAccountId };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListThemesAsync(request);
            return new QsListThemesResult(
                ThemeSummaryList: resp.ThemeSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list themes");
        }
    }

    /// <summary>Update a QuickSight theme.</summary>
    public static async Task<QsThemeResult> UpdateThemeAsync(
        string awsAccountId,
        string themeId,
        string baseThemeId,
        ThemeConfiguration? configuration = null,
        string? name = null,
        string? versionDescription = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateThemeRequest
        {
            AwsAccountId = awsAccountId,
            ThemeId = themeId,
            BaseThemeId = baseThemeId
        };
        if (configuration != null) request.Configuration = configuration;
        if (name != null) request.Name = name;
        if (versionDescription != null)
            request.VersionDescription = versionDescription;

        try
        {
            var resp = await client.UpdateThemeAsync(request);
            return new QsThemeResult(
                ThemeId: resp.ThemeId,
                Arn: resp.Arn,
                VersionArn: resp.VersionArn,
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update theme '{themeId}'");
        }
    }

    // ── Group ────────────────────────────────────────────────────────

    /// <summary>Create a QuickSight group.</summary>
    public static async Task<QsGroupResult> CreateGroupAsync(
        string awsAccountId,
        string @namespace,
        string groupName,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGroupRequest
        {
            AwsAccountId = awsAccountId,
            Namespace = @namespace,
            GroupName = groupName
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.CreateGroupAsync(request);
            return new QsGroupResult(
                GroupName: resp.Group?.GroupName,
                Arn: resp.Group?.Arn,
                Description: resp.Group?.Description,
                PrincipalId: resp.Group?.PrincipalId,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create group '{groupName}'");
        }
    }

    /// <summary>Delete a QuickSight group.</summary>
    public static async Task<QsDeleteResult> DeleteGroupAsync(
        string awsAccountId,
        string @namespace,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGroupAsync(new DeleteGroupRequest
            {
                AwsAccountId = awsAccountId,
                Namespace = @namespace,
                GroupName = groupName
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete group '{groupName}'");
        }
    }

    /// <summary>Describe a QuickSight group.</summary>
    public static async Task<QsDescribeGroupResult> DescribeGroupAsync(
        string awsAccountId,
        string @namespace,
        string groupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeGroupAsync(
                new DescribeGroupRequest
                {
                    AwsAccountId = awsAccountId,
                    Namespace = @namespace,
                    GroupName = groupName
                });
            return new QsDescribeGroupResult(
                Group: resp.Group,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe group '{groupName}'");
        }
    }

    /// <summary>List QuickSight groups.</summary>
    public static async Task<QsListGroupsResult> ListGroupsAsync(
        string awsAccountId,
        string @namespace,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupsRequest
        {
            AwsAccountId = awsAccountId,
            Namespace = @namespace
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListGroupsAsync(request);
            return new QsListGroupsResult(
                GroupList: resp.GroupList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list groups");
        }
    }

    // ── Group Membership ─────────────────────────────────────────────

    /// <summary>Add a member to a QuickSight group.</summary>
    public static async Task<QsGroupMembershipResult>
        CreateGroupMembershipAsync(
            string awsAccountId,
            string @namespace,
            string groupName,
            string memberName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGroupMembershipAsync(
                new CreateGroupMembershipRequest
                {
                    AwsAccountId = awsAccountId,
                    Namespace = @namespace,
                    GroupName = groupName,
                    MemberName = memberName
                });
            return new QsGroupMembershipResult(
                GroupMember: resp.GroupMember,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add '{memberName}' to group '{groupName}'");
        }
    }

    /// <summary>Remove a member from a QuickSight group.</summary>
    public static async Task<QsDeleteResult> DeleteGroupMembershipAsync(
        string awsAccountId,
        string @namespace,
        string groupName,
        string memberName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteGroupMembershipAsync(
                new DeleteGroupMembershipRequest
                {
                    AwsAccountId = awsAccountId,
                    Namespace = @namespace,
                    GroupName = groupName,
                    MemberName = memberName
                });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove '{memberName}' from group '{groupName}'");
        }
    }

    /// <summary>List members of a QuickSight group.</summary>
    public static async Task<QsListGroupMembershipsResult>
        ListGroupMembershipsAsync(
            string awsAccountId,
            string @namespace,
            string groupName,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupMembershipsRequest
        {
            AwsAccountId = awsAccountId,
            Namespace = @namespace,
            GroupName = groupName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListGroupMembershipsAsync(request);
            return new QsListGroupMembershipsResult(
                GroupMemberList: resp.GroupMemberList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list memberships for group '{groupName}'");
        }
    }

    // ── User ─────────────────────────────────────────────────────────

    /// <summary>Register a QuickSight user.</summary>
    public static async Task<QsUserResult> RegisterUserAsync(
        string awsAccountId,
        string @namespace,
        string email,
        UserRole userRole,
        IdentityType identityType,
        string? iamArn = null,
        string? sessionName = null,
        string? userName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RegisterUserRequest
        {
            AwsAccountId = awsAccountId,
            Namespace = @namespace,
            Email = email,
            UserRole = userRole,
            IdentityType = identityType
        };
        if (iamArn != null) request.IamArn = iamArn;
        if (sessionName != null) request.SessionName = sessionName;
        if (userName != null) request.UserName = userName;

        try
        {
            var resp = await client.RegisterUserAsync(request);
            return new QsUserResult(
                UserName: resp.User?.UserName,
                Arn: resp.User?.Arn,
                Email: resp.User?.Email,
                Role: resp.User?.Role?.Value,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to register user '{email}'");
        }
    }

    /// <summary>Delete a QuickSight user.</summary>
    public static async Task<QsDeleteResult> DeleteUserAsync(
        string awsAccountId,
        string @namespace,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteUserAsync(new DeleteUserRequest
            {
                AwsAccountId = awsAccountId,
                Namespace = @namespace,
                UserName = userName
            });
            return new QsDeleteResult(
                Status: (int)resp.HttpStatusCode,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete user '{userName}'");
        }
    }

    /// <summary>Describe a QuickSight user.</summary>
    public static async Task<QsDescribeUserResult> DescribeUserAsync(
        string awsAccountId,
        string @namespace,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeUserAsync(
                new DescribeUserRequest
                {
                    AwsAccountId = awsAccountId,
                    Namespace = @namespace,
                    UserName = userName
                });
            return new QsDescribeUserResult(
                User: resp.User,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe user '{userName}'");
        }
    }

    /// <summary>List QuickSight users.</summary>
    public static async Task<QsListUsersResult> ListUsersAsync(
        string awsAccountId,
        string @namespace,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsersRequest
        {
            AwsAccountId = awsAccountId,
            Namespace = @namespace
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListUsersAsync(request);
            return new QsListUsersResult(
                UserList: resp.UserList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list users");
        }
    }

    // ── Search ───────────────────────────────────────────────────────

    /// <summary>Search QuickSight dashboards.</summary>
    public static async Task<QsSearchResult> SearchDashboardsAsync(
        string awsAccountId,
        List<DashboardSearchFilter> filters,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchDashboardsRequest
        {
            AwsAccountId = awsAccountId,
            Filters = filters
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.SearchDashboardsAsync(request);
            return new QsSearchResult(
                DashboardSummaryList: resp.DashboardSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to search dashboards");
        }
    }

    /// <summary>Search QuickSight analyses.</summary>
    public static async Task<QsSearchResult> SearchAnalysesAsync(
        string awsAccountId,
        List<AnalysisSearchFilter> filters,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchAnalysesRequest
        {
            AwsAccountId = awsAccountId,
            Filters = filters
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.SearchAnalysesAsync(request);
            return new QsSearchResult(
                AnalysisSummaryList: resp.AnalysisSummaryList,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to search analyses");
        }
    }

    /// <summary>Search QuickSight data sets.</summary>
    public static async Task<QsSearchResult> SearchDataSetsAsync(
        string awsAccountId,
        List<DataSetSearchFilter> filters,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchDataSetsRequest
        {
            AwsAccountId = awsAccountId,
            Filters = filters
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.SearchDataSetsAsync(request);
            return new QsSearchResult(
                DataSetSummaries: resp.DataSetSummaries,
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to search data sets");
        }
    }

    /// <summary>Search QuickSight data sources.</summary>
    public static async Task<QsSearchResult> SearchDataSourcesAsync(
        string awsAccountId,
        List<DataSourceSearchFilter> filters,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchDataSourcesRequest
        {
            AwsAccountId = awsAccountId,
            Filters = filters
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.SearchDataSourcesAsync(request);
            return new QsSearchResult(
                DataSources: resp.DataSourceSummaries?.Select(s =>
                    new DataSource
                    {
                        DataSourceId = s.DataSourceId,
                        Arn = s.Arn,
                        Name = s.Name
                    }).ToList(),
                NextToken: resp.NextToken,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to search data sources");
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────

    /// <summary>Tag a QuickSight resource.</summary>
    public static async Task<QsTagResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
            return new QsTagResult(RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a QuickSight resource.</summary>
    public static async Task<QsTagResult> UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UntagResourceAsync(
                new UntagResourceRequest
                {
                    ResourceArn = resourceArn,
                    TagKeys = tagKeys
                });
            return new QsTagResult(RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a QuickSight resource.</summary>
    public static async Task<QsListTagsResult> ListTagsForResourceAsync(
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
            return new QsListTagsResult(
                Tags: resp.Tags,
                RequestId: resp.RequestId);
        }
        catch (AmazonQuickSightException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}
