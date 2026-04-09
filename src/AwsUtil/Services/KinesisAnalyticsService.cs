using Amazon;
using Amazon.KinesisAnalyticsV2;
using Amazon.KinesisAnalyticsV2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record KaCreateApplicationResult(
    string? ApplicationName = null,
    string? ApplicationArn = null,
    string? ApplicationStatus = null,
    long? ApplicationVersionId = null);

public sealed record KaDeleteApplicationResult(bool Success = true);

public sealed record KaDescribeApplicationResult(
    ApplicationDetail? ApplicationDetail = null);

public sealed record KaListApplicationsResult(
    List<ApplicationSummary>? ApplicationSummaries = null,
    string? NextToken = null);

public sealed record KaUpdateApplicationResult(
    string? ApplicationName = null,
    string? ApplicationArn = null,
    long? ApplicationVersionId = null);

public sealed record KaStartApplicationResult(bool Success = true);
public sealed record KaStopApplicationResult(bool Success = true);

public sealed record KaAddApplicationInputResult(
    List<InputDescription>? InputDescriptions = null);

public sealed record KaAddApplicationOutputResult(
    List<OutputDescription>? OutputDescriptions = null);

public sealed record KaAddApplicationReferenceDataSourceResult(
    List<ReferenceDataSourceDescription>? ReferenceDataSourceDescriptions = null);

public sealed record KaDeleteApplicationInputProcessingConfigurationResult(
    bool Success = true);

public sealed record KaDeleteApplicationOutputResult(bool Success = true);

public sealed record KaDeleteApplicationReferenceDataSourceResult(bool Success = true);

public sealed record KaAddApplicationVpcConfigurationResult(
    VpcConfigurationDescription? VpcConfigurationDescription = null);

public sealed record KaDeleteApplicationVpcConfigurationResult(bool Success = true);

public sealed record KaAddApplicationCloudWatchLoggingOptionResult(
    List<CloudWatchLoggingOptionDescription>?
        CloudWatchLoggingOptionDescriptions = null);

public sealed record KaDeleteApplicationCloudWatchLoggingOptionResult(
    bool Success = true);

public sealed record KaDescribeApplicationVersionResult(
    ApplicationDetail? ApplicationDetail = null);

public sealed record KaListApplicationVersionsResult(
    List<ApplicationVersionSummary>? ApplicationVersionSummaries = null,
    string? NextToken = null);

public sealed record KaRollbackApplicationResult(
    string? ApplicationArn = null,
    long? ApplicationVersionId = null);

public sealed record KaCreateApplicationSnapshotResult(bool Success = true);
public sealed record KaDeleteApplicationSnapshotResult(bool Success = true);

public sealed record KaDescribeApplicationSnapshotResult(
    SnapshotDetails? SnapshotDetails = null);

public sealed record KaListApplicationSnapshotsResult(
    List<SnapshotDetails>? SnapshotDetails = null,
    string? NextToken = null);

public sealed record KaDiscoverInputSchemaResult(
    SourceSchema? InputSchema = null,
    List<List<string>>? ParsedInputRecords = null,
    List<string>? RawInputRecords = null);

public sealed record KaTagResourceResult(bool Success = true);
public sealed record KaUntagResourceResult(bool Success = true);

public sealed record KaListTagsForResourceResult(
    List<Tag>? Tags = null);

public sealed record KaCreateApplicationPresignedUrlResult(
    string? AuthorizedUrl = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Kinesis Analytics V2.
/// </summary>
public static class KinesisAnalyticsService
{
    private static AmazonKinesisAnalyticsV2Client GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKinesisAnalyticsV2Client>(region);

    /// <summary>
    /// Create a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaCreateApplicationResult> CreateApplicationAsync(
        CreateApplicationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateApplicationAsync(request);
            return new KaCreateApplicationResult(
                ApplicationName: resp.ApplicationDetail?.ApplicationName,
                ApplicationArn: resp.ApplicationDetail?.ApplicationARN,
                ApplicationStatus: resp.ApplicationDetail?.ApplicationStatus?.Value,
                ApplicationVersionId: resp.ApplicationDetail?.ApplicationVersionId);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Kinesis Analytics application");
        }
    }

    /// <summary>
    /// Delete a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationResult> DeleteApplicationAsync(
        string applicationName,
        DateTime createTimestamp,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationAsync(new DeleteApplicationRequest
            {
                ApplicationName = applicationName,
                CreateTimestamp = createTimestamp
            });
            return new KaDeleteApplicationResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Kinesis Analytics application '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDescribeApplicationResult> DescribeApplicationAsync(
        string applicationName,
        bool? includeAdditionalDetails = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (includeAdditionalDetails.HasValue)
            request.IncludeAdditionalDetails = includeAdditionalDetails.Value;

        try
        {
            var resp = await client.DescribeApplicationAsync(request);
            return new KaDescribeApplicationResult(
                ApplicationDetail: resp.ApplicationDetail);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Kinesis Analytics application '{applicationName}'");
        }
    }

    /// <summary>
    /// List Kinesis Analytics applications.
    /// </summary>
    public static async Task<KaListApplicationsResult> ListApplicationsAsync(
        string? nextToken = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApplicationsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListApplicationsAsync(request);
            return new KaListApplicationsResult(
                ApplicationSummaries: resp.ApplicationSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Kinesis Analytics applications");
        }
    }

    /// <summary>
    /// Update a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaUpdateApplicationResult> UpdateApplicationAsync(
        UpdateApplicationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateApplicationAsync(request);
            return new KaUpdateApplicationResult(
                ApplicationName: resp.ApplicationDetail?.ApplicationName,
                ApplicationArn: resp.ApplicationDetail?.ApplicationARN,
                ApplicationVersionId: resp.ApplicationDetail?.ApplicationVersionId);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Kinesis Analytics application");
        }
    }

    /// <summary>
    /// Start a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaStartApplicationResult> StartApplicationAsync(
        string applicationName,
        RunConfiguration? runConfiguration = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (runConfiguration != null) request.RunConfiguration = runConfiguration;

        try
        {
            await client.StartApplicationAsync(request);
            return new KaStartApplicationResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start Kinesis Analytics application '{applicationName}'");
        }
    }

    /// <summary>
    /// Stop a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaStopApplicationResult> StopApplicationAsync(
        string applicationName,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            await client.StopApplicationAsync(request);
            return new KaStopApplicationResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Kinesis Analytics application '{applicationName}'");
        }
    }

    /// <summary>
    /// Add an input to a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaAddApplicationInputResult> AddApplicationInputAsync(
        string applicationName,
        long currentApplicationVersionId,
        Input input,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddApplicationInputAsync(
                new AddApplicationInputRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    Input = input
                });
            return new KaAddApplicationInputResult(
                InputDescriptions: resp.InputDescriptions);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add input to application '{applicationName}'");
        }
    }

    /// <summary>
    /// Add an output to a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaAddApplicationOutputResult> AddApplicationOutputAsync(
        string applicationName,
        long currentApplicationVersionId,
        Output output,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddApplicationOutputAsync(
                new AddApplicationOutputRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    Output = output
                });
            return new KaAddApplicationOutputResult(
                OutputDescriptions: resp.OutputDescriptions);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add output to application '{applicationName}'");
        }
    }

    /// <summary>
    /// Add a reference data source to a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaAddApplicationReferenceDataSourceResult>
        AddApplicationReferenceDataSourceAsync(
            string applicationName,
            long currentApplicationVersionId,
            ReferenceDataSource referenceDataSource,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddApplicationReferenceDataSourceAsync(
                new AddApplicationReferenceDataSourceRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    ReferenceDataSource = referenceDataSource
                });
            return new KaAddApplicationReferenceDataSourceResult(
                ReferenceDataSourceDescriptions: resp.ReferenceDataSourceDescriptions);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add reference data source to application '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete an input processing configuration from a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationInputProcessingConfigurationResult>
        DeleteApplicationInputProcessingConfigurationAsync(
            string applicationName,
            long currentApplicationVersionId,
            string inputId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationInputProcessingConfigurationAsync(
                new DeleteApplicationInputProcessingConfigurationRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    InputId = inputId
                });
            return new KaDeleteApplicationInputProcessingConfigurationResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete input processing configuration from '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete an output from a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationOutputResult>
        DeleteApplicationOutputAsync(
            string applicationName,
            long currentApplicationVersionId,
            string outputId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationOutputAsync(
                new DeleteApplicationOutputRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    OutputId = outputId
                });
            return new KaDeleteApplicationOutputResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete output from application '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete a reference data source from a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationReferenceDataSourceResult>
        DeleteApplicationReferenceDataSourceAsync(
            string applicationName,
            long currentApplicationVersionId,
            string referenceId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationReferenceDataSourceAsync(
                new DeleteApplicationReferenceDataSourceRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    ReferenceId = referenceId
                });
            return new KaDeleteApplicationReferenceDataSourceResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete reference data source from '{applicationName}'");
        }
    }

    /// <summary>
    /// Add a VPC configuration to a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaAddApplicationVpcConfigurationResult>
        AddApplicationVpcConfigurationAsync(
            string applicationName,
            long currentApplicationVersionId,
            VpcConfiguration vpcConfiguration,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddApplicationVpcConfigurationAsync(
                new AddApplicationVpcConfigurationRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    VpcConfiguration = vpcConfiguration
                });
            return new KaAddApplicationVpcConfigurationResult(
                VpcConfigurationDescription: resp.VpcConfigurationDescription);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add VPC configuration to application '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete a VPC configuration from a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationVpcConfigurationResult>
        DeleteApplicationVpcConfigurationAsync(
            string applicationName,
            long currentApplicationVersionId,
            string vpcConfigurationId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationVpcConfigurationAsync(
                new DeleteApplicationVpcConfigurationRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    VpcConfigurationId = vpcConfigurationId
                });
            return new KaDeleteApplicationVpcConfigurationResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete VPC configuration from '{applicationName}'");
        }
    }

    /// <summary>
    /// Add a CloudWatch logging option to a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaAddApplicationCloudWatchLoggingOptionResult>
        AddApplicationCloudWatchLoggingOptionAsync(
            string applicationName,
            long currentApplicationVersionId,
            CloudWatchLoggingOption cloudWatchLoggingOption,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddApplicationCloudWatchLoggingOptionAsync(
                new AddApplicationCloudWatchLoggingOptionRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    CloudWatchLoggingOption = cloudWatchLoggingOption
                });
            return new KaAddApplicationCloudWatchLoggingOptionResult(
                CloudWatchLoggingOptionDescriptions:
                    resp.CloudWatchLoggingOptionDescriptions);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add CloudWatch logging to application '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete a CloudWatch logging option from a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationCloudWatchLoggingOptionResult>
        DeleteApplicationCloudWatchLoggingOptionAsync(
            string applicationName,
            long currentApplicationVersionId,
            string cloudWatchLoggingOptionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationCloudWatchLoggingOptionAsync(
                new DeleteApplicationCloudWatchLoggingOptionRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId,
                    CloudWatchLoggingOptionId = cloudWatchLoggingOptionId
                });
            return new KaDeleteApplicationCloudWatchLoggingOptionResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CloudWatch logging from application '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe a specific version of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDescribeApplicationVersionResult>
        DescribeApplicationVersionAsync(
            string applicationName,
            long applicationVersionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeApplicationVersionAsync(
                new DescribeApplicationVersionRequest
                {
                    ApplicationName = applicationName,
                    ApplicationVersionId = applicationVersionId
                });
            return new KaDescribeApplicationVersionResult(
                ApplicationDetail: resp.ApplicationVersionDetail);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe version {applicationVersionId} of '{applicationName}'");
        }
    }

    /// <summary>
    /// List versions of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaListApplicationVersionsResult>
        ListApplicationVersionsAsync(
            string applicationName,
            string? nextToken = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApplicationVersionsRequest
        {
            ApplicationName = applicationName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListApplicationVersionsAsync(request);
            return new KaListApplicationVersionsResult(
                ApplicationVersionSummaries: resp.ApplicationVersionSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list versions for application '{applicationName}'");
        }
    }

    /// <summary>
    /// Roll back a Kinesis Analytics application to the previous version.
    /// </summary>
    public static async Task<KaRollbackApplicationResult> RollbackApplicationAsync(
        string applicationName,
        long currentApplicationVersionId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RollbackApplicationAsync(
                new RollbackApplicationRequest
                {
                    ApplicationName = applicationName,
                    CurrentApplicationVersionId = currentApplicationVersionId
                });
            return new KaRollbackApplicationResult(
                ApplicationArn: resp.ApplicationDetail?.ApplicationARN,
                ApplicationVersionId: resp.ApplicationDetail?.ApplicationVersionId);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to roll back application '{applicationName}'");
        }
    }

    /// <summary>
    /// Create a snapshot of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaCreateApplicationSnapshotResult>
        CreateApplicationSnapshotAsync(
            string applicationName,
            string snapshotName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateApplicationSnapshotAsync(
                new CreateApplicationSnapshotRequest
                {
                    ApplicationName = applicationName,
                    SnapshotName = snapshotName
                });
            return new KaCreateApplicationSnapshotResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create snapshot '{snapshotName}' for '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete a snapshot of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDeleteApplicationSnapshotResult>
        DeleteApplicationSnapshotAsync(
            string applicationName,
            string snapshotName,
            DateTime snapshotCreationTimestamp,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationSnapshotAsync(
                new DeleteApplicationSnapshotRequest
                {
                    ApplicationName = applicationName,
                    SnapshotName = snapshotName,
                    SnapshotCreationTimestamp = snapshotCreationTimestamp
                });
            return new KaDeleteApplicationSnapshotResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete snapshot '{snapshotName}' for '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe a snapshot of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaDescribeApplicationSnapshotResult>
        DescribeApplicationSnapshotAsync(
            string applicationName,
            string snapshotName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeApplicationSnapshotAsync(
                new DescribeApplicationSnapshotRequest
                {
                    ApplicationName = applicationName,
                    SnapshotName = snapshotName
                });
            return new KaDescribeApplicationSnapshotResult(
                SnapshotDetails: resp.SnapshotDetails);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe snapshot '{snapshotName}' for '{applicationName}'");
        }
    }

    /// <summary>
    /// List snapshots of a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaListApplicationSnapshotsResult>
        ListApplicationSnapshotsAsync(
            string applicationName,
            string? nextToken = null,
            int? limit = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApplicationSnapshotsRequest
        {
            ApplicationName = applicationName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (limit.HasValue) request.Limit = limit.Value;

        try
        {
            var resp = await client.ListApplicationSnapshotsAsync(request);
            return new KaListApplicationSnapshotsResult(
                SnapshotDetails: resp.SnapshotSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list snapshots for application '{applicationName}'");
        }
    }

    /// <summary>
    /// Discover the input schema for a streaming source.
    /// </summary>
    public static async Task<KaDiscoverInputSchemaResult> DiscoverInputSchemaAsync(
        DiscoverInputSchemaRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DiscoverInputSchemaAsync(request);
            return new KaDiscoverInputSchemaResult(
                InputSchema: resp.InputSchema,
                ParsedInputRecords: resp.ParsedInputRecords,
                RawInputRecords: resp.RawInputRecords);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to discover input schema");
        }
    }

    /// <summary>
    /// Tag a Kinesis Analytics resource.
    /// </summary>
    public static async Task<KaTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags
            });
            return new KaTagResourceResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Kinesis Analytics resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Kinesis Analytics resource.
    /// </summary>
    public static async Task<KaUntagResourceResult> UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
            return new KaUntagResourceResult();
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Kinesis Analytics resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Kinesis Analytics resource.
    /// </summary>
    public static async Task<KaListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceARN = resourceArn });
            return new KaListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Kinesis Analytics resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Create a pre-signed URL for a Kinesis Analytics application.
    /// </summary>
    public static async Task<KaCreateApplicationPresignedUrlResult>
        CreateApplicationPresignedUrlAsync(
            string applicationName,
            UrlType urlType,
            long? sessionExpirationDurationInSeconds = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateApplicationPresignedUrlRequest
        {
            ApplicationName = applicationName,
            UrlType = urlType
        };
        if (sessionExpirationDurationInSeconds.HasValue)
            request.SessionExpirationDurationInSeconds =
                sessionExpirationDurationInSeconds.Value;

        try
        {
            var resp = await client.CreateApplicationPresignedUrlAsync(request);
            return new KaCreateApplicationPresignedUrlResult(
                AuthorizedUrl: resp.AuthorizedUrl);
        }
        catch (AmazonKinesisAnalyticsV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create pre-signed URL for application '{applicationName}'");
        }
    }
}
