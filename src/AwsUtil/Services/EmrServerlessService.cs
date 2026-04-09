using Amazon;
using Amazon.EMRServerless;
using Amazon.EMRServerless.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record EmrsCreateApplicationResult(
    string? ApplicationId = null,
    string? Name = null,
    string? Arn = null);

public sealed record EmrsDeleteApplicationResult(bool Success = true);

public sealed record EmrsGetApplicationResult(
    Application? Application = null);

public sealed record EmrsListApplicationsResult(
    List<ApplicationSummary>? Applications = null,
    string? NextToken = null);

public sealed record EmrsUpdateApplicationResult(
    Application? Application = null);

public sealed record EmrsStartApplicationResult(bool Success = true);
public sealed record EmrsStopApplicationResult(bool Success = true);

public sealed record EmrsStartJobRunResult(
    string? ApplicationId = null,
    string? JobRunId = null,
    string? Arn = null);

public sealed record EmrsCancelJobRunResult(
    string? ApplicationId = null,
    string? JobRunId = null);

public sealed record EmrsGetJobRunResult(
    JobRun? JobRun = null);

public sealed record EmrsListJobRunsResult(
    List<JobRunSummary>? JobRuns = null,
    string? NextToken = null);

public sealed record EmrsGetDashboardForJobRunResult(
    string? Url = null);

public sealed record EmrsTagResourceResult(bool Success = true);
public sealed record EmrsUntagResourceResult(bool Success = true);

public sealed record EmrsListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon EMR Serverless.
/// </summary>
public static class EmrServerlessService
{
    private static AmazonEMRServerlessClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEMRServerlessClient>(region);

    /// <summary>Create an EMR Serverless application.</summary>
    public static async Task<EmrsCreateApplicationResult> CreateApplicationAsync(
        CreateApplicationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateApplicationAsync(request);
            return new EmrsCreateApplicationResult(
                ApplicationId: resp.ApplicationId,
                Name: resp.Name,
                Arn: resp.Arn);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create EMR Serverless application");
        }
    }

    /// <summary>Delete an EMR Serverless application.</summary>
    public static async Task<EmrsDeleteApplicationResult> DeleteApplicationAsync(
        string applicationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationAsync(
                new DeleteApplicationRequest { ApplicationId = applicationId });
            return new EmrsDeleteApplicationResult();
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EMR Serverless application '{applicationId}'");
        }
    }

    /// <summary>Get details of an EMR Serverless application.</summary>
    public static async Task<EmrsGetApplicationResult> GetApplicationAsync(
        string applicationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetApplicationAsync(
                new GetApplicationRequest { ApplicationId = applicationId });
            return new EmrsGetApplicationResult(Application: resp.Application);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get EMR Serverless application '{applicationId}'");
        }
    }

    /// <summary>List EMR Serverless applications.</summary>
    public static async Task<EmrsListApplicationsResult> ListApplicationsAsync(
        List<string>? states = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApplicationsRequest();
        if (states != null) request.States = states;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListApplicationsAsync(request);
            return new EmrsListApplicationsResult(
                Applications: resp.Applications,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR Serverless applications");
        }
    }

    /// <summary>Update an EMR Serverless application.</summary>
    public static async Task<EmrsUpdateApplicationResult> UpdateApplicationAsync(
        UpdateApplicationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateApplicationAsync(request);
            return new EmrsUpdateApplicationResult(Application: resp.Application);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update EMR Serverless application");
        }
    }

    /// <summary>Start an EMR Serverless application.</summary>
    public static async Task<EmrsStartApplicationResult> StartApplicationAsync(
        string applicationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartApplicationAsync(
                new StartApplicationRequest { ApplicationId = applicationId });
            return new EmrsStartApplicationResult();
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start EMR Serverless application '{applicationId}'");
        }
    }

    /// <summary>Stop an EMR Serverless application.</summary>
    public static async Task<EmrsStopApplicationResult> StopApplicationAsync(
        string applicationId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopApplicationAsync(
                new StopApplicationRequest { ApplicationId = applicationId });
            return new EmrsStopApplicationResult();
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop EMR Serverless application '{applicationId}'");
        }
    }

    /// <summary>Start a job run on an EMR Serverless application.</summary>
    public static async Task<EmrsStartJobRunResult> StartJobRunAsync(
        StartJobRunRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartJobRunAsync(request);
            return new EmrsStartJobRunResult(
                ApplicationId: resp.ApplicationId,
                JobRunId: resp.JobRunId,
                Arn: resp.Arn);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start EMR Serverless job run");
        }
    }

    /// <summary>Cancel a job run.</summary>
    public static async Task<EmrsCancelJobRunResult> CancelJobRunAsync(
        string applicationId,
        string jobRunId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelJobRunAsync(new CancelJobRunRequest
            {
                ApplicationId = applicationId,
                JobRunId = jobRunId
            });
            return new EmrsCancelJobRunResult(
                ApplicationId: resp.ApplicationId,
                JobRunId: resp.JobRunId);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel job run '{jobRunId}'");
        }
    }

    /// <summary>Get details of a job run.</summary>
    public static async Task<EmrsGetJobRunResult> GetJobRunAsync(
        string applicationId,
        string jobRunId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetJobRunAsync(new GetJobRunRequest
            {
                ApplicationId = applicationId,
                JobRunId = jobRunId
            });
            return new EmrsGetJobRunResult(JobRun: resp.JobRun);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get job run '{jobRunId}'");
        }
    }

    /// <summary>List job runs for an EMR Serverless application.</summary>
    public static async Task<EmrsListJobRunsResult> ListJobRunsAsync(
        string applicationId,
        List<string>? states = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListJobRunsRequest
        {
            ApplicationId = applicationId
        };
        if (states != null) request.States = states;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListJobRunsAsync(request);
            return new EmrsListJobRunsResult(
                JobRuns: resp.JobRuns,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR Serverless job runs");
        }
    }

    /// <summary>Get the dashboard URL for a job run.</summary>
    public static async Task<EmrsGetDashboardForJobRunResult>
        GetDashboardForJobRunAsync(
            string applicationId,
            string jobRunId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDashboardForJobRunAsync(
                new GetDashboardForJobRunRequest
                {
                    ApplicationId = applicationId,
                    JobRunId = jobRunId
                });
            return new EmrsGetDashboardForJobRunResult(Url: resp.Url);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get dashboard for job run '{jobRunId}'");
        }
    }

    /// <summary>Tag an EMR Serverless resource.</summary>
    public static async Task<EmrsTagResourceResult> TagResourceAsync(
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
            return new EmrsTagResourceResult();
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag EMR Serverless resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from an EMR Serverless resource.</summary>
    public static async Task<EmrsUntagResourceResult> UntagResourceAsync(
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
            return new EmrsUntagResourceResult();
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag EMR Serverless resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for an EMR Serverless resource.</summary>
    public static async Task<EmrsListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new EmrsListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonEMRServerlessException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for EMR Serverless resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateApplicationAsync"/>.</summary>
    public static EmrsCreateApplicationResult CreateApplication(CreateApplicationRequest request, RegionEndpoint? region = null)
        => CreateApplicationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteApplicationAsync"/>.</summary>
    public static EmrsDeleteApplicationResult DeleteApplication(string applicationId, RegionEndpoint? region = null)
        => DeleteApplicationAsync(applicationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetApplicationAsync"/>.</summary>
    public static EmrsGetApplicationResult GetApplication(string applicationId, RegionEndpoint? region = null)
        => GetApplicationAsync(applicationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListApplicationsAsync"/>.</summary>
    public static EmrsListApplicationsResult ListApplications(List<string>? states = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListApplicationsAsync(states, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateApplicationAsync"/>.</summary>
    public static EmrsUpdateApplicationResult UpdateApplication(UpdateApplicationRequest request, RegionEndpoint? region = null)
        => UpdateApplicationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartApplicationAsync"/>.</summary>
    public static EmrsStartApplicationResult StartApplication(string applicationId, RegionEndpoint? region = null)
        => StartApplicationAsync(applicationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopApplicationAsync"/>.</summary>
    public static EmrsStopApplicationResult StopApplication(string applicationId, RegionEndpoint? region = null)
        => StopApplicationAsync(applicationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartJobRunAsync"/>.</summary>
    public static EmrsStartJobRunResult StartJobRun(StartJobRunRequest request, RegionEndpoint? region = null)
        => StartJobRunAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelJobRunAsync"/>.</summary>
    public static EmrsCancelJobRunResult CancelJobRun(string applicationId, string jobRunId, RegionEndpoint? region = null)
        => CancelJobRunAsync(applicationId, jobRunId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetJobRunAsync"/>.</summary>
    public static EmrsGetJobRunResult GetJobRun(string applicationId, string jobRunId, RegionEndpoint? region = null)
        => GetJobRunAsync(applicationId, jobRunId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobRunsAsync"/>.</summary>
    public static EmrsListJobRunsResult ListJobRuns(string applicationId, List<string>? states = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListJobRunsAsync(applicationId, states, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDashboardForJobRunAsync"/>.</summary>
    public static EmrsGetDashboardForJobRunResult GetDashboardForJobRun(string applicationId, string jobRunId, RegionEndpoint? region = null)
        => GetDashboardForJobRunAsync(applicationId, jobRunId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static EmrsTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static EmrsUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static EmrsListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
