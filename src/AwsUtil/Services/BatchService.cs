using Amazon;
using Amazon.Batch;
using Amazon.Batch.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of CreateComputeEnvironment.</summary>
public sealed record CreateComputeEnvironmentResult(
    string? ComputeEnvironmentName = null,
    string? ComputeEnvironmentArn = null);

/// <summary>Result of DeleteComputeEnvironment (void operation).</summary>
public sealed record DeleteComputeEnvironmentResult(bool Success = true);

/// <summary>Result of DescribeComputeEnvironments.</summary>
public sealed record DescribeComputeEnvironmentsResult(
    List<ComputeEnvironmentDetail>? ComputeEnvironments = null,
    string? NextToken = null);

/// <summary>Result of UpdateComputeEnvironment.</summary>
public sealed record UpdateComputeEnvironmentResult(
    string? ComputeEnvironmentName = null,
    string? ComputeEnvironmentArn = null);

/// <summary>Result of CreateJobQueue.</summary>
public sealed record CreateJobQueueResult(
    string? JobQueueName = null,
    string? JobQueueArn = null);

/// <summary>Result of DeleteJobQueue (void operation).</summary>
public sealed record DeleteJobQueueResult(bool Success = true);

/// <summary>Result of DescribeJobQueues.</summary>
public sealed record DescribeJobQueuesResult(
    List<JobQueueDetail>? JobQueues = null,
    string? NextToken = null);

/// <summary>Result of UpdateJobQueue.</summary>
public sealed record UpdateJobQueueResult(
    string? JobQueueName = null,
    string? JobQueueArn = null);

/// <summary>Result of RegisterJobDefinition.</summary>
public sealed record RegisterJobDefinitionResult(
    string? JobDefinitionName = null,
    string? JobDefinitionArn = null,
    int? Revision = null);

/// <summary>Result of DeregisterJobDefinition (void operation).</summary>
public sealed record DeregisterJobDefinitionResult(bool Success = true);

/// <summary>Result of DescribeJobDefinitions.</summary>
public sealed record DescribeJobDefinitionsResult(
    List<JobDefinition>? JobDefinitions = null,
    string? NextToken = null);

/// <summary>Result of SubmitJob.</summary>
public sealed record SubmitJobResult(
    string? JobName = null,
    string? JobId = null,
    string? JobArn = null);

/// <summary>Result of CancelJob (void operation).</summary>
public sealed record CancelBatchJobResult(bool Success = true);

/// <summary>Result of TerminateJob (void operation).</summary>
public sealed record TerminateBatchJobResult(bool Success = true);

/// <summary>Result of DescribeJobs.</summary>
public sealed record DescribeJobsResult(List<JobDetail>? Jobs = null);

/// <summary>Result of ListJobs.</summary>
public sealed record ListBatchJobsResult(
    List<JobSummary>? JobSummaryList = null,
    string? NextToken = null);

/// <summary>Result of CreateSchedulingPolicy.</summary>
public sealed record CreateSchedulingPolicyResult(
    string? Name = null,
    string? Arn = null);

/// <summary>Result of DeleteSchedulingPolicy (void operation).</summary>
public sealed record DeleteSchedulingPolicyResult(bool Success = true);

/// <summary>Result of DescribeSchedulingPolicies.</summary>
public sealed record DescribeSchedulingPoliciesResult(
    List<SchedulingPolicyDetail>? SchedulingPolicies = null);

/// <summary>Result of ListSchedulingPolicies.</summary>
public sealed record ListSchedulingPoliciesResult(
    List<SchedulingPolicyListingDetail>? SchedulingPolicies = null,
    string? NextToken = null);

/// <summary>Result of UpdateSchedulingPolicy (void operation).</summary>
public sealed record UpdateSchedulingPolicyResult(bool Success = true);

/// <summary>Result of TagResource (void operation).</summary>
public sealed record BatchTagResourceResult(bool Success = true);

/// <summary>Result of UntagResource (void operation).</summary>
public sealed record BatchUntagResourceResult(bool Success = true);

/// <summary>Result of ListTagsForResource.</summary>
public sealed record BatchListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for AWS Batch.
/// </summary>
public static class BatchService
{
    private static AmazonBatchClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonBatchClient>(region);

    // -----------------------------------------------------------------------
    // Compute environments
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a Batch compute environment.
    /// </summary>
    public static async Task<CreateComputeEnvironmentResult> CreateComputeEnvironmentAsync(
        CreateComputeEnvironmentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateComputeEnvironmentAsync(request);
            return new CreateComputeEnvironmentResult(
                ComputeEnvironmentName: resp.ComputeEnvironmentName,
                ComputeEnvironmentArn: resp.ComputeEnvironmentArn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create compute environment");
        }
    }

    /// <summary>
    /// Delete a Batch compute environment.
    /// </summary>
    public static async Task<DeleteComputeEnvironmentResult> DeleteComputeEnvironmentAsync(
        string computeEnvironment,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteComputeEnvironmentAsync(new DeleteComputeEnvironmentRequest
            {
                ComputeEnvironment = computeEnvironment
            });
            return new DeleteComputeEnvironmentResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete compute environment '{computeEnvironment}'");
        }
    }

    /// <summary>
    /// Describe Batch compute environments.
    /// </summary>
    public static async Task<DescribeComputeEnvironmentsResult>
        DescribeComputeEnvironmentsAsync(
            DescribeComputeEnvironmentsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeComputeEnvironmentsAsync(request);
            return new DescribeComputeEnvironmentsResult(
                ComputeEnvironments: resp.ComputeEnvironments,
                NextToken: resp.NextToken);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe compute environments");
        }
    }

    /// <summary>
    /// Update a Batch compute environment.
    /// </summary>
    public static async Task<UpdateComputeEnvironmentResult> UpdateComputeEnvironmentAsync(
        UpdateComputeEnvironmentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateComputeEnvironmentAsync(request);
            return new UpdateComputeEnvironmentResult(
                ComputeEnvironmentName: resp.ComputeEnvironmentName,
                ComputeEnvironmentArn: resp.ComputeEnvironmentArn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update compute environment");
        }
    }

    // -----------------------------------------------------------------------
    // Job queues
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a Batch job queue.
    /// </summary>
    public static async Task<CreateJobQueueResult> CreateJobQueueAsync(
        CreateJobQueueRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateJobQueueAsync(request);
            return new CreateJobQueueResult(
                JobQueueName: resp.JobQueueName,
                JobQueueArn: resp.JobQueueArn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create job queue");
        }
    }

    /// <summary>
    /// Delete a Batch job queue.
    /// </summary>
    public static async Task<DeleteJobQueueResult> DeleteJobQueueAsync(
        string jobQueue,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteJobQueueAsync(new DeleteJobQueueRequest
            {
                JobQueue = jobQueue
            });
            return new DeleteJobQueueResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete job queue '{jobQueue}'");
        }
    }

    /// <summary>
    /// Describe Batch job queues.
    /// </summary>
    public static async Task<DescribeJobQueuesResult> DescribeJobQueuesAsync(
        DescribeJobQueuesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobQueuesAsync(request);
            return new DescribeJobQueuesResult(
                JobQueues: resp.JobQueues,
                NextToken: resp.NextToken);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe job queues");
        }
    }

    /// <summary>
    /// Update a Batch job queue.
    /// </summary>
    public static async Task<UpdateJobQueueResult> UpdateJobQueueAsync(
        UpdateJobQueueRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateJobQueueAsync(request);
            return new UpdateJobQueueResult(
                JobQueueName: resp.JobQueueName,
                JobQueueArn: resp.JobQueueArn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update job queue");
        }
    }

    // -----------------------------------------------------------------------
    // Job definitions
    // -----------------------------------------------------------------------

    /// <summary>
    /// Register a Batch job definition.
    /// </summary>
    public static async Task<RegisterJobDefinitionResult> RegisterJobDefinitionAsync(
        RegisterJobDefinitionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RegisterJobDefinitionAsync(request);
            return new RegisterJobDefinitionResult(
                JobDefinitionName: resp.JobDefinitionName,
                JobDefinitionArn: resp.JobDefinitionArn,
                Revision: resp.Revision);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to register job definition");
        }
    }

    /// <summary>
    /// Deregister a Batch job definition.
    /// </summary>
    public static async Task<DeregisterJobDefinitionResult> DeregisterJobDefinitionAsync(
        string jobDefinition,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeregisterJobDefinitionAsync(new DeregisterJobDefinitionRequest
            {
                JobDefinition = jobDefinition
            });
            return new DeregisterJobDefinitionResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister job definition '{jobDefinition}'");
        }
    }

    /// <summary>
    /// Describe Batch job definitions.
    /// </summary>
    public static async Task<DescribeJobDefinitionsResult> DescribeJobDefinitionsAsync(
        DescribeJobDefinitionsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobDefinitionsAsync(request);
            return new DescribeJobDefinitionsResult(
                JobDefinitions: resp.JobDefinitions,
                NextToken: resp.NextToken);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe job definitions");
        }
    }

    // -----------------------------------------------------------------------
    // Jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Submit a Batch job.
    /// </summary>
    public static async Task<SubmitJobResult> SubmitJobAsync(
        SubmitJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SubmitJobAsync(request);
            return new SubmitJobResult(
                JobName: resp.JobName,
                JobId: resp.JobId,
                JobArn: resp.JobArn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to submit job");
        }
    }

    /// <summary>
    /// Cancel a Batch job.
    /// </summary>
    public static async Task<CancelBatchJobResult> CancelJobAsync(
        string jobId,
        string reason,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CancelJobAsync(new CancelJobRequest
            {
                JobId = jobId,
                Reason = reason
            });
            return new CancelBatchJobResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel job '{jobId}'");
        }
    }

    /// <summary>
    /// Terminate a Batch job.
    /// </summary>
    public static async Task<TerminateBatchJobResult> TerminateJobAsync(
        string jobId,
        string reason,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TerminateJobAsync(new TerminateJobRequest
            {
                JobId = jobId,
                Reason = reason
            });
            return new TerminateBatchJobResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to terminate job '{jobId}'");
        }
    }

    /// <summary>
    /// Describe Batch jobs.
    /// </summary>
    public static async Task<DescribeJobsResult> DescribeJobsAsync(
        List<string> jobs,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobsAsync(new DescribeJobsRequest
            {
                Jobs = jobs
            });
            return new DescribeJobsResult(Jobs: resp.Jobs);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe jobs");
        }
    }

    /// <summary>
    /// List Batch jobs in a job queue.
    /// </summary>
    public static async Task<ListBatchJobsResult> ListJobsAsync(
        ListJobsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListJobsAsync(request);
            return new ListBatchJobsResult(
                JobSummaryList: resp.JobSummaryList,
                NextToken: resp.NextToken);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list jobs");
        }
    }

    // -----------------------------------------------------------------------
    // Scheduling policies
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a Batch scheduling policy.
    /// </summary>
    public static async Task<CreateSchedulingPolicyResult> CreateSchedulingPolicyAsync(
        CreateSchedulingPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSchedulingPolicyAsync(request);
            return new CreateSchedulingPolicyResult(
                Name: resp.Name,
                Arn: resp.Arn);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create scheduling policy");
        }
    }

    /// <summary>
    /// Delete a Batch scheduling policy.
    /// </summary>
    public static async Task<DeleteSchedulingPolicyResult> DeleteSchedulingPolicyAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSchedulingPolicyAsync(new DeleteSchedulingPolicyRequest
            {
                Arn = arn
            });
            return new DeleteSchedulingPolicyResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete scheduling policy '{arn}'");
        }
    }

    /// <summary>
    /// Describe Batch scheduling policies.
    /// </summary>
    public static async Task<DescribeSchedulingPoliciesResult> DescribeSchedulingPoliciesAsync(
        List<string> arns,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSchedulingPoliciesAsync(
                new DescribeSchedulingPoliciesRequest { Arns = arns });
            return new DescribeSchedulingPoliciesResult(
                SchedulingPolicies: resp.SchedulingPolicies);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe scheduling policies");
        }
    }

    /// <summary>
    /// List Batch scheduling policies.
    /// </summary>
    public static async Task<ListSchedulingPoliciesResult> ListSchedulingPoliciesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSchedulingPoliciesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSchedulingPoliciesAsync(request);
            return new ListSchedulingPoliciesResult(
                SchedulingPolicies: resp.SchedulingPolicies,
                NextToken: resp.NextToken);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list scheduling policies");
        }
    }

    /// <summary>
    /// Update a Batch scheduling policy.
    /// </summary>
    public static async Task<UpdateSchedulingPolicyResult> UpdateSchedulingPolicyAsync(
        UpdateSchedulingPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateSchedulingPolicyAsync(request);
            return new UpdateSchedulingPolicyResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update scheduling policy");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Tag a Batch resource.
    /// </summary>
    public static async Task<BatchTagResourceResult> TagResourceAsync(
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
            return new BatchTagResourceResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Batch resource.
    /// </summary>
    public static async Task<BatchUntagResourceResult> UntagResourceAsync(
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
            return new BatchUntagResourceResult();
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Batch resource.
    /// </summary>
    public static async Task<BatchListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new BatchListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonBatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateComputeEnvironmentAsync"/>.</summary>
    public static CreateComputeEnvironmentResult CreateComputeEnvironment(CreateComputeEnvironmentRequest request, RegionEndpoint? region = null)
        => CreateComputeEnvironmentAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteComputeEnvironmentAsync"/>.</summary>
    public static DeleteComputeEnvironmentResult DeleteComputeEnvironment(string computeEnvironment, RegionEndpoint? region = null)
        => DeleteComputeEnvironmentAsync(computeEnvironment, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeComputeEnvironmentsAsync"/>.</summary>
    public static DescribeComputeEnvironmentsResult DescribeComputeEnvironments(DescribeComputeEnvironmentsRequest request, RegionEndpoint? region = null)
        => DescribeComputeEnvironmentsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateComputeEnvironmentAsync"/>.</summary>
    public static UpdateComputeEnvironmentResult UpdateComputeEnvironment(UpdateComputeEnvironmentRequest request, RegionEndpoint? region = null)
        => UpdateComputeEnvironmentAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateJobQueueAsync"/>.</summary>
    public static CreateJobQueueResult CreateJobQueue(CreateJobQueueRequest request, RegionEndpoint? region = null)
        => CreateJobQueueAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteJobQueueAsync"/>.</summary>
    public static DeleteJobQueueResult DeleteJobQueue(string jobQueue, RegionEndpoint? region = null)
        => DeleteJobQueueAsync(jobQueue, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobQueuesAsync"/>.</summary>
    public static DescribeJobQueuesResult DescribeJobQueues(DescribeJobQueuesRequest request, RegionEndpoint? region = null)
        => DescribeJobQueuesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateJobQueueAsync"/>.</summary>
    public static UpdateJobQueueResult UpdateJobQueue(UpdateJobQueueRequest request, RegionEndpoint? region = null)
        => UpdateJobQueueAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterJobDefinitionAsync"/>.</summary>
    public static RegisterJobDefinitionResult RegisterJobDefinition(RegisterJobDefinitionRequest request, RegionEndpoint? region = null)
        => RegisterJobDefinitionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterJobDefinitionAsync"/>.</summary>
    public static DeregisterJobDefinitionResult DeregisterJobDefinition(string jobDefinition, RegionEndpoint? region = null)
        => DeregisterJobDefinitionAsync(jobDefinition, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobDefinitionsAsync"/>.</summary>
    public static DescribeJobDefinitionsResult DescribeJobDefinitions(DescribeJobDefinitionsRequest request, RegionEndpoint? region = null)
        => DescribeJobDefinitionsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SubmitJobAsync"/>.</summary>
    public static SubmitJobResult SubmitJob(SubmitJobRequest request, RegionEndpoint? region = null)
        => SubmitJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelJobAsync"/>.</summary>
    public static CancelBatchJobResult CancelJob(string jobId, string reason, RegionEndpoint? region = null)
        => CancelJobAsync(jobId, reason, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TerminateJobAsync"/>.</summary>
    public static TerminateBatchJobResult TerminateJob(string jobId, string reason, RegionEndpoint? region = null)
        => TerminateJobAsync(jobId, reason, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobsAsync"/>.</summary>
    public static DescribeJobsResult DescribeJobs(List<string> jobs, RegionEndpoint? region = null)
        => DescribeJobsAsync(jobs, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobsAsync"/>.</summary>
    public static ListBatchJobsResult ListJobs(ListJobsRequest request, RegionEndpoint? region = null)
        => ListJobsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSchedulingPolicyAsync"/>.</summary>
    public static CreateSchedulingPolicyResult CreateSchedulingPolicy(CreateSchedulingPolicyRequest request, RegionEndpoint? region = null)
        => CreateSchedulingPolicyAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSchedulingPolicyAsync"/>.</summary>
    public static DeleteSchedulingPolicyResult DeleteSchedulingPolicy(string arn, RegionEndpoint? region = null)
        => DeleteSchedulingPolicyAsync(arn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSchedulingPoliciesAsync"/>.</summary>
    public static DescribeSchedulingPoliciesResult DescribeSchedulingPolicies(List<string> arns, RegionEndpoint? region = null)
        => DescribeSchedulingPoliciesAsync(arns, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSchedulingPoliciesAsync"/>.</summary>
    public static ListSchedulingPoliciesResult ListSchedulingPolicies(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListSchedulingPoliciesAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateSchedulingPolicyAsync"/>.</summary>
    public static UpdateSchedulingPolicyResult UpdateSchedulingPolicy(UpdateSchedulingPolicyRequest request, RegionEndpoint? region = null)
        => UpdateSchedulingPolicyAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static BatchTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static BatchUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static BatchListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
