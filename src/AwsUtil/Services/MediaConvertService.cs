using Amazon;
using Amazon.MediaConvert;
using Amazon.MediaConvert.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for AWS Elemental MediaConvert operations.
/// </summary>
public sealed record CreateMediaConvertJobResult(string? JobId = null, string? JobArn = null);
public sealed record GetMediaConvertJobResult(Job? Job = null);
public sealed record CancelMediaConvertJobResult(bool Success = true);
public sealed record ListMediaConvertJobsResult(List<Job>? Jobs = null);
public sealed record CreateMediaConvertJobTemplateResult(string? Name = null, string? Arn = null);
public sealed record DeleteMediaConvertJobTemplateResult(bool Success = true);
public sealed record GetMediaConvertJobTemplateResult(JobTemplate? JobTemplate = null);
public sealed record ListMediaConvertJobTemplatesResult(List<JobTemplate>? JobTemplates = null);
public sealed record UpdateMediaConvertJobTemplateResult(string? Name = null, string? Arn = null);
public sealed record CreateMediaConvertPresetResult(string? Name = null, string? Arn = null);
public sealed record DeleteMediaConvertPresetResult(bool Success = true);
public sealed record GetMediaConvertPresetResult(Preset? Preset = null);
public sealed record ListMediaConvertPresetsResult(List<Preset>? Presets = null);
public sealed record UpdateMediaConvertPresetResult(string? Name = null, string? Arn = null);
public sealed record CreateMediaConvertQueueResult(string? Name = null, string? Arn = null);
public sealed record DeleteMediaConvertQueueResult(bool Success = true);
public sealed record GetMediaConvertQueueResult(Queue? Queue = null);
public sealed record ListMediaConvertQueuesResult(List<Queue>? Queues = null);
public sealed record UpdateMediaConvertQueueResult(string? Name = null, string? Arn = null);
public sealed record DescribeEndpointsResult(List<Endpoint>? Endpoints = null);
public sealed record AssociateCertificateResult(bool Success = true);
public sealed record DisassociateCertificateResult(bool Success = true);
public sealed record PutMediaConvertPolicyResult(bool Success = true);
public sealed record GetMediaConvertPolicyResult(string? Policy = null);
public sealed record MediaConvertTagResourceResult(bool Success = true);
public sealed record MediaConvertUntagResourceResult(bool Success = true);
public sealed record ListMediaConvertTagsResult(Dictionary<string, string>? Tags = null);

/// <summary>
/// Utility helpers for AWS Elemental MediaConvert.
/// </summary>
public static class MediaConvertService
{
    private static AmazonMediaConvertClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonMediaConvertClient>(region);

    // ──────────────────────────── Jobs ────────────────────────────

    /// <summary>
    /// Create a MediaConvert transcoding job.
    /// </summary>
    public static async Task<CreateMediaConvertJobResult> CreateJobAsync(
        CreateJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateJobAsync(request);
            return new CreateMediaConvertJobResult(
                JobId: resp.Job?.Id,
                JobArn: resp.Job?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create MediaConvert job");
        }
    }

    /// <summary>
    /// Cancel a MediaConvert job.
    /// </summary>
    public static async Task<CancelMediaConvertJobResult> CancelJobAsync(
        string jobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CancelJobAsync(new CancelJobRequest { Id = jobId });
            return new CancelMediaConvertJobResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel MediaConvert job '{jobId}'");
        }
    }

    /// <summary>
    /// Get details about a MediaConvert job.
    /// </summary>
    public static async Task<GetMediaConvertJobResult> GetJobAsync(
        string jobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetJobAsync(new GetJobRequest { Id = jobId });
            return new GetMediaConvertJobResult(Job: resp.Job);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get MediaConvert job '{jobId}'");
        }
    }

    /// <summary>
    /// List MediaConvert jobs, automatically paginating.
    /// </summary>
    public static async Task<ListMediaConvertJobsResult> ListJobsAsync(
        string? queue = null,
        string? status = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var jobs = new List<Job>();
        var request = new ListJobsRequest();
        if (queue != null) request.Queue = queue;
        if (status != null) request.Status = status;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListJobsAsync(request);
                if (resp.Jobs != null) jobs.AddRange(resp.Jobs);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MediaConvert jobs");
        }

        return new ListMediaConvertJobsResult(Jobs: jobs);
    }

    // ──────────────────────────── Job Templates ────────────────────────────

    /// <summary>
    /// Create a MediaConvert job template.
    /// </summary>
    public static async Task<CreateMediaConvertJobTemplateResult> CreateJobTemplateAsync(
        CreateJobTemplateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateJobTemplateAsync(request);
            return new CreateMediaConvertJobTemplateResult(
                Name: resp.JobTemplate?.Name,
                Arn: resp.JobTemplate?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create MediaConvert job template '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a MediaConvert job template.
    /// </summary>
    public static async Task<DeleteMediaConvertJobTemplateResult> DeleteJobTemplateAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteJobTemplateAsync(
                new DeleteJobTemplateRequest { Name = name });
            return new DeleteMediaConvertJobTemplateResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MediaConvert job template '{name}'");
        }
    }

    /// <summary>
    /// Get details about a MediaConvert job template.
    /// </summary>
    public static async Task<GetMediaConvertJobTemplateResult> GetJobTemplateAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetJobTemplateAsync(
                new GetJobTemplateRequest { Name = name });
            return new GetMediaConvertJobTemplateResult(JobTemplate: resp.JobTemplate);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get MediaConvert job template '{name}'");
        }
    }

    /// <summary>
    /// List MediaConvert job templates, automatically paginating.
    /// </summary>
    public static async Task<ListMediaConvertJobTemplatesResult> ListJobTemplatesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var templates = new List<JobTemplate>();
        var request = new ListJobTemplatesRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListJobTemplatesAsync(request);
                if (resp.JobTemplates != null) templates.AddRange(resp.JobTemplates);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list MediaConvert job templates");
        }

        return new ListMediaConvertJobTemplatesResult(JobTemplates: templates);
    }

    /// <summary>
    /// Update a MediaConvert job template.
    /// </summary>
    public static async Task<UpdateMediaConvertJobTemplateResult> UpdateJobTemplateAsync(
        UpdateJobTemplateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateJobTemplateAsync(request);
            return new UpdateMediaConvertJobTemplateResult(
                Name: resp.JobTemplate?.Name,
                Arn: resp.JobTemplate?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MediaConvert job template '{request.Name}'");
        }
    }

    // ──────────────────────────── Presets ────────────────────────────

    /// <summary>
    /// Create a MediaConvert output preset.
    /// </summary>
    public static async Task<CreateMediaConvertPresetResult> CreatePresetAsync(
        CreatePresetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePresetAsync(request);
            return new CreateMediaConvertPresetResult(
                Name: resp.Preset?.Name,
                Arn: resp.Preset?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create MediaConvert preset '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a MediaConvert output preset.
    /// </summary>
    public static async Task<DeleteMediaConvertPresetResult> DeletePresetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePresetAsync(new DeletePresetRequest { Name = name });
            return new DeleteMediaConvertPresetResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MediaConvert preset '{name}'");
        }
    }

    /// <summary>
    /// Get details about a MediaConvert output preset.
    /// </summary>
    public static async Task<GetMediaConvertPresetResult> GetPresetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPresetAsync(
                new GetPresetRequest { Name = name });
            return new GetMediaConvertPresetResult(Preset: resp.Preset);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get MediaConvert preset '{name}'");
        }
    }

    /// <summary>
    /// List MediaConvert output presets, automatically paginating.
    /// </summary>
    public static async Task<ListMediaConvertPresetsResult> ListPresetsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var presets = new List<Preset>();
        var request = new ListPresetsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListPresetsAsync(request);
                if (resp.Presets != null) presets.AddRange(resp.Presets);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MediaConvert presets");
        }

        return new ListMediaConvertPresetsResult(Presets: presets);
    }

    /// <summary>
    /// Update a MediaConvert output preset.
    /// </summary>
    public static async Task<UpdateMediaConvertPresetResult> UpdatePresetAsync(
        UpdatePresetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdatePresetAsync(request);
            return new UpdateMediaConvertPresetResult(
                Name: resp.Preset?.Name,
                Arn: resp.Preset?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MediaConvert preset '{request.Name}'");
        }
    }

    // ──────────────────────────── Queues ────────────────────────────

    /// <summary>
    /// Create a MediaConvert queue.
    /// </summary>
    public static async Task<CreateMediaConvertQueueResult> CreateQueueAsync(
        CreateQueueRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateQueueAsync(request);
            return new CreateMediaConvertQueueResult(
                Name: resp.Queue?.Name,
                Arn: resp.Queue?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create MediaConvert queue '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a MediaConvert queue.
    /// </summary>
    public static async Task<DeleteMediaConvertQueueResult> DeleteQueueAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteQueueAsync(new DeleteQueueRequest { Name = name });
            return new DeleteMediaConvertQueueResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MediaConvert queue '{name}'");
        }
    }

    /// <summary>
    /// Get details about a MediaConvert queue.
    /// </summary>
    public static async Task<GetMediaConvertQueueResult> GetQueueAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueueAsync(
                new GetQueueRequest { Name = name });
            return new GetMediaConvertQueueResult(Queue: resp.Queue);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get MediaConvert queue '{name}'");
        }
    }

    /// <summary>
    /// List MediaConvert queues, automatically paginating.
    /// </summary>
    public static async Task<ListMediaConvertQueuesResult> ListQueuesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var queues = new List<Queue>();
        var request = new ListQueuesRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListQueuesAsync(request);
                if (resp.Queues != null) queues.AddRange(resp.Queues);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MediaConvert queues");
        }

        return new ListMediaConvertQueuesResult(Queues: queues);
    }

    /// <summary>
    /// Update a MediaConvert queue.
    /// </summary>
    public static async Task<UpdateMediaConvertQueueResult> UpdateQueueAsync(
        UpdateQueueRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateQueueAsync(request);
            return new UpdateMediaConvertQueueResult(
                Name: resp.Queue?.Name,
                Arn: resp.Queue?.Arn);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MediaConvert queue '{request.Name}'");
        }
    }

    // ──────────────────────────── Endpoints ────────────────────────────

    /// <summary>
    /// Describe MediaConvert endpoints, automatically paginating.
    /// </summary>
    public static async Task<DescribeEndpointsResult> DescribeEndpointsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var endpoints = new List<Endpoint>();
        var request = new DescribeEndpointsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeEndpointsAsync(request);
                if (resp.Endpoints != null) endpoints.AddRange(resp.Endpoints);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe MediaConvert endpoints");
        }

        return new DescribeEndpointsResult(Endpoints: endpoints);
    }

    // ──────────────────────────── Certificates ────────────────────────────

    /// <summary>
    /// Associate an ACM certificate with a MediaConvert resource.
    /// </summary>
    public static async Task<AssociateCertificateResult> AssociateCertificateAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateCertificateAsync(
                new AssociateCertificateRequest { Arn = arn });
            return new AssociateCertificateResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate certificate '{arn}'");
        }
    }

    /// <summary>
    /// Disassociate an ACM certificate from a MediaConvert resource.
    /// </summary>
    public static async Task<DisassociateCertificateResult> DisassociateCertificateAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateCertificateAsync(
                new DisassociateCertificateRequest { Arn = arn });
            return new DisassociateCertificateResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate certificate '{arn}'");
        }
    }

    // ──────────────────────────── Policy ────────────────────────────

    /// <summary>
    /// Put a resource policy for MediaConvert.
    /// </summary>
    public static async Task<PutMediaConvertPolicyResult> PutPolicyAsync(
        PutPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutPolicyAsync(request);
            return new PutMediaConvertPolicyResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put MediaConvert policy");
        }
    }

    /// <summary>
    /// Get the resource policy for MediaConvert.
    /// </summary>
    public static async Task<GetMediaConvertPolicyResult> GetPolicyAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPolicyAsync(new GetPolicyRequest());
            return new GetMediaConvertPolicyResult(Policy: resp.Policy?.ToString());
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get MediaConvert policy");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a MediaConvert resource.
    /// </summary>
    public static async Task<MediaConvertTagResourceResult> TagResourceAsync(
        string arn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                Arn = arn,
                Tags = tags
            });
            return new MediaConvertTagResourceResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag MediaConvert resource '{arn}'");
        }
    }

    /// <summary>
    /// Remove tags from a MediaConvert resource.
    /// </summary>
    public static async Task<MediaConvertUntagResourceResult> UntagResourceAsync(
        string arn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                Arn = arn,
                TagKeys = tagKeys
            });
            return new MediaConvertUntagResourceResult();
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag MediaConvert resource '{arn}'");
        }
    }

    /// <summary>
    /// List tags for a MediaConvert resource.
    /// </summary>
    public static async Task<ListMediaConvertTagsResult> ListTagsForResourceAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { Arn = arn });
            return new ListMediaConvertTagsResult(
                Tags: resp.ResourceTags?.Tags);
        }
        catch (AmazonMediaConvertException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for MediaConvert resource '{arn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateJobAsync"/>.</summary>
    public static CreateMediaConvertJobResult CreateJob(CreateJobRequest request, RegionEndpoint? region = null)
        => CreateJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelJobAsync"/>.</summary>
    public static CancelMediaConvertJobResult CancelJob(string jobId, RegionEndpoint? region = null)
        => CancelJobAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetJobAsync"/>.</summary>
    public static GetMediaConvertJobResult GetJob(string jobId, RegionEndpoint? region = null)
        => GetJobAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobsAsync"/>.</summary>
    public static ListMediaConvertJobsResult ListJobs(string? queue = null, string? status = null, RegionEndpoint? region = null)
        => ListJobsAsync(queue, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateJobTemplateAsync"/>.</summary>
    public static CreateMediaConvertJobTemplateResult CreateJobTemplate(CreateJobTemplateRequest request, RegionEndpoint? region = null)
        => CreateJobTemplateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteJobTemplateAsync"/>.</summary>
    public static DeleteMediaConvertJobTemplateResult DeleteJobTemplate(string name, RegionEndpoint? region = null)
        => DeleteJobTemplateAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetJobTemplateAsync"/>.</summary>
    public static GetMediaConvertJobTemplateResult GetJobTemplate(string name, RegionEndpoint? region = null)
        => GetJobTemplateAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobTemplatesAsync"/>.</summary>
    public static ListMediaConvertJobTemplatesResult ListJobTemplates(RegionEndpoint? region = null)
        => ListJobTemplatesAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateJobTemplateAsync"/>.</summary>
    public static UpdateMediaConvertJobTemplateResult UpdateJobTemplate(UpdateJobTemplateRequest request, RegionEndpoint? region = null)
        => UpdateJobTemplateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePresetAsync"/>.</summary>
    public static CreateMediaConvertPresetResult CreatePreset(CreatePresetRequest request, RegionEndpoint? region = null)
        => CreatePresetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePresetAsync"/>.</summary>
    public static DeleteMediaConvertPresetResult DeletePreset(string name, RegionEndpoint? region = null)
        => DeletePresetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPresetAsync"/>.</summary>
    public static GetMediaConvertPresetResult GetPreset(string name, RegionEndpoint? region = null)
        => GetPresetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPresetsAsync"/>.</summary>
    public static ListMediaConvertPresetsResult ListPresets(RegionEndpoint? region = null)
        => ListPresetsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdatePresetAsync"/>.</summary>
    public static UpdateMediaConvertPresetResult UpdatePreset(UpdatePresetRequest request, RegionEndpoint? region = null)
        => UpdatePresetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateQueueAsync"/>.</summary>
    public static CreateMediaConvertQueueResult CreateQueue(CreateQueueRequest request, RegionEndpoint? region = null)
        => CreateQueueAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteQueueAsync"/>.</summary>
    public static DeleteMediaConvertQueueResult DeleteQueue(string name, RegionEndpoint? region = null)
        => DeleteQueueAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueueAsync"/>.</summary>
    public static GetMediaConvertQueueResult GetQueue(string name, RegionEndpoint? region = null)
        => GetQueueAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueuesAsync"/>.</summary>
    public static ListMediaConvertQueuesResult ListQueues(RegionEndpoint? region = null)
        => ListQueuesAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateQueueAsync"/>.</summary>
    public static UpdateMediaConvertQueueResult UpdateQueue(UpdateQueueRequest request, RegionEndpoint? region = null)
        => UpdateQueueAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEndpointsAsync"/>.</summary>
    public static DescribeEndpointsResult DescribeEndpoints(RegionEndpoint? region = null)
        => DescribeEndpointsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateCertificateAsync"/>.</summary>
    public static AssociateCertificateResult AssociateCertificate(string arn, RegionEndpoint? region = null)
        => AssociateCertificateAsync(arn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateCertificateAsync"/>.</summary>
    public static DisassociateCertificateResult DisassociateCertificate(string arn, RegionEndpoint? region = null)
        => DisassociateCertificateAsync(arn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutPolicyAsync"/>.</summary>
    public static PutMediaConvertPolicyResult PutPolicy(PutPolicyRequest request, RegionEndpoint? region = null)
        => PutPolicyAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPolicyAsync"/>.</summary>
    public static GetMediaConvertPolicyResult GetPolicy(RegionEndpoint? region = null)
        => GetPolicyAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static MediaConvertTagResourceResult TagResource(string arn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(arn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static MediaConvertUntagResourceResult UntagResource(string arn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(arn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListMediaConvertTagsResult ListTagsForResource(string arn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(arn, region).GetAwaiter().GetResult();

}
