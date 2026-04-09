using Amazon;
using Amazon.CodePipeline;
using Amazon.CodePipeline.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CpCreatePipelineResult(
    PipelineDeclaration? Pipeline = null,
    List<Amazon.CodePipeline.Model.Tag>? Tags = null);

public sealed record CpDeletePipelineResult(bool Success = true);

public sealed record CpGetPipelineResult(
    PipelineDeclaration? Pipeline = null,
    PipelineMetadata? Metadata = null);

public sealed record CpListPipelinesResult(
    List<PipelineSummary>? Pipelines = null,
    string? NextToken = null);

public sealed record CpUpdatePipelineResult(
    PipelineDeclaration? Pipeline = null);

public sealed record CpStartPipelineExecutionResult(
    string? PipelineExecutionId = null);

public sealed record CpStopPipelineExecutionResult(
    string? PipelineExecutionId = null);

public sealed record CpGetPipelineExecutionResult(
    PipelineExecution? PipelineExecution = null);

public sealed record CpListPipelineExecutionsResult(
    List<PipelineExecutionSummary>? PipelineExecutionSummaries = null,
    string? NextToken = null);

public sealed record CpGetPipelineStateResult(
    string? PipelineName = null,
    int? PipelineVersion = null,
    List<StageState>? StageStates = null,
    DateTime? Created = null,
    DateTime? Updated = null);

public sealed record CpEnableStageTransitionResult(bool Success = true);

public sealed record CpDisableStageTransitionResult(bool Success = true);

public sealed record CpPutApprovalResultResult(
    DateTime? ApprovedAt = null);

public sealed record CpAcknowledgeJobResult(
    string? Status = null);

public sealed record CpPollForJobsResult(
    List<Job>? Jobs = null);

public sealed record CpPutJobSuccessResultResult(bool Success = true);

public sealed record CpPutJobFailureResultResult(bool Success = true);

public sealed record CpRetryStageExecutionResult(
    string? PipelineExecutionId = null);

public sealed record CpRollbackStageResult(
    string? PipelineExecutionId = null);

public sealed record CpTagResourceResult(bool Success = true);
public sealed record CpUntagResourceResult(bool Success = true);

public sealed record CpListTagsForResourceResult(
    List<Amazon.CodePipeline.Model.Tag>? Tags = null,
    string? NextToken = null);

public sealed record CpPutActionRevisionResult(
    bool? NewRevision = null,
    string? PipelineExecutionId = null);

public sealed record CpListActionExecutionsResult(
    List<ActionExecutionDetail>? ActionExecutionDetails = null,
    string? NextToken = null);

public sealed record CpRegisterWebhookWithThirdPartyResult(
    bool Success = true);

public sealed record CpDeregisterWebhookWithThirdPartyResult(
    bool Success = true);

public sealed record CpPutWebhookResult(
    ListWebhookItem? Webhook = null);

public sealed record CpDeleteWebhookResult(bool Success = true);

public sealed record CpListWebhooksResult(
    List<ListWebhookItem>? Webhooks = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodePipeline.
/// </summary>
public static class CodePipelineService
{
    private static AmazonCodePipelineClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodePipelineClient>(region);

    /// <summary>
    /// Create a new pipeline.
    /// </summary>
    public static async Task<CpCreatePipelineResult>
        CreatePipelineAsync(
            CreatePipelineRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePipelineAsync(request);
            return new CpCreatePipelineResult(
                Pipeline: resp.Pipeline,
                Tags: resp.Tags);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create pipeline");
        }
    }

    /// <summary>
    /// Delete a pipeline.
    /// </summary>
    public static async Task<CpDeletePipelineResult>
        DeletePipelineAsync(
            string name,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePipelineAsync(
                new DeletePipelineRequest { Name = name });
            return new CpDeletePipelineResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete pipeline '{name}'");
        }
    }

    /// <summary>
    /// Get the definition and metadata of a pipeline.
    /// </summary>
    public static async Task<CpGetPipelineResult> GetPipelineAsync(
        string name,
        int? version = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPipelineRequest { Name = name };
        if (version.HasValue) request.Version = version.Value;

        try
        {
            var resp = await client.GetPipelineAsync(request);
            return new CpGetPipelineResult(
                Pipeline: resp.Pipeline,
                Metadata: resp.Metadata);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get pipeline '{name}'");
        }
    }

    /// <summary>
    /// List pipelines.
    /// </summary>
    public static async Task<CpListPipelinesResult> ListPipelinesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPipelinesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListPipelinesAsync(request);
            return new CpListPipelinesResult(
                Pipelines: resp.Pipelines,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list pipelines");
        }
    }

    /// <summary>
    /// Update a pipeline definition.
    /// </summary>
    public static async Task<CpUpdatePipelineResult>
        UpdatePipelineAsync(
            PipelineDeclaration pipeline,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdatePipelineAsync(
                new UpdatePipelineRequest { Pipeline = pipeline });
            return new CpUpdatePipelineResult(Pipeline: resp.Pipeline);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update pipeline");
        }
    }

    /// <summary>
    /// Start a pipeline execution.
    /// </summary>
    public static async Task<CpStartPipelineExecutionResult>
        StartPipelineExecutionAsync(
            StartPipelineExecutionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartPipelineExecutionAsync(request);
            return new CpStartPipelineExecutionResult(
                PipelineExecutionId: resp.PipelineExecutionId);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start pipeline execution");
        }
    }

    /// <summary>
    /// Stop a pipeline execution.
    /// </summary>
    public static async Task<CpStopPipelineExecutionResult>
        StopPipelineExecutionAsync(
            StopPipelineExecutionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopPipelineExecutionAsync(request);
            return new CpStopPipelineExecutionResult(
                PipelineExecutionId: resp.PipelineExecutionId);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to stop pipeline execution");
        }
    }

    /// <summary>
    /// Get information about a pipeline execution.
    /// </summary>
    public static async Task<CpGetPipelineExecutionResult>
        GetPipelineExecutionAsync(
            string pipelineName,
            string pipelineExecutionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPipelineExecutionAsync(
                new GetPipelineExecutionRequest
                {
                    PipelineName = pipelineName,
                    PipelineExecutionId = pipelineExecutionId
                });
            return new CpGetPipelineExecutionResult(
                PipelineExecution: resp.PipelineExecution);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get pipeline execution");
        }
    }

    /// <summary>
    /// List executions for a pipeline.
    /// </summary>
    public static async Task<CpListPipelineExecutionsResult>
        ListPipelineExecutionsAsync(
            string pipelineName,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPipelineExecutionsRequest
        {
            PipelineName = pipelineName
        };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListPipelineExecutionsAsync(request);
            return new CpListPipelineExecutionsResult(
                PipelineExecutionSummaries:
                    resp.PipelineExecutionSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list pipeline executions");
        }
    }

    /// <summary>
    /// Get the current state of a pipeline.
    /// </summary>
    public static async Task<CpGetPipelineStateResult>
        GetPipelineStateAsync(
            string name,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPipelineStateAsync(
                new GetPipelineStateRequest { Name = name });
            return new CpGetPipelineStateResult(
                PipelineName: resp.PipelineName,
                PipelineVersion: resp.PipelineVersion,
                StageStates: resp.StageStates,
                Created: resp.Created,
                Updated: resp.Updated);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get pipeline state for '{name}'");
        }
    }

    /// <summary>
    /// Enable a stage transition in a pipeline.
    /// </summary>
    public static async Task<CpEnableStageTransitionResult>
        EnableStageTransitionAsync(
            string pipelineName,
            string stageName,
            string transitionType,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableStageTransitionAsync(
                new EnableStageTransitionRequest
                {
                    PipelineName = pipelineName,
                    StageName = stageName,
                    TransitionType =
                        new StageTransitionType(transitionType)
                });
            return new CpEnableStageTransitionResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to enable stage transition");
        }
    }

    /// <summary>
    /// Disable a stage transition in a pipeline.
    /// </summary>
    public static async Task<CpDisableStageTransitionResult>
        DisableStageTransitionAsync(
            string pipelineName,
            string stageName,
            string transitionType,
            string reason,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableStageTransitionAsync(
                new DisableStageTransitionRequest
                {
                    PipelineName = pipelineName,
                    StageName = stageName,
                    TransitionType =
                        new StageTransitionType(transitionType),
                    Reason = reason
                });
            return new CpDisableStageTransitionResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disable stage transition");
        }
    }

    /// <summary>
    /// Provide the response to a manual approval request.
    /// </summary>
    public static async Task<CpPutApprovalResultResult>
        PutApprovalResultAsync(
            PutApprovalResultRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutApprovalResultAsync(request);
            return new CpPutApprovalResultResult(
                ApprovedAt: resp.ApprovedAt);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put approval result");
        }
    }

    /// <summary>
    /// Acknowledge a job for a custom action.
    /// </summary>
    public static async Task<CpAcknowledgeJobResult>
        AcknowledgeJobAsync(
            string jobId,
            string nonce,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AcknowledgeJobAsync(
                new AcknowledgeJobRequest
                {
                    JobId = jobId,
                    Nonce = nonce
                });
            return new CpAcknowledgeJobResult(
                Status: resp.Status?.Value);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to acknowledge job '{jobId}'");
        }
    }

    /// <summary>
    /// Poll for available jobs for a custom action.
    /// </summary>
    public static async Task<CpPollForJobsResult> PollForJobsAsync(
        PollForJobsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PollForJobsAsync(request);
            return new CpPollForJobsResult(Jobs: resp.Jobs);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to poll for jobs");
        }
    }

    /// <summary>
    /// Report a successful job result.
    /// </summary>
    public static async Task<CpPutJobSuccessResultResult>
        PutJobSuccessResultAsync(
            PutJobSuccessResultRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutJobSuccessResultAsync(request);
            return new CpPutJobSuccessResultResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put job success result");
        }
    }

    /// <summary>
    /// Report a failed job result.
    /// </summary>
    public static async Task<CpPutJobFailureResultResult>
        PutJobFailureResultAsync(
            PutJobFailureResultRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutJobFailureResultAsync(request);
            return new CpPutJobFailureResultResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put job failure result");
        }
    }

    /// <summary>
    /// Retry a failed stage execution.
    /// </summary>
    public static async Task<CpRetryStageExecutionResult>
        RetryStageExecutionAsync(
            RetryStageExecutionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RetryStageExecutionAsync(request);
            return new CpRetryStageExecutionResult(
                PipelineExecutionId: resp.PipelineExecutionId);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to retry stage execution");
        }
    }

    /// <summary>
    /// Roll back a stage execution.
    /// </summary>
    public static async Task<CpRollbackStageResult>
        RollbackStageAsync(
            RollbackStageRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RollbackStageAsync(request);
            return new CpRollbackStageResult(
                PipelineExecutionId: resp.PipelineExecutionId);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to rollback stage");
        }
    }

    /// <summary>
    /// Tag a CodePipeline resource.
    /// </summary>
    public static async Task<CpTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Amazon.CodePipeline.Model.Tag> tags,
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
            return new CpTagResourceResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag CodePipeline resource");
        }
    }

    /// <summary>
    /// Remove tags from a CodePipeline resource.
    /// </summary>
    public static async Task<CpUntagResourceResult> UntagResourceAsync(
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
            return new CpUntagResourceResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag CodePipeline resource");
        }
    }

    /// <summary>
    /// List tags for a CodePipeline resource.
    /// </summary>
    public static async Task<CpListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new CpListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for CodePipeline resource");
        }
    }

    /// <summary>
    /// Record a revision for an action.
    /// </summary>
    public static async Task<CpPutActionRevisionResult>
        PutActionRevisionAsync(
            PutActionRevisionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutActionRevisionAsync(request);
            return new CpPutActionRevisionResult(
                NewRevision: resp.NewRevision,
                PipelineExecutionId: resp.PipelineExecutionId);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put action revision");
        }
    }

    /// <summary>
    /// List action executions for a pipeline.
    /// </summary>
    public static async Task<CpListActionExecutionsResult>
        ListActionExecutionsAsync(
            ListActionExecutionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListActionExecutionsAsync(request);
            return new CpListActionExecutionsResult(
                ActionExecutionDetails: resp.ActionExecutionDetails,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list action executions");
        }
    }

    /// <summary>
    /// Register a webhook with a third-party provider.
    /// </summary>
    public static async Task<CpRegisterWebhookWithThirdPartyResult>
        RegisterWebhookWithThirdPartyAsync(
            string? webhookName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RegisterWebhookWithThirdPartyRequest();
        if (webhookName != null) request.WebhookName = webhookName;

        try
        {
            await client.RegisterWebhookWithThirdPartyAsync(request);
            return new CpRegisterWebhookWithThirdPartyResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to register webhook with third party");
        }
    }

    /// <summary>
    /// Deregister a webhook from a third-party provider.
    /// </summary>
    public static async Task<CpDeregisterWebhookWithThirdPartyResult>
        DeregisterWebhookWithThirdPartyAsync(
            string? webhookName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeregisterWebhookWithThirdPartyRequest();
        if (webhookName != null) request.WebhookName = webhookName;

        try
        {
            await client.DeregisterWebhookWithThirdPartyAsync(request);
            return new CpDeregisterWebhookWithThirdPartyResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to deregister webhook with third party");
        }
    }

    /// <summary>
    /// Create or update a webhook.
    /// </summary>
    public static async Task<CpPutWebhookResult> PutWebhookAsync(
        PutWebhookRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutWebhookAsync(request);
            return new CpPutWebhookResult(Webhook: resp.Webhook);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put webhook");
        }
    }

    /// <summary>
    /// Delete a webhook.
    /// </summary>
    public static async Task<CpDeleteWebhookResult> DeleteWebhookAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWebhookAsync(
                new DeleteWebhookRequest { Name = name });
            return new CpDeleteWebhookResult();
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete webhook '{name}'");
        }
    }

    /// <summary>
    /// List webhooks for the account.
    /// </summary>
    public static async Task<CpListWebhooksResult> ListWebhooksAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWebhooksRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListWebhooksAsync(request);
            return new CpListWebhooksResult(
                Webhooks: resp.Webhooks,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodePipelineException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list webhooks");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreatePipelineAsync"/>.</summary>
    public static CpCreatePipelineResult CreatePipeline(CreatePipelineRequest request, RegionEndpoint? region = null)
        => CreatePipelineAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePipelineAsync"/>.</summary>
    public static CpDeletePipelineResult DeletePipeline(string name, RegionEndpoint? region = null)
        => DeletePipelineAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPipelineAsync"/>.</summary>
    public static CpGetPipelineResult GetPipeline(string name, int? version = null, RegionEndpoint? region = null)
        => GetPipelineAsync(name, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPipelinesAsync"/>.</summary>
    public static CpListPipelinesResult ListPipelines(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListPipelinesAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdatePipelineAsync"/>.</summary>
    public static CpUpdatePipelineResult UpdatePipeline(PipelineDeclaration pipeline, RegionEndpoint? region = null)
        => UpdatePipelineAsync(pipeline, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartPipelineExecutionAsync"/>.</summary>
    public static CpStartPipelineExecutionResult StartPipelineExecution(StartPipelineExecutionRequest request, RegionEndpoint? region = null)
        => StartPipelineExecutionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopPipelineExecutionAsync"/>.</summary>
    public static CpStopPipelineExecutionResult StopPipelineExecution(StopPipelineExecutionRequest request, RegionEndpoint? region = null)
        => StopPipelineExecutionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPipelineExecutionAsync"/>.</summary>
    public static CpGetPipelineExecutionResult GetPipelineExecution(string pipelineName, string pipelineExecutionId, RegionEndpoint? region = null)
        => GetPipelineExecutionAsync(pipelineName, pipelineExecutionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPipelineExecutionsAsync"/>.</summary>
    public static CpListPipelineExecutionsResult ListPipelineExecutions(string pipelineName, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListPipelineExecutionsAsync(pipelineName, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPipelineStateAsync"/>.</summary>
    public static CpGetPipelineStateResult GetPipelineState(string name, RegionEndpoint? region = null)
        => GetPipelineStateAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableStageTransitionAsync"/>.</summary>
    public static CpEnableStageTransitionResult EnableStageTransition(string pipelineName, string stageName, string transitionType, RegionEndpoint? region = null)
        => EnableStageTransitionAsync(pipelineName, stageName, transitionType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableStageTransitionAsync"/>.</summary>
    public static CpDisableStageTransitionResult DisableStageTransition(string pipelineName, string stageName, string transitionType, string reason, RegionEndpoint? region = null)
        => DisableStageTransitionAsync(pipelineName, stageName, transitionType, reason, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutApprovalResultAsync"/>.</summary>
    public static CpPutApprovalResultResult PutApprovalResult(PutApprovalResultRequest request, RegionEndpoint? region = null)
        => PutApprovalResultAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AcknowledgeJobAsync"/>.</summary>
    public static CpAcknowledgeJobResult AcknowledgeJob(string jobId, string nonce, RegionEndpoint? region = null)
        => AcknowledgeJobAsync(jobId, nonce, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PollForJobsAsync"/>.</summary>
    public static CpPollForJobsResult PollForJobs(PollForJobsRequest request, RegionEndpoint? region = null)
        => PollForJobsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutJobSuccessResultAsync"/>.</summary>
    public static CpPutJobSuccessResultResult PutJobSuccessResult(PutJobSuccessResultRequest request, RegionEndpoint? region = null)
        => PutJobSuccessResultAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutJobFailureResultAsync"/>.</summary>
    public static CpPutJobFailureResultResult PutJobFailureResult(PutJobFailureResultRequest request, RegionEndpoint? region = null)
        => PutJobFailureResultAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RetryStageExecutionAsync"/>.</summary>
    public static CpRetryStageExecutionResult RetryStageExecution(RetryStageExecutionRequest request, RegionEndpoint? region = null)
        => RetryStageExecutionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RollbackStageAsync"/>.</summary>
    public static CpRollbackStageResult RollbackStage(RollbackStageRequest request, RegionEndpoint? region = null)
        => RollbackStageAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static CpTagResourceResult TagResource(string resourceArn, List<Amazon.CodePipeline.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static CpUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static CpListTagsForResourceResult ListTagsForResource(string resourceArn, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutActionRevisionAsync"/>.</summary>
    public static CpPutActionRevisionResult PutActionRevision(PutActionRevisionRequest request, RegionEndpoint? region = null)
        => PutActionRevisionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListActionExecutionsAsync"/>.</summary>
    public static CpListActionExecutionsResult ListActionExecutions(ListActionExecutionsRequest request, RegionEndpoint? region = null)
        => ListActionExecutionsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterWebhookWithThirdPartyAsync"/>.</summary>
    public static CpRegisterWebhookWithThirdPartyResult RegisterWebhookWithThirdParty(string? webhookName = null, RegionEndpoint? region = null)
        => RegisterWebhookWithThirdPartyAsync(webhookName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterWebhookWithThirdPartyAsync"/>.</summary>
    public static CpDeregisterWebhookWithThirdPartyResult DeregisterWebhookWithThirdParty(string? webhookName = null, RegionEndpoint? region = null)
        => DeregisterWebhookWithThirdPartyAsync(webhookName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutWebhookAsync"/>.</summary>
    public static CpPutWebhookResult PutWebhook(PutWebhookRequest request, RegionEndpoint? region = null)
        => PutWebhookAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteWebhookAsync"/>.</summary>
    public static CpDeleteWebhookResult DeleteWebhook(string name, RegionEndpoint? region = null)
        => DeleteWebhookAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListWebhooksAsync"/>.</summary>
    public static CpListWebhooksResult ListWebhooks(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListWebhooksAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

}
