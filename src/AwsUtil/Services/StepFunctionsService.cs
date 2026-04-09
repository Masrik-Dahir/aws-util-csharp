using Amazon;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateStateMachineResult(
    string? StateMachineArn = null,
    DateTime? CreationDate = null);

public sealed record DeleteStateMachineResult(bool Success = true);

public sealed record DescribeStateMachineResult(
    string? StateMachineArn = null,
    string? Name = null,
    string? Status = null,
    string? Definition = null,
    string? RoleArn = null,
    string? Type = null,
    DateTime? CreationDate = null,
    string? Description = null);

public sealed record ListStateMachinesResult(
    List<StateMachineListItem>? StateMachines = null,
    string? NextToken = null);

public sealed record UpdateStateMachineResult(
    DateTime? UpdateDate = null,
    string? RevisionId = null,
    string? StateMachineVersionArn = null);

public sealed record StartExecutionResult(
    string? ExecutionArn = null,
    DateTime? StartDate = null);

public sealed record StopExecutionResult(
    DateTime? StopDate = null);

public sealed record DescribeExecutionResult(
    string? ExecutionArn = null,
    string? StateMachineArn = null,
    string? Name = null,
    string? Status = null,
    DateTime? StartDate = null,
    DateTime? StopDate = null,
    string? Input = null,
    string? Output = null,
    string? Error = null,
    string? Cause = null,
    string? MapRunArn = null,
    string? RedriveStatus = null,
    int? RedriveCount = null);

public sealed record ListExecutionsResult(
    List<ExecutionListItem>? Executions = null,
    string? NextToken = null);

public sealed record GetExecutionHistoryResult(
    List<HistoryEvent>? Events = null,
    string? NextToken = null);

public sealed record StartSyncExecutionResult(
    string? ExecutionArn = null,
    string? StateMachineArn = null,
    string? Name = null,
    DateTime? StartDate = null,
    DateTime? StopDate = null,
    string? Status = null,
    string? Error = null,
    string? Cause = null,
    string? Input = null,
    string? Output = null);

public sealed record SendTaskSuccessResult(bool Success = true);

public sealed record SendTaskFailureResult(bool Success = true);

public sealed record SendTaskHeartbeatResult(bool Success = true);

public sealed record DescribeActivityResult(
    string? ActivityArn = null,
    string? Name = null,
    DateTime? CreationDate = null);

public sealed record CreateActivityResult(
    string? ActivityArn = null,
    DateTime? CreationDate = null);

public sealed record DeleteActivityResult(bool Success = true);

public sealed record ListActivitiesResult(
    List<ActivityListItem>? Activities = null,
    string? NextToken = null);

public sealed record GetActivityTaskResult(
    string? TaskToken = null,
    string? Input = null);

public sealed record SfnTagResourceResult(bool Success = true);

public sealed record SfnUntagResourceResult(bool Success = true);

public sealed record SfnListTagsForResourceResult(
    List<Tag>? Tags = null);

public sealed record DescribeStateMachineForExecutionResult(
    string? StateMachineArn = null,
    string? Name = null,
    string? Definition = null,
    string? RoleArn = null,
    DateTime? UpdateDate = null,
    string? RevisionId = null,
    string? Description = null);

public sealed record DescribeMapRunResult(
    string? MapRunArn = null,
    string? ExecutionArn = null,
    string? Status = null,
    DateTime? StartDate = null,
    DateTime? StopDate = null,
    long? MaxConcurrency = null,
    float? ToleratedFailurePercentage = null,
    long? ToleratedFailureCount = null);

public sealed record ListMapRunsResult(
    List<MapRunListItem>? MapRuns = null,
    string? NextToken = null);

public sealed record UpdateMapRunResult(bool Success = true);

public sealed record PublishStateMachineVersionResult(
    string? StateMachineVersionArn = null,
    DateTime? CreationDate = null);

public sealed record ListStateMachineVersionsResult(
    List<StateMachineVersionListItem>? StateMachineVersions = null,
    string? NextToken = null);

public sealed record CreateStateMachineAliasResult(
    string? StateMachineAliasArn = null,
    DateTime? CreationDate = null);

public sealed record DescribeStateMachineAliasResult(
    string? StateMachineAliasArn = null,
    string? Name = null,
    string? Description = null,
    List<RoutingConfigurationListItem>? RoutingConfiguration = null,
    DateTime? CreationDate = null,
    DateTime? UpdateDate = null);

public sealed record UpdateStateMachineAliasResult(
    DateTime? UpdateDate = null);

public sealed record DeleteStateMachineAliasResult(bool Success = true);

public sealed record ListStateMachineAliasesResult(
    List<StateMachineAliasListItem>? StateMachineAliases = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Step Functions.
/// </summary>
public static class StepFunctionsService
{
    private static AmazonStepFunctionsClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonStepFunctionsClient>(region);

    /// <summary>
    /// Create a new state machine.
    /// </summary>
    public static async Task<CreateStateMachineResult> CreateStateMachineAsync(
        CreateStateMachineRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStateMachineAsync(request);
            return new CreateStateMachineResult(
                StateMachineArn: resp.StateMachineArn,
                CreationDate: resp.CreationDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create state machine");
        }
    }

    /// <summary>
    /// Delete a state machine.
    /// </summary>
    public static async Task<DeleteStateMachineResult> DeleteStateMachineAsync(
        string stateMachineArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteStateMachineAsync(new DeleteStateMachineRequest
            {
                StateMachineArn = stateMachineArn
            });
            return new DeleteStateMachineResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete state machine '{stateMachineArn}'");
        }
    }

    /// <summary>
    /// Describe a state machine.
    /// </summary>
    public static async Task<DescribeStateMachineResult> DescribeStateMachineAsync(
        string stateMachineArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStateMachineAsync(
                new DescribeStateMachineRequest
                {
                    StateMachineArn = stateMachineArn
                });
            return new DescribeStateMachineResult(
                StateMachineArn: resp.StateMachineArn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                Definition: resp.Definition,
                RoleArn: resp.RoleArn,
                Type: resp.Type?.Value,
                CreationDate: resp.CreationDate,
                Description: resp.Description);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe state machine '{stateMachineArn}'");
        }
    }

    /// <summary>
    /// List state machines.
    /// </summary>
    public static async Task<ListStateMachinesResult> ListStateMachinesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStateMachinesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListStateMachinesAsync(request);
            return new ListStateMachinesResult(
                StateMachines: resp.StateMachines,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list state machines");
        }
    }

    /// <summary>
    /// Update a state machine.
    /// </summary>
    public static async Task<UpdateStateMachineResult> UpdateStateMachineAsync(
        UpdateStateMachineRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateStateMachineAsync(request);
            return new UpdateStateMachineResult(
                UpdateDate: resp.UpdateDate,
                RevisionId: resp.RevisionId,
                StateMachineVersionArn: resp.StateMachineVersionArn);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update state machine");
        }
    }

    /// <summary>
    /// Start an execution of a state machine.
    /// </summary>
    public static async Task<StartExecutionResult> StartExecutionAsync(
        string stateMachineArn,
        string? name = null,
        string? input = null,
        string? traceHeader = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartExecutionRequest
        {
            StateMachineArn = stateMachineArn
        };
        if (name != null) request.Name = name;
        if (input != null) request.Input = input;
        if (traceHeader != null) request.TraceHeader = traceHeader;

        try
        {
            var resp = await client.StartExecutionAsync(request);
            return new StartExecutionResult(
                ExecutionArn: resp.ExecutionArn,
                StartDate: resp.StartDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start execution");
        }
    }

    /// <summary>
    /// Stop an execution of a state machine.
    /// </summary>
    public static async Task<StopExecutionResult> StopExecutionAsync(
        string executionArn,
        string? error = null,
        string? cause = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopExecutionRequest { ExecutionArn = executionArn };
        if (error != null) request.Error = error;
        if (cause != null) request.Cause = cause;

        try
        {
            var resp = await client.StopExecutionAsync(request);
            return new StopExecutionResult(StopDate: resp.StopDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop execution '{executionArn}'");
        }
    }

    /// <summary>
    /// Describe an execution.
    /// </summary>
    public static async Task<DescribeExecutionResult> DescribeExecutionAsync(
        string executionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeExecutionAsync(
                new DescribeExecutionRequest { ExecutionArn = executionArn });
            return new DescribeExecutionResult(
                ExecutionArn: resp.ExecutionArn,
                StateMachineArn: resp.StateMachineArn,
                Name: resp.Name,
                Status: resp.Status?.Value,
                StartDate: resp.StartDate,
                StopDate: resp.StopDate,
                Input: resp.Input,
                Output: resp.Output,
                Error: resp.Error,
                Cause: resp.Cause,
                MapRunArn: resp.MapRunArn,
                RedriveStatus: resp.RedriveStatus?.Value,
                RedriveCount: resp.RedriveCount);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe execution '{executionArn}'");
        }
    }

    /// <summary>
    /// List executions for a state machine.
    /// </summary>
    public static async Task<ListExecutionsResult> ListExecutionsAsync(
        string? stateMachineArn = null,
        string? statusFilter = null,
        string? mapRunArn = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListExecutionsRequest();
        if (stateMachineArn != null) request.StateMachineArn = stateMachineArn;
        if (statusFilter != null)
            request.StatusFilter = new ExecutionStatus(statusFilter);
        if (mapRunArn != null) request.MapRunArn = mapRunArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListExecutionsAsync(request);
            return new ListExecutionsResult(
                Executions: resp.Executions,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list executions");
        }
    }

    /// <summary>
    /// Get execution history events.
    /// </summary>
    public static async Task<GetExecutionHistoryResult> GetExecutionHistoryAsync(
        string executionArn,
        bool? reverseOrder = null,
        bool? includeExecutionData = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetExecutionHistoryRequest
        {
            ExecutionArn = executionArn
        };
        if (reverseOrder.HasValue) request.ReverseOrder = reverseOrder.Value;
        if (includeExecutionData.HasValue)
            request.IncludeExecutionData = includeExecutionData.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetExecutionHistoryAsync(request);
            return new GetExecutionHistoryResult(
                Events: resp.Events,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get execution history for '{executionArn}'");
        }
    }

    /// <summary>
    /// Start a synchronous execution of an Express state machine.
    /// </summary>
    public static async Task<StartSyncExecutionResult> StartSyncExecutionAsync(
        string stateMachineArn,
        string? name = null,
        string? input = null,
        string? traceHeader = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartSyncExecutionRequest
        {
            StateMachineArn = stateMachineArn
        };
        if (name != null) request.Name = name;
        if (input != null) request.Input = input;
        if (traceHeader != null) request.TraceHeader = traceHeader;

        try
        {
            var resp = await client.StartSyncExecutionAsync(request);
            return new StartSyncExecutionResult(
                ExecutionArn: resp.ExecutionArn,
                StateMachineArn: resp.StateMachineArn,
                Name: resp.Name,
                StartDate: resp.StartDate,
                StopDate: resp.StopDate,
                Status: resp.Status?.Value,
                Error: resp.Error,
                Cause: resp.Cause,
                Input: resp.Input,
                Output: resp.Output);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start sync execution");
        }
    }

    /// <summary>
    /// Report task success to Step Functions.
    /// </summary>
    public static async Task<SendTaskSuccessResult> SendTaskSuccessAsync(
        string taskToken,
        string output,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SendTaskSuccessAsync(new SendTaskSuccessRequest
            {
                TaskToken = taskToken,
                Output = output
            });
            return new SendTaskSuccessResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send task success");
        }
    }

    /// <summary>
    /// Report task failure to Step Functions.
    /// </summary>
    public static async Task<SendTaskFailureResult> SendTaskFailureAsync(
        string taskToken,
        string? error = null,
        string? cause = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendTaskFailureRequest { TaskToken = taskToken };
        if (error != null) request.Error = error;
        if (cause != null) request.Cause = cause;

        try
        {
            await client.SendTaskFailureAsync(request);
            return new SendTaskFailureResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send task failure");
        }
    }

    /// <summary>
    /// Send a heartbeat for a long-running task.
    /// </summary>
    public static async Task<SendTaskHeartbeatResult> SendTaskHeartbeatAsync(
        string taskToken,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SendTaskHeartbeatAsync(new SendTaskHeartbeatRequest
            {
                TaskToken = taskToken
            });
            return new SendTaskHeartbeatResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send task heartbeat");
        }
    }

    /// <summary>
    /// Describe an activity.
    /// </summary>
    public static async Task<DescribeActivityResult> DescribeActivityAsync(
        string activityArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeActivityAsync(
                new DescribeActivityRequest { ActivityArn = activityArn });
            return new DescribeActivityResult(
                ActivityArn: resp.ActivityArn,
                Name: resp.Name,
                CreationDate: resp.CreationDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe activity '{activityArn}'");
        }
    }

    /// <summary>
    /// Create an activity.
    /// </summary>
    public static async Task<CreateActivityResult> CreateActivityAsync(
        string name,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateActivityRequest { Name = name };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateActivityAsync(request);
            return new CreateActivityResult(
                ActivityArn: resp.ActivityArn,
                CreationDate: resp.CreationDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create activity '{name}'");
        }
    }

    /// <summary>
    /// Delete an activity.
    /// </summary>
    public static async Task<DeleteActivityResult> DeleteActivityAsync(
        string activityArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteActivityAsync(new DeleteActivityRequest
            {
                ActivityArn = activityArn
            });
            return new DeleteActivityResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete activity '{activityArn}'");
        }
    }

    /// <summary>
    /// List activities.
    /// </summary>
    public static async Task<ListActivitiesResult> ListActivitiesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListActivitiesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListActivitiesAsync(request);
            return new ListActivitiesResult(
                Activities: resp.Activities,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list activities");
        }
    }

    /// <summary>
    /// Get an activity task (long-poll for a worker).
    /// </summary>
    public static async Task<GetActivityTaskResult> GetActivityTaskAsync(
        string activityArn,
        string? workerName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetActivityTaskRequest { ActivityArn = activityArn };
        if (workerName != null) request.WorkerName = workerName;

        try
        {
            var resp = await client.GetActivityTaskAsync(request);
            return new GetActivityTaskResult(
                TaskToken: resp.TaskToken,
                Input: resp.Input);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get activity task for '{activityArn}'");
        }
    }

    /// <summary>
    /// Add tags to a Step Functions resource.
    /// </summary>
    public static async Task<SfnTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
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
            return new SfnTagResourceResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resource");
        }
    }

    /// <summary>
    /// Remove tags from a Step Functions resource.
    /// </summary>
    public static async Task<SfnUntagResourceResult> UntagResourceAsync(
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
            return new SfnUntagResourceResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag resource");
        }
    }

    /// <summary>
    /// List tags for a Step Functions resource.
    /// </summary>
    public static async Task<SfnListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new SfnListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags for resource");
        }
    }

    /// <summary>
    /// Describe the state machine associated with an execution.
    /// </summary>
    public static async Task<DescribeStateMachineForExecutionResult>
        DescribeStateMachineForExecutionAsync(
            string executionArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStateMachineForExecutionAsync(
                new DescribeStateMachineForExecutionRequest
                {
                    ExecutionArn = executionArn
                });
            return new DescribeStateMachineForExecutionResult(
                StateMachineArn: resp.StateMachineArn,
                Name: resp.Name,
                Definition: resp.Definition,
                RoleArn: resp.RoleArn,
                UpdateDate: resp.UpdateDate,
                RevisionId: resp.RevisionId,
                Description: null);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe state machine for execution '{executionArn}'");
        }
    }

    /// <summary>
    /// Describe a map run.
    /// </summary>
    public static async Task<DescribeMapRunResult> DescribeMapRunAsync(
        string mapRunArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeMapRunAsync(
                new DescribeMapRunRequest { MapRunArn = mapRunArn });
            return new DescribeMapRunResult(
                MapRunArn: resp.MapRunArn,
                ExecutionArn: resp.ExecutionArn,
                Status: resp.Status?.Value,
                StartDate: resp.StartDate,
                StopDate: resp.StopDate,
                MaxConcurrency: resp.MaxConcurrency,
                ToleratedFailurePercentage: resp.ToleratedFailurePercentage,
                ToleratedFailureCount: resp.ToleratedFailureCount);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe map run '{mapRunArn}'");
        }
    }

    /// <summary>
    /// List map runs for an execution.
    /// </summary>
    public static async Task<ListMapRunsResult> ListMapRunsAsync(
        string executionArn,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMapRunsRequest { ExecutionArn = executionArn };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListMapRunsAsync(request);
            return new ListMapRunsResult(
                MapRuns: resp.MapRuns,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list map runs");
        }
    }

    /// <summary>
    /// Update a map run configuration.
    /// </summary>
    public static async Task<UpdateMapRunResult> UpdateMapRunAsync(
        string mapRunArn,
        long? maxConcurrency = null,
        float? toleratedFailurePercentage = null,
        long? toleratedFailureCount = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateMapRunRequest { MapRunArn = mapRunArn };
        if (maxConcurrency.HasValue) request.MaxConcurrency = (int)maxConcurrency.Value;
        if (toleratedFailurePercentage.HasValue)
            request.ToleratedFailurePercentage = toleratedFailurePercentage.Value;
        if (toleratedFailureCount.HasValue)
            request.ToleratedFailureCount = toleratedFailureCount.Value;

        try
        {
            await client.UpdateMapRunAsync(request);
            return new UpdateMapRunResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update map run '{mapRunArn}'");
        }
    }

    /// <summary>
    /// Publish a version of a state machine.
    /// </summary>
    public static async Task<PublishStateMachineVersionResult>
        PublishStateMachineVersionAsync(
            string stateMachineArn,
            string? revisionId = null,
            string? description = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishStateMachineVersionRequest
        {
            StateMachineArn = stateMachineArn
        };
        if (revisionId != null) request.RevisionId = revisionId;
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.PublishStateMachineVersionAsync(request);
            return new PublishStateMachineVersionResult(
                StateMachineVersionArn: resp.StateMachineVersionArn,
                CreationDate: resp.CreationDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to publish state machine version");
        }
    }

    /// <summary>
    /// List versions of a state machine.
    /// </summary>
    public static async Task<ListStateMachineVersionsResult>
        ListStateMachineVersionsAsync(
            string stateMachineArn,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStateMachineVersionsRequest
        {
            StateMachineArn = stateMachineArn
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListStateMachineVersionsAsync(request);
            return new ListStateMachineVersionsResult(
                StateMachineVersions: resp.StateMachineVersions,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list state machine versions");
        }
    }

    /// <summary>
    /// Create a state machine alias.
    /// </summary>
    public static async Task<CreateStateMachineAliasResult>
        CreateStateMachineAliasAsync(
            CreateStateMachineAliasRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStateMachineAliasAsync(request);
            return new CreateStateMachineAliasResult(
                StateMachineAliasArn: resp.StateMachineAliasArn,
                CreationDate: resp.CreationDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create state machine alias");
        }
    }

    /// <summary>
    /// Describe a state machine alias.
    /// </summary>
    public static async Task<DescribeStateMachineAliasResult>
        DescribeStateMachineAliasAsync(
            string stateMachineAliasArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStateMachineAliasAsync(
                new DescribeStateMachineAliasRequest
                {
                    StateMachineAliasArn = stateMachineAliasArn
                });
            return new DescribeStateMachineAliasResult(
                StateMachineAliasArn: resp.StateMachineAliasArn,
                Name: resp.Name,
                Description: resp.Description,
                RoutingConfiguration: resp.RoutingConfiguration,
                CreationDate: resp.CreationDate,
                UpdateDate: resp.UpdateDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe state machine alias '{stateMachineAliasArn}'");
        }
    }

    /// <summary>
    /// Update a state machine alias.
    /// </summary>
    public static async Task<UpdateStateMachineAliasResult>
        UpdateStateMachineAliasAsync(
            UpdateStateMachineAliasRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateStateMachineAliasAsync(request);
            return new UpdateStateMachineAliasResult(UpdateDate: resp.UpdateDate);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update state machine alias");
        }
    }

    /// <summary>
    /// Delete a state machine alias.
    /// </summary>
    public static async Task<DeleteStateMachineAliasResult>
        DeleteStateMachineAliasAsync(
            string stateMachineAliasArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteStateMachineAliasAsync(
                new DeleteStateMachineAliasRequest
                {
                    StateMachineAliasArn = stateMachineAliasArn
                });
            return new DeleteStateMachineAliasResult();
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete state machine alias '{stateMachineAliasArn}'");
        }
    }

    /// <summary>
    /// List aliases for a state machine.
    /// </summary>
    public static async Task<ListStateMachineAliasesResult>
        ListStateMachineAliasesAsync(
            string stateMachineArn,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStateMachineAliasesRequest
        {
            StateMachineArn = stateMachineArn
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListStateMachineAliasesAsync(request);
            return new ListStateMachineAliasesResult(
                StateMachineAliases: resp.StateMachineAliases,
                NextToken: resp.NextToken);
        }
        catch (AmazonStepFunctionsException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list state machine aliases");
        }
    }
}
