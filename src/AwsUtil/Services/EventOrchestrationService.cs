using System.Text.Json;
using Amazon;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.Pipes;
using Amazon.Pipes.Model;
using Amazon.Scheduler;
using Amazon.Scheduler.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of creating an EventBridge rule.</summary>
public sealed record CreateEventBridgeRuleResult(
    string? RuleArn = null);

/// <summary>Result of putting targets on an EventBridge rule.</summary>
public sealed record PutEventBridgeTargetsResult(
    int? FailedEntryCount = null,
    List<PutTargetsResultEntry>? FailedEntries = null);

/// <summary>Result of deleting an EventBridge rule.</summary>
public sealed record DeleteEventBridgeRuleResult(bool Success = true);

/// <summary>Result of creating an EventBridge schedule.</summary>
public sealed record CreateScheduleResult(
    string? ScheduleArn = null);

/// <summary>Result of deleting an EventBridge schedule.</summary>
public sealed record DeleteScheduleResult(bool Success = true);

/// <summary>Result of running a Step Functions workflow to completion.</summary>
public sealed record RunWorkflowResult(
    string? ExecutionArn = null,
    string? Status = null,
    string? Output = null,
    string? Error = null,
    string? Cause = null,
    DateTime? StartDate = null,
    DateTime? StopDate = null);

/// <summary>Represents a single step in a saga.</summary>
public sealed record SagaStep(
    string Name,
    Func<Task<string>> Action,
    Func<string, Task> Compensate);

/// <summary>Result of a saga orchestration.</summary>
public sealed record SagaOrchestratorResult(
    bool Succeeded = false,
    List<string>? CompletedSteps = null,
    List<string>? CompensatedSteps = null,
    string? FailedStep = null,
    string? Error = null);

/// <summary>Represents a single fan-out target.</summary>
public sealed record FanOutTarget(
    string Name,
    Func<Task<string>> Action);

/// <summary>Result of a fan-out/fan-in operation.</summary>
public sealed record FanOutFanInResult(
    Dictionary<string, string>? Results = null,
    Dictionary<string, string>? Errors = null);

/// <summary>Result of starting an event replay.</summary>
public sealed record StartEventReplayResult(
    string? ReplayArn = null,
    string? State = null,
    string? StateReason = null,
    DateTime? ReplayStartTime = null);

/// <summary>Result of describing an event replay.</summary>
public sealed record DescribeEventReplayResult(
    string? ReplayName = null,
    string? ReplayArn = null,
    string? State = null,
    string? StateReason = null,
    string? EventSourceArn = null,
    DateTime? EventStartTime = null,
    DateTime? EventEndTime = null,
    DateTime? EventLastReplayedTime = null,
    DateTime? ReplayStartTime = null,
    DateTime? ReplayEndTime = null);

/// <summary>Result of creating an EventBridge Pipe.</summary>
public sealed record CreatePipeResult(
    string? Arn = null,
    string? Name = null,
    string? DesiredState = null,
    string? CurrentState = null,
    DateTime? CreationTime = null,
    DateTime? LastModifiedTime = null);

/// <summary>Result of deleting an EventBridge Pipe.</summary>
public sealed record DeletePipeResult(
    string? Arn = null,
    string? Name = null,
    string? DesiredState = null,
    string? CurrentState = null);

/// <summary>Result of creating an SQS event source mapping.</summary>
public sealed record CreateSqsEventSourceMappingResult(
    string? Uuid = null,
    string? EventSourceArn = null,
    string? FunctionArn = null,
    string? State = null);

/// <summary>Result of deleting an event source mapping.</summary>
public sealed record DeleteEventSourceMappingResult(
    string? Uuid = null,
    string? State = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Event-driven orchestration combining EventBridge, StepFunctions, SQS,
/// EventBridge Scheduler, and EventBridge Pipes.
/// </summary>
public static class EventOrchestrationService
{
    private static AmazonEventBridgeClient GetEbClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEventBridgeClient>(region);

    private static AmazonStepFunctionsClient GetSfnClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonStepFunctionsClient>(region);

    private static AmazonLambdaClient GetLambdaClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLambdaClient>(region);

    private static AmazonSchedulerClient GetSchedulerClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSchedulerClient>(region);

    private static AmazonPipesClient GetPipesClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonPipesClient>(region);

    private static AmazonSQSClient GetSqsClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSQSClient>(region);

    /// <summary>
    /// Create an EventBridge rule with an event pattern or schedule expression.
    /// </summary>
    public static async Task<CreateEventBridgeRuleResult> CreateEventBridgeRuleAsync(
        string ruleName,
        string? eventPattern = null,
        string? scheduleExpression = null,
        string? description = null,
        string? eventBusName = null,
        string? roleArn = null,
        string? state = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetEbClient(region);
        var request = new PutRuleRequest { Name = ruleName };
        if (eventPattern != null) request.EventPattern = eventPattern;
        if (scheduleExpression != null) request.ScheduleExpression = scheduleExpression;
        if (description != null) request.Description = description;
        if (eventBusName != null) request.EventBusName = eventBusName;
        if (roleArn != null) request.RoleArn = roleArn;
        if (state != null) request.State = new RuleState(state);
        if (tags != null)
            request.Tags = tags.Select(kv => new Amazon.EventBridge.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            var resp = await client.PutRuleAsync(request);
            return new CreateEventBridgeRuleResult(RuleArn: resp.RuleArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create EventBridge rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Add targets to an EventBridge rule.
    /// </summary>
    public static async Task<PutEventBridgeTargetsResult> PutEventBridgeTargetsAsync(
        string ruleName,
        List<Amazon.EventBridge.Model.Target> targets,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        var client = GetEbClient(region);
        var request = new PutTargetsRequest
        {
            Rule = ruleName,
            Targets = targets
        };
        if (eventBusName != null) request.EventBusName = eventBusName;

        try
        {
            var resp = await client.PutTargetsAsync(request);
            return new PutEventBridgeTargetsResult(
                FailedEntryCount: resp.FailedEntryCount,
                FailedEntries: resp.FailedEntries);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put targets on rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Delete an EventBridge rule and its targets.
    /// </summary>
    public static async Task<DeleteEventBridgeRuleResult> DeleteEventBridgeRuleAsync(
        string ruleName,
        string? eventBusName = null,
        bool removeTargetsFirst = true,
        RegionEndpoint? region = null)
    {
        var client = GetEbClient(region);

        try
        {
            if (removeTargetsFirst)
            {
                var listReq = new ListTargetsByRuleRequest { Rule = ruleName };
                if (eventBusName != null) listReq.EventBusName = eventBusName;
                var listResp = await client.ListTargetsByRuleAsync(listReq);

                if (listResp.Targets is { Count: > 0 })
                {
                    var removeReq = new RemoveTargetsRequest
                    {
                        Rule = ruleName,
                        Ids = listResp.Targets.Select(t => t.Id).ToList()
                    };
                    if (eventBusName != null) removeReq.EventBusName = eventBusName;
                    await client.RemoveTargetsAsync(removeReq);
                }
            }

            var deleteReq = new DeleteRuleRequest { Name = ruleName };
            if (eventBusName != null) deleteReq.EventBusName = eventBusName;
            await client.DeleteRuleAsync(deleteReq);

            return new DeleteEventBridgeRuleResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete EventBridge rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Create an EventBridge Scheduler schedule.
    /// </summary>
    public static async Task<CreateScheduleResult> CreateScheduleAsync(
        string scheduleName,
        string scheduleExpression,
        string targetArn,
        string roleArn,
        string? input = null,
        string? groupName = null,
        string? description = null,
        string? timezone = null,
        DateTime? startDate = null,
        DateTime? endDate = null,
        string? state = null,
        RegionEndpoint? region = null)
    {
        var client = GetSchedulerClient(region);
        var request = new CreateScheduleRequest
        {
            Name = scheduleName,
            ScheduleExpression = scheduleExpression,
            Target = new Amazon.Scheduler.Model.Target
            {
                Arn = targetArn,
                RoleArn = roleArn,
                Input = input
            },
            FlexibleTimeWindow = new FlexibleTimeWindow
            {
                Mode = FlexibleTimeWindowMode.OFF
            }
        };
        if (groupName != null) request.GroupName = groupName;
        if (description != null) request.Description = description;
        if (timezone != null) request.ScheduleExpressionTimezone = timezone;
        if (startDate.HasValue) request.StartDate = startDate.Value;
        if (endDate.HasValue) request.EndDate = endDate.Value;
        if (state != null) request.State = new ScheduleState(state);

        try
        {
            var resp = await client.CreateScheduleAsync(request);
            return new CreateScheduleResult(ScheduleArn: resp.ScheduleArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create schedule '{scheduleName}'");
        }
    }

    /// <summary>
    /// Delete an EventBridge Scheduler schedule.
    /// </summary>
    public static async Task<DeleteScheduleResult> DeleteScheduleAsync(
        string scheduleName,
        string? groupName = null,
        RegionEndpoint? region = null)
    {
        var client = GetSchedulerClient(region);
        var request = new DeleteScheduleRequest { Name = scheduleName };
        if (groupName != null) request.GroupName = groupName;

        try
        {
            await client.DeleteScheduleAsync(request);
            return new DeleteScheduleResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete schedule '{scheduleName}'");
        }
    }

    /// <summary>
    /// Start a Step Functions execution and poll until completion.
    /// </summary>
    public static async Task<RunWorkflowResult> RunWorkflowAsync(
        string stateMachineArn,
        string? input = null,
        string? name = null,
        int pollIntervalSeconds = 5,
        int timeoutSeconds = 300,
        RegionEndpoint? region = null)
    {
        var client = GetSfnClient(region);

        try
        {
            var startReq = new StartExecutionRequest
            {
                StateMachineArn = stateMachineArn
            };
            if (input != null) startReq.Input = input;
            if (name != null) startReq.Name = name;

            var startResp = await client.StartExecutionAsync(startReq);
            var executionArn = startResp.ExecutionArn;

            var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
            while (DateTime.UtcNow < deadline)
            {
                var descResp = await client.DescribeExecutionAsync(
                    new DescribeExecutionRequest { ExecutionArn = executionArn });

                var status = descResp.Status.Value;
                if (status != "RUNNING")
                {
                    return new RunWorkflowResult(
                        ExecutionArn: executionArn,
                        Status: status,
                        Output: descResp.Output,
                        Error: descResp.Error,
                        Cause: descResp.Cause,
                        StartDate: descResp.StartDate,
                        StopDate: descResp.StopDate);
                }

                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }

            return new RunWorkflowResult(
                ExecutionArn: executionArn,
                Status: "TIMED_OUT_WAITING",
                Error: "Polling timeout exceeded");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to run workflow");
        }
    }

    /// <summary>
    /// Execute a saga pattern with compensating transactions on failure.
    /// Steps are executed in order; on failure, completed steps are compensated in reverse.
    /// </summary>
    public static async Task<SagaOrchestratorResult> SagaOrchestratorAsync(
        List<SagaStep> steps)
    {
        var completed = new List<string>();
        var compensated = new List<string>();

        foreach (var step in steps)
        {
            try
            {
                await step.Action();
                completed.Add(step.Name);
            }
            catch (Exception ex)
            {
                var failedStep = step.Name;
                var error = ex.Message;

                // Compensate in reverse order
                for (var i = completed.Count - 1; i >= 0; i--)
                {
                    var completedStep = steps.First(s => s.Name == completed[i]);
                    try
                    {
                        await completedStep.Compensate(error);
                        compensated.Add(completedStep.Name);
                    }
                    catch (Exception)
                    {
                        // Best-effort compensation; continue with remaining
                    }
                }

                return new SagaOrchestratorResult(
                    Succeeded: false,
                    CompletedSteps: completed,
                    CompensatedSteps: compensated,
                    FailedStep: failedStep,
                    Error: error);
            }
        }

        return new SagaOrchestratorResult(
            Succeeded: true,
            CompletedSteps: completed);
    }

    /// <summary>
    /// Fan out work to multiple targets in parallel and collect results.
    /// </summary>
    public static async Task<FanOutFanInResult> FanOutFanInAsync(
        List<FanOutTarget> targets)
    {
        var results = new Dictionary<string, string>();
        var errors = new Dictionary<string, string>();

        var tasks = targets.Select(async target =>
        {
            try
            {
                var result = await target.Action();
                lock (results) results[target.Name] = result;
            }
            catch (Exception ex)
            {
                lock (errors) errors[target.Name] = ex.Message;
            }
        });

        await Task.WhenAll(tasks);

        return new FanOutFanInResult(
            Results: results,
            Errors: errors.Count > 0 ? errors : null);
    }

    /// <summary>
    /// Start an EventBridge event replay from an archive.
    /// </summary>
    public static async Task<StartEventReplayResult> StartEventReplayAsync(
        string replayName,
        string eventSourceArn,
        string destination,
        DateTime eventStartTime,
        DateTime eventEndTime,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetEbClient(region);
        var request = new StartReplayRequest
        {
            ReplayName = replayName,
            EventSourceArn = eventSourceArn,
            Destination = new ReplayDestination { Arn = destination },
            EventStartTime = eventStartTime,
            EventEndTime = eventEndTime
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.StartReplayAsync(request);
            return new StartEventReplayResult(
                ReplayArn: resp.ReplayArn,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                ReplayStartTime: resp.ReplayStartTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start replay '{replayName}'");
        }
    }

    /// <summary>
    /// Describe an EventBridge event replay.
    /// </summary>
    public static async Task<DescribeEventReplayResult> DescribeEventReplayAsync(
        string replayName,
        RegionEndpoint? region = null)
    {
        var client = GetEbClient(region);

        try
        {
            var resp = await client.DescribeReplayAsync(
                new DescribeReplayRequest { ReplayName = replayName });
            return new DescribeEventReplayResult(
                ReplayName: resp.ReplayName,
                ReplayArn: resp.ReplayArn,
                State: resp.State?.Value,
                StateReason: resp.StateReason,
                EventSourceArn: resp.EventSourceArn,
                EventStartTime: resp.EventStartTime,
                EventEndTime: resp.EventEndTime,
                EventLastReplayedTime: resp.EventLastReplayedTime,
                ReplayStartTime: resp.ReplayStartTime,
                ReplayEndTime: resp.ReplayEndTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe replay '{replayName}'");
        }
    }

    /// <summary>
    /// Create an EventBridge Pipe connecting a source to a target.
    /// </summary>
    public static async Task<CreatePipeResult> CreatePipeAsync(
        string pipeName,
        string source,
        string target,
        string roleArn,
        string? description = null,
        string? desiredState = null,
        string? enrichment = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetPipesClient(region);
        var request = new CreatePipeRequest
        {
            Name = pipeName,
            Source = source,
            Target = target,
            RoleArn = roleArn
        };
        if (description != null) request.Description = description;
        if (desiredState != null) request.DesiredState = new RequestedPipeState(desiredState);
        if (enrichment != null) request.Enrichment = enrichment;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePipeAsync(request);
            return new CreatePipeResult(
                Arn: resp.Arn,
                Name: resp.Name,
                DesiredState: resp.DesiredState?.Value,
                CurrentState: resp.CurrentState?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create pipe '{pipeName}'");
        }
    }

    /// <summary>
    /// Delete an EventBridge Pipe.
    /// </summary>
    public static async Task<DeletePipeResult> DeletePipeAsync(
        string pipeName,
        RegionEndpoint? region = null)
    {
        var client = GetPipesClient(region);

        try
        {
            var resp = await client.DeletePipeAsync(
                new DeletePipeRequest { Name = pipeName });
            return new DeletePipeResult(
                Arn: resp.Arn,
                Name: resp.Name,
                DesiredState: resp.DesiredState?.Value,
                CurrentState: resp.CurrentState?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete pipe '{pipeName}'");
        }
    }

    /// <summary>
    /// Create a Lambda event source mapping for an SQS queue.
    /// </summary>
    public static async Task<CreateSqsEventSourceMappingResult> CreateSqsEventSourceMappingAsync(
        string functionName,
        string queueArn,
        int? batchSize = null,
        int? maximumBatchingWindowInSeconds = null,
        bool? enabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);
        var request = new CreateEventSourceMappingRequest
        {
            FunctionName = functionName,
            EventSourceArn = queueArn
        };
        if (batchSize.HasValue) request.BatchSize = batchSize.Value;
        if (maximumBatchingWindowInSeconds.HasValue)
            request.MaximumBatchingWindowInSeconds = maximumBatchingWindowInSeconds.Value;
        if (enabled.HasValue) request.Enabled = enabled.Value;

        try
        {
            var resp = await client.CreateEventSourceMappingAsync(request);
            return new CreateSqsEventSourceMappingResult(
                Uuid: resp.UUID,
                EventSourceArn: resp.EventSourceArn,
                FunctionArn: resp.FunctionArn,
                State: resp.State);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create SQS event source mapping for '{functionName}'");
        }
    }

    /// <summary>
    /// Delete a Lambda event source mapping.
    /// </summary>
    public static async Task<DeleteEventSourceMappingResult> DeleteEventSourceMappingAsync(
        string uuid,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);

        try
        {
            var resp = await client.DeleteEventSourceMappingAsync(
                new DeleteEventSourceMappingRequest { UUID = uuid });
            return new DeleteEventSourceMappingResult(
                Uuid: resp.UUID,
                State: resp.State);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete event source mapping '{uuid}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateEventBridgeRuleAsync"/>.</summary>
    public static CreateEventBridgeRuleResult CreateEventBridgeRule(string ruleName, string? eventPattern = null, string? scheduleExpression = null, string? description = null, string? eventBusName = null, string? roleArn = null, string? state = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateEventBridgeRuleAsync(ruleName, eventPattern, scheduleExpression, description, eventBusName, roleArn, state, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutEventBridgeTargetsAsync"/>.</summary>
    public static PutEventBridgeTargetsResult PutEventBridgeTargets(string ruleName, List<Amazon.EventBridge.Model.Target> targets, string? eventBusName = null, RegionEndpoint? region = null)
        => PutEventBridgeTargetsAsync(ruleName, targets, eventBusName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteEventBridgeRuleAsync"/>.</summary>
    public static DeleteEventBridgeRuleResult DeleteEventBridgeRule(string ruleName, string? eventBusName = null, bool removeTargetsFirst = true, RegionEndpoint? region = null)
        => DeleteEventBridgeRuleAsync(ruleName, eventBusName, removeTargetsFirst, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateScheduleAsync"/>.</summary>
    public static CreateScheduleResult CreateSchedule(string scheduleName, string scheduleExpression, string targetArn, string roleArn, string? input = null, string? groupName = null, string? description = null, string? timezone = null, DateTime? startDate = null, DateTime? endDate = null, string? state = null, RegionEndpoint? region = null)
        => CreateScheduleAsync(scheduleName, scheduleExpression, targetArn, roleArn, input, groupName, description, timezone, startDate, endDate, state, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteScheduleAsync"/>.</summary>
    public static DeleteScheduleResult DeleteSchedule(string scheduleName, string? groupName = null, RegionEndpoint? region = null)
        => DeleteScheduleAsync(scheduleName, groupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RunWorkflowAsync"/>.</summary>
    public static RunWorkflowResult RunWorkflow(string stateMachineArn, string? input = null, string? name = null, int pollIntervalSeconds = 5, int timeoutSeconds = 300, RegionEndpoint? region = null)
        => RunWorkflowAsync(stateMachineArn, input, name, pollIntervalSeconds, timeoutSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SagaOrchestratorAsync"/>.</summary>
    public static SagaOrchestratorResult SagaOrchestrator(List<SagaStep> steps)
        => SagaOrchestratorAsync(steps).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="FanOutFanInAsync"/>.</summary>
    public static FanOutFanInResult FanOutFanIn(List<FanOutTarget> targets)
        => FanOutFanInAsync(targets).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartEventReplayAsync"/>.</summary>
    public static StartEventReplayResult StartEventReplay(string replayName, string eventSourceArn, string destination, DateTime eventStartTime, DateTime eventEndTime, string? description = null, RegionEndpoint? region = null)
        => StartEventReplayAsync(replayName, eventSourceArn, destination, eventStartTime, eventEndTime, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEventReplayAsync"/>.</summary>
    public static DescribeEventReplayResult DescribeEventReplay(string replayName, RegionEndpoint? region = null)
        => DescribeEventReplayAsync(replayName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePipeAsync"/>.</summary>
    public static CreatePipeResult CreatePipe(string pipeName, string source, string target, string roleArn, string? description = null, string? desiredState = null, string? enrichment = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreatePipeAsync(pipeName, source, target, roleArn, description, desiredState, enrichment, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePipeAsync"/>.</summary>
    public static DeletePipeResult DeletePipe(string pipeName, RegionEndpoint? region = null)
        => DeletePipeAsync(pipeName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSqsEventSourceMappingAsync"/>.</summary>
    public static CreateSqsEventSourceMappingResult CreateSqsEventSourceMapping(string functionName, string queueArn, int? batchSize = null, int? maximumBatchingWindowInSeconds = null, bool? enabled = null, RegionEndpoint? region = null)
        => CreateSqsEventSourceMappingAsync(functionName, queueArn, batchSize, maximumBatchingWindowInSeconds, enabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteEventSourceMappingAsync"/>.</summary>
    public static DeleteEventSourceMappingResult DeleteEventSourceMapping(string uuid, RegionEndpoint? region = null)
        => DeleteEventSourceMappingAsync(uuid, region).GetAwaiter().GetResult();

}
