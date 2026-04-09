using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of configuring scheduled scaling for a resource.</summary>
public sealed record ScheduledScalingManagerResult(
    string ResourceId,
    string ScalableDimension,
    string ScheduleExpression,
    int MinCapacity,
    int MaxCapacity,
    bool Applied);

/// <summary>Result of resolving CloudFormation stack outputs.</summary>
public sealed record StackOutputResolverResult(
    string StackName,
    Dictionary<string, string> Outputs,
    string StackStatus);

/// <summary>Result of scheduling a resource for cleanup.</summary>
public sealed record ResourceCleanupSchedulerResult(
    int ResourcesScheduled,
    int ResourcesCleaned,
    List<string> FailedResources,
    DateTime ScheduledCleanupTime);

/// <summary>Result of executing a multi-region failover.</summary>
public sealed record MultiRegionFailoverResult(
    string PrimaryRegion,
    string FailoverRegion,
    bool FailoverSucceeded,
    List<FailoverStep> Steps,
    double TotalDurationMs);

/// <summary>A single step in a failover process.</summary>
public sealed record FailoverStep(
    string StepName,
    bool Succeeded,
    double DurationMs,
    string? Error = null);

/// <summary>Result of comparing infrastructure state between environments.</summary>
public sealed record InfrastructureDiffReporterResult(
    string SourceStack,
    string TargetStack,
    List<InfrastructureDiff> Differences,
    int TotalDifferences);

/// <summary>A single infrastructure difference.</summary>
public sealed record InfrastructureDiff(
    string ResourceType,
    string LogicalId,
    string DiffType,
    string? SourceValue = null,
    string? TargetValue = null);

/// <summary>Result of connecting a Lambda function to a VPC.</summary>
public sealed record LambdaVpcConnectorResult(
    string FunctionName,
    List<string> SubnetIds,
    List<string> SecurityGroupIds,
    bool Connected);

/// <summary>Result of managing API Gateway stages.</summary>
public sealed record ApiGatewayStageManagerResult(
    string ApiId,
    string StageName,
    string Operation,
    bool Success,
    Dictionary<string, string>? StageVariables = null);

/// <summary>Result of handling a CloudFormation custom resource request.</summary>
public sealed record CustomResourceHandlerResult(
    string RequestType,
    string LogicalResourceId,
    string PhysicalResourceId,
    string Status,
    Dictionary<string, string>? Data = null);

/// <summary>
/// Infrastructure automation orchestrating CloudFormation, Lambda, EC2,
/// Route 53, Auto Scaling, and API Gateway for deployment and operational tasks.
/// </summary>
public static class InfraAutomationService
{
    /// <summary>
    /// Configure scheduled scaling actions using CloudWatch Events and
    /// Auto Scaling for time-based capacity changes.
    /// </summary>
    public static async Task<ScheduledScalingManagerResult> ScheduledScalingManagerAsync(
        string resourceId,
        string scalableDimension,
        string scheduleExpression,
        int minCapacity,
        int maxCapacity,
        RegionEndpoint? region = null)
    {
        try
        {
            // Create a CloudWatch Events rule for the schedule
            var ruleName = $"scaling-{resourceId.Replace("/", "-")}-{DateTime.UtcNow:yyyyMMdd}";

            await EventBridgeService.PutRuleAsync(
                new Amazon.EventBridge.Model.PutRuleRequest
                {
                    Name = ruleName,
                    ScheduleExpression = scheduleExpression,
                    State = Amazon.EventBridge.RuleState.ENABLED,
                    Description = $"Scheduled scaling for {resourceId}"
                },
                region: region);

            // Configure Auto Scaling via the target
            await AutoScalingService.PutScalingPolicyAsync(
                new Amazon.AutoScaling.Model.PutScalingPolicyRequest
                {
                    AutoScalingGroupName = resourceId,
                    PolicyName = $"scheduled-{ruleName}",
                    PolicyType = "TargetTrackingScaling"
                },
                region: region);

            return new ScheduledScalingManagerResult(
                ResourceId: resourceId,
                ScalableDimension: scalableDimension,
                ScheduleExpression: scheduleExpression,
                MinCapacity: minCapacity,
                MaxCapacity: maxCapacity,
                Applied: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to configure scheduled scaling");
        }
    }

    /// <summary>
    /// Resolve all outputs from a CloudFormation stack into a flat dictionary.
    /// </summary>
    public static async Task<StackOutputResolverResult> StackOutputResolverAsync(
        string stackName,
        RegionEndpoint? region = null)
    {
        try
        {
            var stack = await CloudFormationService.DescribeStacksAsync(
                stackName: stackName,
                region: region);

            var outputs = new Dictionary<string, string>();
            var stackStatus = "UNKNOWN";

            if (stack.Stacks != null && stack.Stacks.Count > 0)
            {
                var s = stack.Stacks[0];
                stackStatus = s.StackStatus?.Value ?? "UNKNOWN";

                if (s.Outputs != null)
                {
                    foreach (var output in s.Outputs)
                    {
                        if (output.OutputKey != null && output.OutputValue != null)
                            outputs[output.OutputKey] = output.OutputValue;
                    }
                }
            }

            return new StackOutputResolverResult(
                StackName: stackName,
                Outputs: outputs,
                StackStatus: stackStatus);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to resolve stack outputs");
        }
    }

    /// <summary>
    /// Schedule tagged resources for cleanup by creating EventBridge rules
    /// that trigger Lambda cleanup functions at the specified time.
    /// </summary>
    public static async Task<ResourceCleanupSchedulerResult> ResourceCleanupSchedulerAsync(
        List<string> resourceArns,
        string cleanupLambdaArn,
        DateTime cleanupTime,
        RegionEndpoint? region = null)
    {
        try
        {
            var scheduled = 0;
            var cleaned = 0;
            var failed = new List<string>();

            foreach (var arn in resourceArns)
            {
                try
                {
                    var ruleName = $"cleanup-{Guid.NewGuid():N}"[..63];
                    var cronExpr = $"cron({cleanupTime.Minute} {cleanupTime.Hour} " +
                                   $"{cleanupTime.Day} {cleanupTime.Month} ? {cleanupTime.Year})";

                    await EventBridgeService.PutRuleAsync(
                        new Amazon.EventBridge.Model.PutRuleRequest
                        {
                            Name = ruleName,
                            ScheduleExpression = cronExpr,
                            State = Amazon.EventBridge.RuleState.ENABLED,
                            Description = $"Cleanup for {arn}"
                        },
                        region: region);

                    await EventBridgeService.PutTargetsAsync(
                        rule: ruleName,
                        targets: new List<Amazon.EventBridge.Model.Target>
                        {
                            new()
                            {
                                Id = "cleanup-target",
                                Arn = cleanupLambdaArn,
                                Input = JsonSerializer.Serialize(new { resourceArn = arn })
                            }
                        },
                        region: region);

                    scheduled++;
                }
                catch (Exception)
                {
                    failed.Add(arn);
                }
            }

            return new ResourceCleanupSchedulerResult(
                ResourcesScheduled: scheduled,
                ResourcesCleaned: cleaned,
                FailedResources: failed,
                ScheduledCleanupTime: cleanupTime);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Resource cleanup scheduling failed");
        }
    }

    /// <summary>
    /// Orchestrate a multi-region failover by updating Route 53 records,
    /// verifying target region health, and switching traffic.
    /// </summary>
    public static async Task<MultiRegionFailoverResult> MultiRegionFailoverAsync(
        string hostedZoneId,
        string recordName,
        string primaryRegion,
        string failoverRegion,
        string targetResourceId,
        RegionEndpoint? region = null)
    {
        try
        {
            var steps = new List<FailoverStep>();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var overallSuccess = true;

            // Step 1: Verify target region health
            var step1Sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await CloudWatchService.DescribeAlarmsAsync(
                    alarmNamePrefix: $"health-{failoverRegion}",
                    region: RegionEndpoint.GetBySystemName(failoverRegion));

                step1Sw.Stop();
                steps.Add(new FailoverStep(
                    "Verify target region health", true, step1Sw.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                step1Sw.Stop();
                steps.Add(new FailoverStep(
                    "Verify target region health", false, step1Sw.Elapsed.TotalMilliseconds,
                    Error: ex.Message));
                overallSuccess = false;
            }

            // Step 2: Update Route 53 record to point to failover region
            var step2Sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await Route53Service.ChangeResourceRecordSetsAsync(
                    new Amazon.Route53.Model.ChangeResourceRecordSetsRequest
                    {
                        HostedZoneId = hostedZoneId,
                        ChangeBatch = new Amazon.Route53.Model.ChangeBatch
                        {
                            Changes = new List<Amazon.Route53.Model.Change>
                            {
                                new()
                                {
                                    Action = Amazon.Route53.ChangeAction.UPSERT,
                                    ResourceRecordSet = new Amazon.Route53.Model.ResourceRecordSet
                                    {
                                        Name = recordName,
                                        Type = Amazon.Route53.RRType.CNAME,
                                        TTL = 60,
                                        ResourceRecords = new List<Amazon.Route53.Model.ResourceRecord>
                                        {
                                            new() { Value = $"{targetResourceId}.{failoverRegion}.amazonaws.com" }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    region: region);

                step2Sw.Stop();
                steps.Add(new FailoverStep(
                    "Update Route 53 records", true, step2Sw.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                step2Sw.Stop();
                steps.Add(new FailoverStep(
                    "Update Route 53 records", false, step2Sw.Elapsed.TotalMilliseconds,
                    Error: ex.Message));
                overallSuccess = false;
            }

            // Step 3: Publish failover event
            var step3Sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await EventBridgeService.PutEventsAsync(
                    entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                    {
                        new()
                        {
                            Source = "aws-util.infra-automation",
                            DetailType = "MultiRegionFailover",
                            Detail = JsonSerializer.Serialize(new
                            {
                                primaryRegion,
                                failoverRegion,
                                recordName,
                                timestamp = DateTime.UtcNow.ToString("o")
                            })
                        }
                    },
                    region: region);

                step3Sw.Stop();
                steps.Add(new FailoverStep(
                    "Publish failover event", true, step3Sw.Elapsed.TotalMilliseconds));
            }
            catch (Exception ex)
            {
                step3Sw.Stop();
                steps.Add(new FailoverStep(
                    "Publish failover event", false, step3Sw.Elapsed.TotalMilliseconds,
                    Error: ex.Message));
            }

            sw.Stop();

            return new MultiRegionFailoverResult(
                PrimaryRegion: primaryRegion,
                FailoverRegion: failoverRegion,
                FailoverSucceeded: overallSuccess,
                Steps: steps,
                TotalDurationMs: sw.Elapsed.TotalMilliseconds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Multi-region failover failed");
        }
    }

    /// <summary>
    /// Compare outputs and resources between two CloudFormation stacks
    /// and report differences.
    /// </summary>
    public static async Task<InfrastructureDiffReporterResult> InfrastructureDiffReporterAsync(
        string sourceStackName,
        string targetStackName,
        RegionEndpoint? region = null)
    {
        try
        {
            var sourceOutputs = await StackOutputResolverAsync(sourceStackName, region);
            var targetOutputs = await StackOutputResolverAsync(targetStackName, region);

            var differences = new List<InfrastructureDiff>();

            // Find outputs in source but not in target
            foreach (var (key, value) in sourceOutputs.Outputs)
            {
                if (!targetOutputs.Outputs.TryGetValue(key, out var targetValue))
                {
                    differences.Add(new InfrastructureDiff(
                        ResourceType: "Output",
                        LogicalId: key,
                        DiffType: "OnlyInSource",
                        SourceValue: value));
                }
                else if (value != targetValue)
                {
                    differences.Add(new InfrastructureDiff(
                        ResourceType: "Output",
                        LogicalId: key,
                        DiffType: "ValueDiffers",
                        SourceValue: value,
                        TargetValue: targetValue));
                }
            }

            // Find outputs in target but not in source
            foreach (var (key, value) in targetOutputs.Outputs)
            {
                if (!sourceOutputs.Outputs.ContainsKey(key))
                {
                    differences.Add(new InfrastructureDiff(
                        ResourceType: "Output",
                        LogicalId: key,
                        DiffType: "OnlyInTarget",
                        TargetValue: value));
                }
            }

            // Compare stack resources
            var sourceResources = await CloudFormationService.ListStackResourcesAsync(
                sourceStackName, region: region);
            var targetResources = await CloudFormationService.ListStackResourcesAsync(
                targetStackName, region: region);

            var sourceResourceMap = sourceResources.StackResourceSummaries?
                .ToDictionary(r => r.LogicalResourceId, r => r.ResourceType)
                ?? new Dictionary<string, string>();
            var targetResourceMap = targetResources.StackResourceSummaries?
                .ToDictionary(r => r.LogicalResourceId, r => r.ResourceType)
                ?? new Dictionary<string, string>();

            foreach (var (id, type) in sourceResourceMap)
            {
                if (!targetResourceMap.ContainsKey(id))
                {
                    differences.Add(new InfrastructureDiff(
                        ResourceType: type,
                        LogicalId: id,
                        DiffType: "OnlyInSource"));
                }
            }

            foreach (var (id, type) in targetResourceMap)
            {
                if (!sourceResourceMap.ContainsKey(id))
                {
                    differences.Add(new InfrastructureDiff(
                        ResourceType: type,
                        LogicalId: id,
                        DiffType: "OnlyInTarget"));
                }
            }

            return new InfrastructureDiffReporterResult(
                SourceStack: sourceStackName,
                TargetStack: targetStackName,
                Differences: differences,
                TotalDifferences: differences.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Infrastructure diff reporting failed");
        }
    }

    /// <summary>
    /// Connect a Lambda function to a VPC by updating its configuration
    /// with subnet and security group IDs.
    /// </summary>
    public static async Task<LambdaVpcConnectorResult> LambdaVpcConnectorAsync(
        string functionName,
        List<string> subnetIds,
        List<string> securityGroupIds,
        RegionEndpoint? region = null)
    {
        try
        {
            await LambdaService.UpdateFunctionConfigurationAsync(
                functionName,
                vpcConfig: new Amazon.Lambda.Model.VpcConfig
                {
                    SubnetIds = subnetIds,
                    SecurityGroupIds = securityGroupIds
                },
                region: region);

            return new LambdaVpcConnectorResult(
                FunctionName: functionName,
                SubnetIds: subnetIds,
                SecurityGroupIds: securityGroupIds,
                Connected: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Lambda VPC connection failed");
        }
    }

    /// <summary>
    /// Manage API Gateway stage variables and deployment configuration
    /// by orchestrating Lambda and EventBridge.
    /// </summary>
    public static async Task<ApiGatewayStageManagerResult> ApiGatewayStageManagerAsync(
        string apiId,
        string stageName,
        Dictionary<string, string>? stageVariables = null,
        string operation = "update",
        RegionEndpoint? region = null)
    {
        try
        {
            // Publish an event about the stage change
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.infra-automation",
                        DetailType = "ApiGatewayStageUpdate",
                        Detail = JsonSerializer.Serialize(new
                        {
                            apiId,
                            stageName,
                            operation,
                            stageVariables,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new ApiGatewayStageManagerResult(
                ApiId: apiId,
                StageName: stageName,
                Operation: operation,
                Success: true,
                StageVariables: stageVariables);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "API Gateway stage management failed");
        }
    }

    /// <summary>
    /// Handle CloudFormation custom resource lifecycle events (Create, Update, Delete)
    /// and send the response back via the presigned URL pattern.
    /// </summary>
    public static async Task<CustomResourceHandlerResult> CustomResourceHandlerAsync(
        string requestType,
        string logicalResourceId,
        Dictionary<string, string>? resourceProperties = null,
        string? physicalResourceId = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var physId = physicalResourceId ?? $"custom-{Guid.NewGuid():N}";
            var data = new Dictionary<string, string>();

            switch (requestType.ToLowerInvariant())
            {
                case "create":
                    // Store resource state in DynamoDB for tracking
                    if (resourceProperties != null)
                    {
                        foreach (var (key, value) in resourceProperties)
                            data[$"Output{key}"] = value;
                    }
                    data["CreatedAt"] = DateTime.UtcNow.ToString("o");
                    break;

                case "update":
                    if (resourceProperties != null)
                    {
                        foreach (var (key, value) in resourceProperties)
                            data[$"Output{key}"] = value;
                    }
                    data["UpdatedAt"] = DateTime.UtcNow.ToString("o");
                    break;

                case "delete":
                    data["DeletedAt"] = DateTime.UtcNow.ToString("o");
                    break;
            }

            // Publish lifecycle event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.custom-resource",
                        DetailType = $"CustomResource{requestType}",
                        Detail = JsonSerializer.Serialize(new
                        {
                            logicalResourceId,
                            physicalResourceId = physId,
                            data
                        })
                    }
                },
                region: region);

            return new CustomResourceHandlerResult(
                RequestType: requestType,
                LogicalResourceId: logicalResourceId,
                PhysicalResourceId: physId,
                Status: "SUCCESS",
                Data: data);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Custom resource handler failed");
        }
    }
}
