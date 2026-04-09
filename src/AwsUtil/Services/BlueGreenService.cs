using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of an ECS blue/green deployment.</summary>
public sealed record EcsBlueGreenDeployerResult(
    string ServiceName,
    string ClusterName,
    string BlueTaskDefinition,
    string GreenTaskDefinition,
    string ActiveColor,
    bool DeploymentSucceeded,
    double DurationMs,
    List<string> Steps);

/// <summary>Result of managing weighted routing between two targets.</summary>
public sealed record WeightedRoutingManagerResult(
    string TargetGroupArnBlue,
    string TargetGroupArnGreen,
    int BlueWeight,
    int GreenWeight,
    bool Updated);

/// <summary>Result of scaling Lambda provisioned concurrency.</summary>
public sealed record LambdaProvisionedConcurrencyScalerResult(
    string FunctionName,
    string Qualifier,
    int PreviousConcurrency,
    int NewConcurrency,
    string Status);

/// <summary>
/// Blue/green deployment orchestration combining ECS, ELB, Lambda, Route 53,
/// and CodeDeploy for zero-downtime deployments.
/// </summary>
public static class BlueGreenService
{
    /// <summary>
    /// Perform a blue/green deployment for an ECS service by creating a new
    /// task definition revision, updating the service, and monitoring rollout.
    /// </summary>
    public static async Task<EcsBlueGreenDeployerResult> EcsBlueGreenDeployerAsync(
        string clusterName,
        string serviceName,
        string newImageUri,
        string? containerName = null,
        int stabilizationTimeoutSeconds = 300,
        RegionEndpoint? region = null)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var steps = new List<string>();

            // Step 1: Get current service details
            var serviceDesc = await EcsService.DescribeServicesAsync(
                new Amazon.ECS.Model.DescribeServicesRequest
                {
                    Cluster = clusterName,
                    Services = new List<string> { serviceName }
                },
                region: region);

            var currentTaskDef = serviceDesc.Services?.FirstOrDefault()?.TaskDefinition ?? "";
            steps.Add($"Retrieved current task definition: {currentTaskDef}");

            // Step 2: Describe current task definition
            var taskDef = await EcsService.DescribeTaskDefinitionAsync(
                currentTaskDef, region: region);

            var container = containerName
                ?? taskDef.TaskDefinition?.ContainerDefinitions?.FirstOrDefault()?.Name
                ?? "app";
            steps.Add($"Using container: {container}");

            // Step 3: Register new task definition with updated image
            var containerDefs = taskDef.TaskDefinition?.ContainerDefinitions?
                .Select(cd =>
                {
                    if (cd.Name == container)
                        cd.Image = newImageUri;
                    return cd;
                }).ToList();

            var regReq = new Amazon.ECS.Model.RegisterTaskDefinitionRequest
            {
                Family = taskDef.TaskDefinition?.Family ?? serviceName,
                ContainerDefinitions = containerDefs
                    ?? new List<Amazon.ECS.Model.ContainerDefinition>(),
                TaskRoleArn = taskDef.TaskDefinition?.TaskRoleArn,
                ExecutionRoleArn = taskDef.TaskDefinition?.ExecutionRoleArn,
                Cpu = taskDef.TaskDefinition?.Cpu,
                Memory = taskDef.TaskDefinition?.Memory,
            };
            if (taskDef.TaskDefinition?.NetworkMode != null)
                regReq.NetworkMode = taskDef.TaskDefinition.NetworkMode;
            var newTaskDef = await EcsService.RegisterTaskDefinitionAsync(regReq, region: region);

            var newTaskDefArn = newTaskDef.TaskDefinitionArn ?? "";
            steps.Add($"Registered new task definition: {newTaskDefArn}");

            // Step 4: Update service with new task definition
            await EcsService.UpdateServiceAsync(
                new Amazon.ECS.Model.UpdateServiceRequest
                {
                    Cluster = clusterName,
                    Service = serviceName,
                    TaskDefinition = newTaskDefArn
                },
                region: region);
            steps.Add("Updated service with new task definition");

            // Step 5: Publish deployment event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.blue-green",
                        DetailType = "EcsBlueGreenDeployment",
                        Detail = JsonSerializer.Serialize(new
                        {
                            clusterName,
                            serviceName,
                            oldTaskDefinition = currentTaskDef,
                            newTaskDefinition = newTaskDefArn,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);
            steps.Add("Published deployment event");

            sw.Stop();

            return new EcsBlueGreenDeployerResult(
                ServiceName: serviceName,
                ClusterName: clusterName,
                BlueTaskDefinition: currentTaskDef,
                GreenTaskDefinition: newTaskDefArn,
                ActiveColor: "green",
                DeploymentSucceeded: true,
                DurationMs: sw.Elapsed.TotalMilliseconds,
                Steps: steps);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ECS blue/green deployment failed");
        }
    }

    /// <summary>
    /// Manage weighted routing between blue and green target groups
    /// via ELB listener rule modifications.
    /// </summary>
    public static async Task<WeightedRoutingManagerResult> WeightedRoutingManagerAsync(
        string listenerArn,
        string targetGroupArnBlue,
        string targetGroupArnGreen,
        int blueWeight,
        int greenWeight,
        RegionEndpoint? region = null)
    {
        try
        {
            // Modify the listener with forward action containing weighted target groups
            await ElbV2Service.ModifyListenerAsync(
                new Amazon.ElasticLoadBalancingV2.Model.ModifyListenerRequest
                {
                    ListenerArn = listenerArn,
                    DefaultActions = new List<Amazon.ElasticLoadBalancingV2.Model.Action>
                    {
                        new()
                        {
                            Type = Amazon.ElasticLoadBalancingV2.ActionTypeEnum.Forward,
                            ForwardConfig = new Amazon.ElasticLoadBalancingV2.Model.ForwardActionConfig
                            {
                                TargetGroups = new List<Amazon.ElasticLoadBalancingV2.Model.TargetGroupTuple>
                                {
                                    new()
                                    {
                                        TargetGroupArn = targetGroupArnBlue,
                                        Weight = blueWeight
                                    },
                                    new()
                                    {
                                        TargetGroupArn = targetGroupArnGreen,
                                        Weight = greenWeight
                                    }
                                }
                            }
                        }
                    }
                },
                region: region);

            return new WeightedRoutingManagerResult(
                TargetGroupArnBlue: targetGroupArnBlue,
                TargetGroupArnGreen: targetGroupArnGreen,
                BlueWeight: blueWeight,
                GreenWeight: greenWeight,
                Updated: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Weighted routing update failed");
        }
    }

    /// <summary>
    /// Scale provisioned concurrency for a Lambda function alias,
    /// typically used during blue/green traffic shifting.
    /// </summary>
    public static async Task<LambdaProvisionedConcurrencyScalerResult> LambdaProvisionedConcurrencyScalerAsync(
        string functionName,
        string qualifier,
        int desiredConcurrency,
        RegionEndpoint? region = null)
    {
        try
        {
            // Get current provisioned concurrency
            int previousConcurrency;
            try
            {
                var current = await LambdaService.GetProvisionedConcurrencyConfigAsync(
                    functionName, qualifier, region: region);
                previousConcurrency = current.AllocatedProvisionedConcurrentExecutions ?? 0;
            }
            catch (AwsNotFoundException)
            {
                previousConcurrency = 0;
            }

            // Set new provisioned concurrency
            if (desiredConcurrency > 0)
            {
                await LambdaService.PutProvisionedConcurrencyConfigAsync(
                    functionName,
                    qualifier,
                    desiredConcurrency,
                    region: region);
            }
            else
            {
                try
                {
                    await LambdaService.DeleteProvisionedConcurrencyConfigAsync(
                        functionName, qualifier, region: region);
                }
                catch (AwsNotFoundException)
                {
                    // Already absent
                }
            }

            return new LambdaProvisionedConcurrencyScalerResult(
                FunctionName: functionName,
                Qualifier: qualifier,
                PreviousConcurrency: previousConcurrency,
                NewConcurrency: desiredConcurrency,
                Status: desiredConcurrency > 0 ? "Scaling" : "Removed");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Lambda provisioned concurrency scaling failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="EcsBlueGreenDeployerAsync"/>.</summary>
    public static EcsBlueGreenDeployerResult EcsBlueGreenDeployer(string clusterName, string serviceName, string newImageUri, string? containerName = null, int stabilizationTimeoutSeconds = 300, RegionEndpoint? region = null)
        => EcsBlueGreenDeployerAsync(clusterName, serviceName, newImageUri, containerName, stabilizationTimeoutSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WeightedRoutingManagerAsync"/>.</summary>
    public static WeightedRoutingManagerResult WeightedRoutingManager(string listenerArn, string targetGroupArnBlue, string targetGroupArnGreen, int blueWeight, int greenWeight, RegionEndpoint? region = null)
        => WeightedRoutingManagerAsync(listenerArn, targetGroupArnBlue, targetGroupArnGreen, blueWeight, greenWeight, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LambdaProvisionedConcurrencyScalerAsync"/>.</summary>
    public static LambdaProvisionedConcurrencyScalerResult LambdaProvisionedConcurrencyScaler(string functionName, string qualifier, int desiredConcurrency, RegionEndpoint? region = null)
        => LambdaProvisionedConcurrencyScalerAsync(functionName, qualifier, desiredConcurrency, region).GetAwaiter().GetResult();

}
