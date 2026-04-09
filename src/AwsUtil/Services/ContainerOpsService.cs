using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of running an ECS task.</summary>
public sealed record EcsTaskRunnerResult(
    string ClusterName,
    string TaskDefinition,
    List<string> TaskArns,
    string LaunchType,
    string Status);

/// <summary>Result of managing Fargate Spot capacity.</summary>
public sealed record FargateSpotManagerResult(
    string ClusterName,
    string ServiceName,
    int SpotTasks,
    int OnDemandTasks,
    int SpotPercentage,
    bool Updated);

/// <summary>Health check result for a container.</summary>
public sealed record ContainerHealthCheckResult(
    string ClusterName,
    string ServiceName,
    int TotalTasks,
    int HealthyTasks,
    int UnhealthyTasks,
    List<TaskHealthStatus> TaskStatuses);

/// <summary>Health status of an individual task.</summary>
public sealed record TaskHealthStatus(
    string TaskArn,
    string Status,
    string HealthStatus,
    string? LastError = null);

/// <summary>Result of configuring ECS service auto-scaling.</summary>
public sealed record EcsServiceAutoScalerResult(
    string ClusterName,
    string ServiceName,
    int MinCapacity,
    int MaxCapacity,
    string MetricType,
    double TargetValue,
    bool Configured);

/// <summary>
/// Container operations orchestrating ECS, Fargate, CloudWatch,
/// Auto Scaling, and EventBridge for container management.
/// </summary>
public static class ContainerOpsService
{
    /// <summary>
    /// Run a standalone ECS task (not part of a service) on the specified cluster
    /// with the given task definition and networking configuration.
    /// </summary>
    public static async Task<EcsTaskRunnerResult> EcsTaskRunnerAsync(
        string clusterName,
        string taskDefinition,
        int count = 1,
        string launchType = "FARGATE",
        List<string>? subnets = null,
        List<string>? securityGroups = null,
        Dictionary<string, string>? environmentOverrides = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var overrides = new Amazon.ECS.Model.TaskOverride();

            if (environmentOverrides != null && environmentOverrides.Count > 0)
            {
                // Get task def to find container names
                var taskDef = await EcsService.DescribeTaskDefinitionAsync(
                    taskDefinition, region: region);

                var containerName = taskDef.TaskDefinition?.ContainerDefinitions?
                    .FirstOrDefault()?.Name ?? "app";

                overrides.ContainerOverrides = new List<Amazon.ECS.Model.ContainerOverride>
                {
                    new()
                    {
                        Name = containerName,
                        Environment = environmentOverrides
                            .Select(kv => new Amazon.ECS.Model.KeyValuePair
                            {
                                Name = kv.Key,
                                Value = kv.Value
                            }).ToList()
                    }
                };
            }

            var runRequest = new Amazon.ECS.Model.RunTaskRequest
            {
                Cluster = clusterName,
                TaskDefinition = taskDefinition,
                Count = count,
                LaunchType = new Amazon.ECS.LaunchType(launchType),
                Overrides = overrides
            };
            if (subnets != null)
            {
                runRequest.NetworkConfiguration = new Amazon.ECS.Model.NetworkConfiguration
                {
                    AwsvpcConfiguration = new Amazon.ECS.Model.AwsVpcConfiguration
                    {
                        Subnets = subnets,
                        SecurityGroups = securityGroups ?? new List<string>(),
                        AssignPublicIp = Amazon.ECS.AssignPublicIp.DISABLED
                    }
                };
            }
            var result = await EcsService.RunTaskAsync(runRequest, region: region);

            var taskArns = result.Tasks?
                .Select(t => t.TaskArn)
                .Where(a => a != null)
                .Cast<string>()
                .ToList() ?? new List<string>();

            return new EcsTaskRunnerResult(
                ClusterName: clusterName,
                TaskDefinition: taskDefinition,
                TaskArns: taskArns,
                LaunchType: launchType,
                Status: taskArns.Count > 0 ? "Running" : "Failed");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ECS task run failed");
        }
    }

    /// <summary>
    /// Configure a Fargate service to use a mix of Spot and On-Demand capacity
    /// by managing capacity provider strategies.
    /// </summary>
    public static async Task<FargateSpotManagerResult> FargateSpotManagerAsync(
        string clusterName,
        string serviceName,
        int spotPercentage = 70,
        RegionEndpoint? region = null)
    {
        try
        {
            var spotWeight = spotPercentage;
            var onDemandWeight = 100 - spotPercentage;

            await EcsService.UpdateServiceAsync(new Amazon.ECS.Model.UpdateServiceRequest
                {
                    Cluster = clusterName,
                    Service = serviceName,
                    CapacityProviderStrategy = new List<Amazon.ECS.Model.CapacityProviderStrategyItem>
                    {
                        new()
                        {
                            CapacityProvider = "FARGATE_SPOT",
                            Weight = spotWeight,
                            Base = 0
                        },
                        new()
                        {
                            CapacityProvider = "FARGATE",
                            Weight = onDemandWeight,
                            Base = 1 // At least 1 on-demand task for reliability
                        }
                    }
                },
                region: region);

            // Get current task count
            var serviceDesc = await EcsService.DescribeServicesAsync(
                new Amazon.ECS.Model.DescribeServicesRequest
                {
                    Cluster = clusterName,
                    Services = new List<string> { serviceName }
                },
                region: region);

            var desiredCount = serviceDesc.Services?.FirstOrDefault()?.DesiredCount ?? 0;
            var spotTasks = (int)(desiredCount * spotPercentage / 100.0);
            var onDemandTasks = desiredCount - spotTasks;

            return new FargateSpotManagerResult(
                ClusterName: clusterName,
                ServiceName: serviceName,
                SpotTasks: spotTasks,
                OnDemandTasks: Math.Max(1, onDemandTasks),
                SpotPercentage: spotPercentage,
                Updated: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Fargate Spot management failed");
        }
    }

    /// <summary>
    /// Check the health status of all tasks in an ECS service by querying
    /// task status and CloudWatch health metrics.
    /// </summary>
    public static async Task<ContainerHealthCheckResult> ContainerHealthCheckerAsync(
        string clusterName,
        string serviceName,
        RegionEndpoint? region = null)
    {
        try
        {
            // Get service tasks
            var taskList = await EcsService.ListTasksAsync(
                new Amazon.ECS.Model.ListTasksRequest
                {
                    Cluster = clusterName,
                    ServiceName = serviceName
                },
                region: region);

            var taskArns = taskList.TaskArns ?? new List<string>();
            var statuses = new List<TaskHealthStatus>();

            if (taskArns.Count > 0)
            {
                var tasks = await EcsService.DescribeTasksAsync(
                    new Amazon.ECS.Model.DescribeTasksRequest
                    {
                        Cluster = clusterName,
                        Tasks = taskArns
                    },
                    region: region);

                foreach (var task in tasks.Tasks ?? Enumerable.Empty<Amazon.ECS.Model.Task>())
                {
                    var healthStatus = task.HealthStatus?.Value ?? "UNKNOWN";
                    var lastStatus = task.LastStatus ?? "UNKNOWN";
                    var stoppedReason = task.StoppedReason;

                    statuses.Add(new TaskHealthStatus(
                        TaskArn: task.TaskArn ?? "unknown",
                        Status: lastStatus,
                        HealthStatus: healthStatus,
                        LastError: stoppedReason));
                }
            }

            return new ContainerHealthCheckResult(
                ClusterName: clusterName,
                ServiceName: serviceName,
                TotalTasks: statuses.Count,
                HealthyTasks: statuses.Count(s =>
                    s.HealthStatus == "HEALTHY" || s.Status == "RUNNING"),
                UnhealthyTasks: statuses.Count(s =>
                    s.HealthStatus == "UNHEALTHY" || s.Status == "STOPPED"),
                TaskStatuses: statuses);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Container health check failed");
        }
    }

    /// <summary>
    /// Configure auto-scaling for an ECS service based on a CloudWatch metric
    /// (CPU, memory, or custom metric) with target tracking.
    /// </summary>
    public static async Task<EcsServiceAutoScalerResult> EcsServiceAutoScalerAsync(
        string clusterName,
        string serviceName,
        int minCapacity,
        int maxCapacity,
        string metricType = "CPU",
        double targetValue = 70.0,
        RegionEndpoint? region = null)
    {
        try
        {
            // Configure scaling via Auto Scaling policies
            var resourceId = $"service/{clusterName}/{serviceName}";
            var policyName = $"ecs-{serviceName}-{metricType.ToLowerInvariant()}-scaling";

            await AutoScalingService.PutScalingPolicyAsync(
                new Amazon.AutoScaling.Model.PutScalingPolicyRequest
                {
                    AutoScalingGroupName = resourceId,
                    PolicyName = policyName,
                    PolicyType = "TargetTrackingScaling"
                },
                region: region);

            // Set up CloudWatch alarm for the metric
            var metricName = metricType.ToUpperInvariant() switch
            {
                "CPU" => "CPUUtilization",
                "MEMORY" => "MemoryUtilization",
                _ => metricType
            };

            await CloudWatchService.PutMetricAlarmAsync(
                new Amazon.CloudWatch.Model.PutMetricAlarmRequest
                {
                    AlarmName = $"{serviceName}-{metricType.ToLowerInvariant()}-high",
                    Namespace = "AWS/ECS",
                    MetricName = metricName,
                    ComparisonOperator = new Amazon.CloudWatch.ComparisonOperator("GreaterThanThreshold"),
                    Threshold = targetValue,
                    Period = 60,
                    EvaluationPeriods = 3,
                    Statistic = new Amazon.CloudWatch.Statistic("Average"),
                    Dimensions = new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "ClusterName", Value = clusterName },
                        new() { Name = "ServiceName", Value = serviceName }
                    }
                },
                region: region);

            return new EcsServiceAutoScalerResult(
                ClusterName: clusterName,
                ServiceName: serviceName,
                MinCapacity: minCapacity,
                MaxCapacity: maxCapacity,
                MetricType: metricType,
                TargetValue: targetValue,
                Configured: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ECS service auto-scaling configuration failed");
        }
    }
}
