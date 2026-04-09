using Amazon;
using Amazon.ECS;
using Amazon.ECS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for ECS operations.
/// </summary>
public sealed record CreateClusterResult(
    string? ClusterArn = null, string? ClusterName = null, string? Status = null);

public sealed record DeleteClusterResult(
    string? ClusterArn = null, string? ClusterName = null, string? Status = null);

public sealed record DescribeClustersResult(
    List<Cluster>? Clusters = null, List<Failure>? Failures = null);

public sealed record ListClustersResult(
    List<string>? ClusterArns = null, string? NextToken = null);

public sealed record RegisterTaskDefinitionResult(
    string? TaskDefinitionArn = null, string? Family = null, int? Revision = null);

public sealed record DeregisterTaskDefinitionResult(
    string? TaskDefinitionArn = null, string? Status = null);

public sealed record DescribeTaskDefinitionResult(TaskDefinition? TaskDefinition = null);

public sealed record ListTaskDefinitionsResult(
    List<string>? TaskDefinitionArns = null, string? NextToken = null);

public sealed record RunTaskResult(
    List<Amazon.ECS.Model.Task>? Tasks = null, List<Failure>? Failures = null);

public sealed record StopTaskResult(string? TaskArn = null, string? StoppedReason = null);

public sealed record DescribeTasksResult(
    List<Amazon.ECS.Model.Task>? Tasks = null, List<Failure>? Failures = null);

public sealed record ListTasksResult(
    List<string>? TaskArns = null, string? NextToken = null);

public sealed record CreateEcsServiceResult(
    string? ServiceArn = null, string? ServiceName = null, string? Status = null);

public sealed record DeleteEcsServiceResult(
    string? ServiceArn = null, string? ServiceName = null, string? Status = null);

public sealed record UpdateEcsServiceResult(
    string? ServiceArn = null, string? ServiceName = null, string? Status = null);

public sealed record DescribeEcsServicesResult(
    List<Service>? Services = null, List<Failure>? Failures = null);

public sealed record ListEcsServicesResult(
    List<string>? ServiceArns = null, string? NextToken = null);

public sealed record UpdateClusterSettingsResult(
    string? ClusterArn = null, string? ClusterName = null);

public sealed record PutClusterCapacityProvidersResult(
    string? ClusterArn = null, string? ClusterName = null);

public sealed record DescribeContainerInstancesResult(
    List<ContainerInstance>? ContainerInstances = null, List<Failure>? Failures = null);

public sealed record ListContainerInstancesResult(
    List<string>? ContainerInstanceArns = null, string? NextToken = null);

public sealed record EcsTagResourceResult(bool Success = true);
public sealed record EcsUntagResourceResult(bool Success = true);

public sealed record EcsListTagsForResourceResult(List<Tag>? Tags = null);

public sealed record ExecuteCommandResult(
    string? ClusterArn = null, string? TaskArn = null, string? ContainerName = null,
    bool? Interactive = null, Amazon.ECS.Model.Session? Session = null);

public sealed record UpdateServicePrimaryTaskSetResult(
    string? TaskSetArn = null, string? Status = null);

public sealed record CreateTaskSetResult(
    string? TaskSetArn = null, string? Status = null, string? Id = null);

public sealed record DeleteTaskSetResult(
    string? TaskSetArn = null, string? Status = null);

public sealed record DescribeTaskSetsResult(
    List<TaskSet>? TaskSets = null, List<Failure>? Failures = null);

/// <summary>
/// Utility helpers for Amazon Elastic Container Service (ECS).
/// </summary>
public static class EcsService
{
    private static AmazonECSClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonECSClient>(region);

    /// <summary>
    /// Create a new ECS cluster.
    /// </summary>
    public static async Task<CreateClusterResult> CreateClusterAsync(
        CreateClusterRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterAsync(request);
            return new CreateClusterResult(
                ClusterArn: resp.Cluster.ClusterArn,
                ClusterName: resp.Cluster.ClusterName,
                Status: resp.Cluster.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create ECS cluster");
        }
    }

    /// <summary>
    /// Delete an ECS cluster.
    /// </summary>
    public static async Task<DeleteClusterResult> DeleteClusterAsync(
        string cluster, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteClusterAsync(new DeleteClusterRequest
            {
                Cluster = cluster
            });
            return new DeleteClusterResult(
                ClusterArn: resp.Cluster.ClusterArn,
                ClusterName: resp.Cluster.ClusterName,
                Status: resp.Cluster.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete ECS cluster '{cluster}'");
        }
    }

    /// <summary>
    /// Describe one or more ECS clusters.
    /// </summary>
    public static async Task<DescribeClustersResult> DescribeClustersAsync(
        DescribeClustersRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClustersAsync(request);
            return new DescribeClustersResult(
                Clusters: resp.Clusters,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe ECS clusters");
        }
    }

    /// <summary>
    /// List ECS clusters.
    /// </summary>
    public static async Task<ListClustersResult> ListClustersAsync(
        string? nextToken = null, int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListClustersRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListClustersAsync(request);
            return new ListClustersResult(
                ClusterArns: resp.ClusterArns,
                NextToken: resp.NextToken);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list ECS clusters");
        }
    }

    /// <summary>
    /// Register a new task definition.
    /// </summary>
    public static async Task<RegisterTaskDefinitionResult> RegisterTaskDefinitionAsync(
        RegisterTaskDefinitionRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RegisterTaskDefinitionAsync(request);
            return new RegisterTaskDefinitionResult(
                TaskDefinitionArn: resp.TaskDefinition.TaskDefinitionArn,
                Family: resp.TaskDefinition.Family,
                Revision: resp.TaskDefinition.Revision);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to register task definition");
        }
    }

    /// <summary>
    /// Deregister a task definition.
    /// </summary>
    public static async Task<DeregisterTaskDefinitionResult> DeregisterTaskDefinitionAsync(
        string taskDefinition, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeregisterTaskDefinitionAsync(
                new DeregisterTaskDefinitionRequest { TaskDefinition = taskDefinition });
            return new DeregisterTaskDefinitionResult(
                TaskDefinitionArn: resp.TaskDefinition.TaskDefinitionArn,
                Status: resp.TaskDefinition.Status?.Value);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister task definition '{taskDefinition}'");
        }
    }

    /// <summary>
    /// Describe a task definition.
    /// </summary>
    public static async Task<DescribeTaskDefinitionResult> DescribeTaskDefinitionAsync(
        string taskDefinition, List<string>? include = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTaskDefinitionRequest
        {
            TaskDefinition = taskDefinition
        };
        if (include != null)
            request.Include = include;

        try
        {
            var resp = await client.DescribeTaskDefinitionAsync(request);
            return new DescribeTaskDefinitionResult(TaskDefinition: resp.TaskDefinition);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe task definition '{taskDefinition}'");
        }
    }

    /// <summary>
    /// List task definitions.
    /// </summary>
    public static async Task<ListTaskDefinitionsResult> ListTaskDefinitionsAsync(
        string? familyPrefix = null, string? status = null,
        string? nextToken = null, int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTaskDefinitionsRequest();
        if (familyPrefix != null) request.FamilyPrefix = familyPrefix;
        if (status != null) request.Status = new TaskDefinitionStatus(status);
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTaskDefinitionsAsync(request);
            return new ListTaskDefinitionsResult(
                TaskDefinitionArns: resp.TaskDefinitionArns,
                NextToken: resp.NextToken);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list task definitions");
        }
    }

    /// <summary>
    /// Run a task on an ECS cluster.
    /// </summary>
    public static async Task<RunTaskResult> RunTaskAsync(
        RunTaskRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RunTaskAsync(request);
            return new RunTaskResult(
                Tasks: resp.Tasks,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to run ECS task");
        }
    }

    /// <summary>
    /// Stop a running ECS task.
    /// </summary>
    public static async Task<StopTaskResult> StopTaskAsync(
        string cluster, string task, string? reason = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopTaskRequest
        {
            Cluster = cluster,
            Task = task
        };
        if (reason != null) request.Reason = reason;

        try
        {
            var resp = await client.StopTaskAsync(request);
            return new StopTaskResult(
                TaskArn: resp.Task.TaskArn,
                StoppedReason: resp.Task.StoppedReason);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop ECS task '{task}'");
        }
    }

    /// <summary>
    /// Describe one or more ECS tasks.
    /// </summary>
    public static async Task<DescribeTasksResult> DescribeTasksAsync(
        DescribeTasksRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTasksAsync(request);
            return new DescribeTasksResult(
                Tasks: resp.Tasks,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe ECS tasks");
        }
    }

    /// <summary>
    /// List tasks in a cluster.
    /// </summary>
    public static async Task<ListTasksResult> ListTasksAsync(
        ListTasksRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTasksAsync(request);
            return new ListTasksResult(
                TaskArns: resp.TaskArns,
                NextToken: resp.NextToken);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list ECS tasks");
        }
    }

    /// <summary>
    /// Create a new ECS service.
    /// </summary>
    public static async Task<CreateEcsServiceResult> CreateServiceAsync(
        CreateServiceRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServiceAsync(request);
            return new CreateEcsServiceResult(
                ServiceArn: resp.Service.ServiceArn,
                ServiceName: resp.Service.ServiceName,
                Status: resp.Service.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create ECS service");
        }
    }

    /// <summary>
    /// Delete an ECS service.
    /// </summary>
    public static async Task<DeleteEcsServiceResult> DeleteServiceAsync(
        string cluster, string service, bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteServiceRequest
        {
            Cluster = cluster,
            Service = service
        };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.DeleteServiceAsync(request);
            return new DeleteEcsServiceResult(
                ServiceArn: resp.Service.ServiceArn,
                ServiceName: resp.Service.ServiceName,
                Status: resp.Service.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete ECS service '{service}'");
        }
    }

    /// <summary>
    /// Update an ECS service.
    /// </summary>
    public static async Task<UpdateEcsServiceResult> UpdateServiceAsync(
        UpdateServiceRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServiceAsync(request);
            return new UpdateEcsServiceResult(
                ServiceArn: resp.Service.ServiceArn,
                ServiceName: resp.Service.ServiceName,
                Status: resp.Service.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update ECS service");
        }
    }

    /// <summary>
    /// Describe one or more ECS services.
    /// </summary>
    public static async Task<DescribeEcsServicesResult> DescribeServicesAsync(
        DescribeServicesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeServicesAsync(request);
            return new DescribeEcsServicesResult(
                Services: resp.Services,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe ECS services");
        }
    }

    /// <summary>
    /// List ECS services in a cluster.
    /// </summary>
    public static async Task<ListEcsServicesResult> ListServicesAsync(
        ListServicesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListServicesAsync(request);
            return new ListEcsServicesResult(
                ServiceArns: resp.ServiceArns,
                NextToken: resp.NextToken);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list ECS services");
        }
    }

    /// <summary>
    /// Update cluster settings.
    /// </summary>
    public static async Task<UpdateClusterSettingsResult> UpdateClusterSettingsAsync(
        string cluster, List<ClusterSetting> settings,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateClusterSettingsAsync(
                new UpdateClusterSettingsRequest
                {
                    Cluster = cluster,
                    Settings = settings
                });
            return new UpdateClusterSettingsResult(
                ClusterArn: resp.Cluster.ClusterArn,
                ClusterName: resp.Cluster.ClusterName);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update cluster settings for '{cluster}'");
        }
    }

    /// <summary>
    /// Put cluster capacity providers.
    /// </summary>
    public static async Task<PutClusterCapacityProvidersResult>
        PutClusterCapacityProvidersAsync(
            PutClusterCapacityProvidersRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutClusterCapacityProvidersAsync(request);
            return new PutClusterCapacityProvidersResult(
                ClusterArn: resp.Cluster.ClusterArn,
                ClusterName: resp.Cluster.ClusterName);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put cluster capacity providers");
        }
    }

    /// <summary>
    /// Describe container instances.
    /// </summary>
    public static async Task<DescribeContainerInstancesResult>
        DescribeContainerInstancesAsync(
            DescribeContainerInstancesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeContainerInstancesAsync(request);
            return new DescribeContainerInstancesResult(
                ContainerInstances: resp.ContainerInstances,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe container instances");
        }
    }

    /// <summary>
    /// List container instances in a cluster.
    /// </summary>
    public static async Task<ListContainerInstancesResult>
        ListContainerInstancesAsync(
            ListContainerInstancesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListContainerInstancesAsync(request);
            return new ListContainerInstancesResult(
                ContainerInstanceArns: resp.ContainerInstanceArns,
                NextToken: resp.NextToken);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list container instances");
        }
    }

    /// <summary>
    /// Tag an ECS resource.
    /// </summary>
    public static async Task<EcsTagResourceResult> TagResourceAsync(
        string resourceArn, List<Tag> tags,
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
            return new EcsTagResourceResult();
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag ECS resource");
        }
    }

    /// <summary>
    /// Remove tags from an ECS resource.
    /// </summary>
    public static async Task<EcsUntagResourceResult> UntagResourceAsync(
        string resourceArn, List<string> tagKeys,
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
            return new EcsUntagResourceResult();
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag ECS resource");
        }
    }

    /// <summary>
    /// List tags for an ECS resource.
    /// </summary>
    public static async Task<EcsListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new EcsListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for ECS resource");
        }
    }

    /// <summary>
    /// Execute a command in a running container.
    /// </summary>
    public static async Task<ExecuteCommandResult> ExecuteCommandAsync(
        ExecuteCommandRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ExecuteCommandAsync(request);
            return new ExecuteCommandResult(
                ClusterArn: resp.ClusterArn,
                TaskArn: resp.TaskArn,
                ContainerName: resp.ContainerName,
                Interactive: resp.Interactive,
                Session: resp.Session);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to execute command");
        }
    }

    /// <summary>
    /// Update the primary task set for a service.
    /// </summary>
    public static async Task<UpdateServicePrimaryTaskSetResult>
        UpdateServicePrimaryTaskSetAsync(
            string cluster, string service, string primaryTaskSet,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServicePrimaryTaskSetAsync(
                new UpdateServicePrimaryTaskSetRequest
                {
                    Cluster = cluster,
                    Service = service,
                    PrimaryTaskSet = primaryTaskSet
                });
            return new UpdateServicePrimaryTaskSetResult(
                TaskSetArn: resp.TaskSet.TaskSetArn,
                Status: resp.TaskSet.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update service primary task set");
        }
    }

    /// <summary>
    /// Create a task set within a service.
    /// </summary>
    public static async Task<CreateTaskSetResult> CreateTaskSetAsync(
        CreateTaskSetRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTaskSetAsync(request);
            return new CreateTaskSetResult(
                TaskSetArn: resp.TaskSet.TaskSetArn,
                Status: resp.TaskSet.Status,
                Id: resp.TaskSet.Id);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create task set");
        }
    }

    /// <summary>
    /// Delete a task set.
    /// </summary>
    public static async Task<DeleteTaskSetResult> DeleteTaskSetAsync(
        string cluster, string service, string taskSet,
        bool? force = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteTaskSetRequest
        {
            Cluster = cluster,
            Service = service,
            TaskSet = taskSet
        };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.DeleteTaskSetAsync(request);
            return new DeleteTaskSetResult(
                TaskSetArn: resp.TaskSet.TaskSetArn,
                Status: resp.TaskSet.Status);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete task set");
        }
    }

    /// <summary>
    /// Describe task sets in a service.
    /// </summary>
    public static async Task<DescribeTaskSetsResult> DescribeTaskSetsAsync(
        DescribeTaskSetsRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTaskSetsAsync(request);
            return new DescribeTaskSetsResult(
                TaskSets: resp.TaskSets,
                Failures: resp.Failures);
        }
        catch (AmazonECSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe task sets");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateClusterAsync"/>.</summary>
    public static CreateClusterResult CreateCluster(CreateClusterRequest request, RegionEndpoint? region = null)
        => CreateClusterAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteClusterAsync"/>.</summary>
    public static DeleteClusterResult DeleteCluster(string cluster, RegionEndpoint? region = null)
        => DeleteClusterAsync(cluster, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeClustersAsync"/>.</summary>
    public static DescribeClustersResult DescribeClusters(DescribeClustersRequest request, RegionEndpoint? region = null)
        => DescribeClustersAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListClustersAsync"/>.</summary>
    public static ListClustersResult ListClusters(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListClustersAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterTaskDefinitionAsync"/>.</summary>
    public static RegisterTaskDefinitionResult RegisterTaskDefinition(RegisterTaskDefinitionRequest request, RegionEndpoint? region = null)
        => RegisterTaskDefinitionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterTaskDefinitionAsync"/>.</summary>
    public static DeregisterTaskDefinitionResult DeregisterTaskDefinition(string taskDefinition, RegionEndpoint? region = null)
        => DeregisterTaskDefinitionAsync(taskDefinition, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTaskDefinitionAsync"/>.</summary>
    public static DescribeTaskDefinitionResult DescribeTaskDefinition(string taskDefinition, List<string>? include = null, RegionEndpoint? region = null)
        => DescribeTaskDefinitionAsync(taskDefinition, include, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTaskDefinitionsAsync"/>.</summary>
    public static ListTaskDefinitionsResult ListTaskDefinitions(string? familyPrefix = null, string? status = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTaskDefinitionsAsync(familyPrefix, status, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RunTaskAsync"/>.</summary>
    public static RunTaskResult RunTask(RunTaskRequest request, RegionEndpoint? region = null)
        => RunTaskAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopTaskAsync"/>.</summary>
    public static StopTaskResult StopTask(string cluster, string task, string? reason = null, RegionEndpoint? region = null)
        => StopTaskAsync(cluster, task, reason, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTasksAsync"/>.</summary>
    public static DescribeTasksResult DescribeTasks(DescribeTasksRequest request, RegionEndpoint? region = null)
        => DescribeTasksAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTasksAsync"/>.</summary>
    public static ListTasksResult ListTasks(ListTasksRequest request, RegionEndpoint? region = null)
        => ListTasksAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateServiceAsync"/>.</summary>
    public static CreateEcsServiceResult CreateService(CreateServiceRequest request, RegionEndpoint? region = null)
        => CreateServiceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceAsync"/>.</summary>
    public static DeleteEcsServiceResult DeleteService(string cluster, string service, bool? force = null, RegionEndpoint? region = null)
        => DeleteServiceAsync(cluster, service, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServiceAsync"/>.</summary>
    public static UpdateEcsServiceResult UpdateService(UpdateServiceRequest request, RegionEndpoint? region = null)
        => UpdateServiceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeServicesAsync"/>.</summary>
    public static DescribeEcsServicesResult DescribeServices(DescribeServicesRequest request, RegionEndpoint? region = null)
        => DescribeServicesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServicesAsync"/>.</summary>
    public static ListEcsServicesResult ListServices(ListServicesRequest request, RegionEndpoint? region = null)
        => ListServicesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateClusterSettingsAsync"/>.</summary>
    public static UpdateClusterSettingsResult UpdateClusterSettings(string cluster, List<ClusterSetting> settings, RegionEndpoint? region = null)
        => UpdateClusterSettingsAsync(cluster, settings, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutClusterCapacityProvidersAsync"/>.</summary>
    public static PutClusterCapacityProvidersResult PutClusterCapacityProviders(PutClusterCapacityProvidersRequest request, RegionEndpoint? region = null)
        => PutClusterCapacityProvidersAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeContainerInstancesAsync"/>.</summary>
    public static DescribeContainerInstancesResult DescribeContainerInstances(DescribeContainerInstancesRequest request, RegionEndpoint? region = null)
        => DescribeContainerInstancesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListContainerInstancesAsync"/>.</summary>
    public static ListContainerInstancesResult ListContainerInstances(ListContainerInstancesRequest request, RegionEndpoint? region = null)
        => ListContainerInstancesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static EcsTagResourceResult TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static EcsUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static EcsListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExecuteCommandAsync"/>.</summary>
    public static ExecuteCommandResult ExecuteCommand(ExecuteCommandRequest request, RegionEndpoint? region = null)
        => ExecuteCommandAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServicePrimaryTaskSetAsync"/>.</summary>
    public static UpdateServicePrimaryTaskSetResult UpdateServicePrimaryTaskSet(string cluster, string service, string primaryTaskSet, RegionEndpoint? region = null)
        => UpdateServicePrimaryTaskSetAsync(cluster, service, primaryTaskSet, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTaskSetAsync"/>.</summary>
    public static CreateTaskSetResult CreateTaskSet(CreateTaskSetRequest request, RegionEndpoint? region = null)
        => CreateTaskSetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTaskSetAsync"/>.</summary>
    public static DeleteTaskSetResult DeleteTaskSet(string cluster, string service, string taskSet, bool? force = null, RegionEndpoint? region = null)
        => DeleteTaskSetAsync(cluster, service, taskSet, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTaskSetsAsync"/>.</summary>
    public static DescribeTaskSetsResult DescribeTaskSets(DescribeTaskSetsRequest request, RegionEndpoint? region = null)
        => DescribeTaskSetsAsync(request, region).GetAwaiter().GetResult();

}
