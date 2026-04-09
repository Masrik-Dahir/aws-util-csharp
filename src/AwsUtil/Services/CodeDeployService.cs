using Amazon;
using Amazon.CodeDeploy;
using Amazon.CodeDeploy.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CdCreateApplicationResult(
    string? ApplicationId = null);

public sealed record CdDeleteApplicationResult(bool Success = true);

public sealed record CdGetApplicationResult(
    ApplicationInfo? Application = null);

public sealed record CdListApplicationsResult(
    List<string>? Applications = null,
    string? NextToken = null);

public sealed record CdCreateDeploymentGroupResult(
    string? DeploymentGroupId = null);

public sealed record CdDeleteDeploymentGroupResult(bool Success = true);

public sealed record CdGetDeploymentGroupResult(
    DeploymentGroupInfo? DeploymentGroupInfo = null);

public sealed record CdListDeploymentGroupsResult(
    string? ApplicationName = null,
    List<string>? DeploymentGroups = null,
    string? NextToken = null);

public sealed record CdUpdateDeploymentGroupResult(
    List<AutoScalingGroup>? HooksNotCleanedUp = null);

public sealed record CdCreateDeploymentResult(
    string? DeploymentId = null);

public sealed record CdStopDeploymentResult(
    string? Status = null,
    string? StatusMessage = null);

public sealed record CdGetDeploymentResult(
    DeploymentInfo? DeploymentInfo = null);

public sealed record CdListDeploymentsResult(
    List<string>? Deployments = null,
    string? NextToken = null);

public sealed record CdGetDeploymentInstanceResult(
    InstanceSummary? InstanceSummary = null);

public sealed record CdListDeploymentInstancesResult(
    List<string>? InstancesList = null,
    string? NextToken = null);

public sealed record CdContinueDeploymentResult(bool Success = true);

public sealed record CdCreateDeploymentConfigResult(
    string? DeploymentConfigId = null);

public sealed record CdDeleteDeploymentConfigResult(bool Success = true);

public sealed record CdGetDeploymentConfigResult(
    DeploymentConfigInfo? DeploymentConfigInfo = null);

public sealed record CdListDeploymentConfigsResult(
    List<string>? DeploymentConfigsList = null,
    string? NextToken = null);

public sealed record CdRegisterOnPremisesInstanceResult(bool Success = true);

public sealed record CdDeregisterOnPremisesInstanceResult(bool Success = true);

public sealed record CdListOnPremisesInstancesResult(
    List<string>? InstanceNames = null,
    string? NextToken = null);

public sealed record CdAddTagsToOnPremisesInstancesResult(
    bool Success = true);

public sealed record CdRemoveTagsFromOnPremisesInstancesResult(
    bool Success = true);

public sealed record CdBatchGetApplicationsResult(
    List<ApplicationInfo>? ApplicationsInfo = null);

public sealed record CdBatchGetDeploymentGroupsResult(
    List<DeploymentGroupInfo>? DeploymentGroupsInfo = null,
    string? ErrorMessage = null);

public sealed record CdBatchGetDeploymentsResult(
    List<DeploymentInfo>? DeploymentsInfo = null);

public sealed record CdPutLifecycleEventHookExecutionStatusResult(
    string? LifecycleEventHookExecutionId = null);

public sealed record CdTagResourceResult(bool Success = true);
public sealed record CdUntagResourceResult(bool Success = true);

public sealed record CdListTagsForResourceResult(
    List<Amazon.CodeDeploy.Model.Tag>? Tags = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodeDeploy.
/// </summary>
public static class CodeDeployService
{
    private static AmazonCodeDeployClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodeDeployClient>(region);

    /// <summary>
    /// Create a CodeDeploy application.
    /// </summary>
    public static async Task<CdCreateApplicationResult>
        CreateApplicationAsync(
            string applicationName,
            string? computePlatform = null,
            List<Amazon.CodeDeploy.Model.Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (computePlatform != null)
            request.ComputePlatform = new ComputePlatform(computePlatform);
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateApplicationAsync(request);
            return new CdCreateApplicationResult(
                ApplicationId: resp.ApplicationId);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeDeploy application");
        }
    }

    /// <summary>
    /// Delete a CodeDeploy application.
    /// </summary>
    public static async Task<CdDeleteApplicationResult>
        DeleteApplicationAsync(
            string applicationName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteApplicationAsync(
                new DeleteApplicationRequest
                {
                    ApplicationName = applicationName
                });
            return new CdDeleteApplicationResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeDeploy application '{applicationName}'");
        }
    }

    /// <summary>
    /// Get information about a CodeDeploy application.
    /// </summary>
    public static async Task<CdGetApplicationResult> GetApplicationAsync(
        string applicationName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetApplicationAsync(
                new GetApplicationRequest
                {
                    ApplicationName = applicationName
                });
            return new CdGetApplicationResult(Application: resp.Application);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get CodeDeploy application '{applicationName}'");
        }
    }

    /// <summary>
    /// List CodeDeploy applications.
    /// </summary>
    public static async Task<CdListApplicationsResult>
        ListApplicationsAsync(
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListApplicationsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListApplicationsAsync(request);
            return new CdListApplicationsResult(
                Applications: resp.Applications,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list CodeDeploy applications");
        }
    }

    /// <summary>
    /// Create a deployment group.
    /// </summary>
    public static async Task<CdCreateDeploymentGroupResult>
        CreateDeploymentGroupAsync(
            CreateDeploymentGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDeploymentGroupAsync(request);
            return new CdCreateDeploymentGroupResult(
                DeploymentGroupId: resp.DeploymentGroupId);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create deployment group");
        }
    }

    /// <summary>
    /// Delete a deployment group.
    /// </summary>
    public static async Task<CdDeleteDeploymentGroupResult>
        DeleteDeploymentGroupAsync(
            string applicationName,
            string deploymentGroupName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDeploymentGroupAsync(
                new DeleteDeploymentGroupRequest
                {
                    ApplicationName = applicationName,
                    DeploymentGroupName = deploymentGroupName
                });
            return new CdDeleteDeploymentGroupResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete deployment group '{deploymentGroupName}'");
        }
    }

    /// <summary>
    /// Get information about a deployment group.
    /// </summary>
    public static async Task<CdGetDeploymentGroupResult>
        GetDeploymentGroupAsync(
            string applicationName,
            string deploymentGroupName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeploymentGroupAsync(
                new GetDeploymentGroupRequest
                {
                    ApplicationName = applicationName,
                    DeploymentGroupName = deploymentGroupName
                });
            return new CdGetDeploymentGroupResult(
                DeploymentGroupInfo: resp.DeploymentGroupInfo);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get deployment group '{deploymentGroupName}'");
        }
    }

    /// <summary>
    /// List deployment groups for an application.
    /// </summary>
    public static async Task<CdListDeploymentGroupsResult>
        ListDeploymentGroupsAsync(
            string applicationName,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeploymentGroupsRequest
        {
            ApplicationName = applicationName
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDeploymentGroupsAsync(request);
            return new CdListDeploymentGroupsResult(
                ApplicationName: resp.ApplicationName,
                DeploymentGroups: resp.DeploymentGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list deployment groups");
        }
    }

    /// <summary>
    /// Update a deployment group.
    /// </summary>
    public static async Task<CdUpdateDeploymentGroupResult>
        UpdateDeploymentGroupAsync(
            UpdateDeploymentGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDeploymentGroupAsync(request);
            return new CdUpdateDeploymentGroupResult(
                HooksNotCleanedUp: resp.HooksNotCleanedUp);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update deployment group");
        }
    }

    /// <summary>
    /// Create a deployment.
    /// </summary>
    public static async Task<CdCreateDeploymentResult>
        CreateDeploymentAsync(
            CreateDeploymentRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDeploymentAsync(request);
            return new CdCreateDeploymentResult(
                DeploymentId: resp.DeploymentId);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create deployment");
        }
    }

    /// <summary>
    /// Stop a deployment.
    /// </summary>
    public static async Task<CdStopDeploymentResult> StopDeploymentAsync(
        string deploymentId,
        bool? autoRollbackEnabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopDeploymentRequest
        {
            DeploymentId = deploymentId
        };
        if (autoRollbackEnabled.HasValue)
            request.AutoRollbackEnabled = autoRollbackEnabled.Value;

        try
        {
            var resp = await client.StopDeploymentAsync(request);
            return new CdStopDeploymentResult(
                Status: resp.Status?.Value,
                StatusMessage: resp.StatusMessage);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop deployment '{deploymentId}'");
        }
    }

    /// <summary>
    /// Get information about a deployment.
    /// </summary>
    public static async Task<CdGetDeploymentResult> GetDeploymentAsync(
        string deploymentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeploymentAsync(
                new GetDeploymentRequest
                {
                    DeploymentId = deploymentId
                });
            return new CdGetDeploymentResult(
                DeploymentInfo: resp.DeploymentInfo);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get deployment '{deploymentId}'");
        }
    }

    /// <summary>
    /// List deployments.
    /// </summary>
    public static async Task<CdListDeploymentsResult>
        ListDeploymentsAsync(
            ListDeploymentsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListDeploymentsAsync(request);
            return new CdListDeploymentsResult(
                Deployments: resp.Deployments,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list deployments");
        }
    }

    /// <summary>
    /// Get information about a deployment instance.
    /// </summary>
    public static async Task<CdGetDeploymentInstanceResult>
        GetDeploymentInstanceAsync(
            string deploymentId,
            string instanceId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeploymentInstanceAsync(
                new GetDeploymentInstanceRequest
                {
                    DeploymentId = deploymentId,
                    InstanceId = instanceId
                });
            return new CdGetDeploymentInstanceResult(
                InstanceSummary: resp.InstanceSummary);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get deployment instance '{instanceId}'");
        }
    }

    /// <summary>
    /// List deployment instances.
    /// </summary>
    public static async Task<CdListDeploymentInstancesResult>
        ListDeploymentInstancesAsync(
            ListDeploymentInstancesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListDeploymentInstancesAsync(request);
            return new CdListDeploymentInstancesResult(
                InstancesList: resp.InstancesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list deployment instances");
        }
    }

    /// <summary>
    /// Continue a deployment with a wait period.
    /// </summary>
    public static async Task<CdContinueDeploymentResult>
        ContinueDeploymentAsync(
            string deploymentId,
            string? deploymentWaitType = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ContinueDeploymentRequest
        {
            DeploymentId = deploymentId
        };
        if (deploymentWaitType != null)
            request.DeploymentWaitType =
                new DeploymentWaitType(deploymentWaitType);

        try
        {
            await client.ContinueDeploymentAsync(request);
            return new CdContinueDeploymentResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to continue deployment '{deploymentId}'");
        }
    }

    /// <summary>
    /// Create a deployment configuration.
    /// </summary>
    public static async Task<CdCreateDeploymentConfigResult>
        CreateDeploymentConfigAsync(
            CreateDeploymentConfigRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDeploymentConfigAsync(request);
            return new CdCreateDeploymentConfigResult(
                DeploymentConfigId: resp.DeploymentConfigId);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create deployment config");
        }
    }

    /// <summary>
    /// Delete a deployment configuration.
    /// </summary>
    public static async Task<CdDeleteDeploymentConfigResult>
        DeleteDeploymentConfigAsync(
            string deploymentConfigName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDeploymentConfigAsync(
                new DeleteDeploymentConfigRequest
                {
                    DeploymentConfigName = deploymentConfigName
                });
            return new CdDeleteDeploymentConfigResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete deployment config '{deploymentConfigName}'");
        }
    }

    /// <summary>
    /// Get information about a deployment configuration.
    /// </summary>
    public static async Task<CdGetDeploymentConfigResult>
        GetDeploymentConfigAsync(
            string deploymentConfigName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeploymentConfigAsync(
                new GetDeploymentConfigRequest
                {
                    DeploymentConfigName = deploymentConfigName
                });
            return new CdGetDeploymentConfigResult(
                DeploymentConfigInfo: resp.DeploymentConfigInfo);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get deployment config '{deploymentConfigName}'");
        }
    }

    /// <summary>
    /// List deployment configurations.
    /// </summary>
    public static async Task<CdListDeploymentConfigsResult>
        ListDeploymentConfigsAsync(
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeploymentConfigsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDeploymentConfigsAsync(request);
            return new CdListDeploymentConfigsResult(
                DeploymentConfigsList: resp.DeploymentConfigsList,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list deployment configs");
        }
    }

    /// <summary>
    /// Register an on-premises instance.
    /// </summary>
    public static async Task<CdRegisterOnPremisesInstanceResult>
        RegisterOnPremisesInstanceAsync(
            RegisterOnPremisesInstanceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RegisterOnPremisesInstanceAsync(request);
            return new CdRegisterOnPremisesInstanceResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to register on-premises instance");
        }
    }

    /// <summary>
    /// Deregister an on-premises instance.
    /// </summary>
    public static async Task<CdDeregisterOnPremisesInstanceResult>
        DeregisterOnPremisesInstanceAsync(
            string instanceName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeregisterOnPremisesInstanceAsync(
                new DeregisterOnPremisesInstanceRequest
                {
                    InstanceName = instanceName
                });
            return new CdDeregisterOnPremisesInstanceResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deregister on-premises instance '{instanceName}'");
        }
    }

    /// <summary>
    /// List on-premises instances.
    /// </summary>
    public static async Task<CdListOnPremisesInstancesResult>
        ListOnPremisesInstancesAsync(
            ListOnPremisesInstancesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListOnPremisesInstancesAsync(request);
            return new CdListOnPremisesInstancesResult(
                InstanceNames: resp.InstanceNames,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list on-premises instances");
        }
    }

    /// <summary>
    /// Add tags to on-premises instances.
    /// </summary>
    public static async Task<CdAddTagsToOnPremisesInstancesResult>
        AddTagsToOnPremisesInstancesAsync(
            List<Amazon.CodeDeploy.Model.Tag> tags,
            List<string> instanceNames,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToOnPremisesInstancesAsync(
                new AddTagsToOnPremisesInstancesRequest
                {
                    Tags = tags,
                    InstanceNames = instanceNames
                });
            return new CdAddTagsToOnPremisesInstancesResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to add tags to on-premises instances");
        }
    }

    /// <summary>
    /// Remove tags from on-premises instances.
    /// </summary>
    public static async Task<CdRemoveTagsFromOnPremisesInstancesResult>
        RemoveTagsFromOnPremisesInstancesAsync(
            List<Amazon.CodeDeploy.Model.Tag> tags,
            List<string> instanceNames,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromOnPremisesInstancesAsync(
                new RemoveTagsFromOnPremisesInstancesRequest
                {
                    Tags = tags,
                    InstanceNames = instanceNames
                });
            return new CdRemoveTagsFromOnPremisesInstancesResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to remove tags from on-premises instances");
        }
    }

    /// <summary>
    /// Get information about multiple applications.
    /// </summary>
    public static async Task<CdBatchGetApplicationsResult>
        BatchGetApplicationsAsync(
            List<string> applicationNames,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetApplicationsAsync(
                new BatchGetApplicationsRequest
                {
                    ApplicationNames = applicationNames
                });
            return new CdBatchGetApplicationsResult(
                ApplicationsInfo: resp.ApplicationsInfo);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get applications");
        }
    }

    /// <summary>
    /// Get information about multiple deployment groups.
    /// </summary>
    public static async Task<CdBatchGetDeploymentGroupsResult>
        BatchGetDeploymentGroupsAsync(
            string applicationName,
            List<string> deploymentGroupNames,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetDeploymentGroupsAsync(
                new BatchGetDeploymentGroupsRequest
                {
                    ApplicationName = applicationName,
                    DeploymentGroupNames = deploymentGroupNames
                });
            return new CdBatchGetDeploymentGroupsResult(
                DeploymentGroupsInfo: resp.DeploymentGroupsInfo,
                ErrorMessage: resp.ErrorMessage);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get deployment groups");
        }
    }

    /// <summary>
    /// Get information about multiple deployments.
    /// </summary>
    public static async Task<CdBatchGetDeploymentsResult>
        BatchGetDeploymentsAsync(
            List<string> deploymentIds,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetDeploymentsAsync(
                new BatchGetDeploymentsRequest
                {
                    DeploymentIds = deploymentIds
                });
            return new CdBatchGetDeploymentsResult(
                DeploymentsInfo: resp.DeploymentsInfo);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get deployments");
        }
    }

    /// <summary>
    /// Set the result of a lifecycle event hook execution.
    /// </summary>
    public static async Task<CdPutLifecycleEventHookExecutionStatusResult>
        PutLifecycleEventHookExecutionStatusAsync(
            string deploymentId,
            string lifecycleEventHookExecutionId,
            string status,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.PutLifecycleEventHookExecutionStatusAsync(
                    new PutLifecycleEventHookExecutionStatusRequest
                    {
                        DeploymentId = deploymentId,
                        LifecycleEventHookExecutionId =
                            lifecycleEventHookExecutionId,
                        Status =
                            new LifecycleEventStatus(status)
                    });
            return new CdPutLifecycleEventHookExecutionStatusResult(
                LifecycleEventHookExecutionId:
                    resp.LifecycleEventHookExecutionId);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put lifecycle event hook execution status");
        }
    }

    /// <summary>
    /// Tag a CodeDeploy resource.
    /// </summary>
    public static async Task<CdTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Amazon.CodeDeploy.Model.Tag> tags,
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
            return new CdTagResourceResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag CodeDeploy resource");
        }
    }

    /// <summary>
    /// Remove tags from a CodeDeploy resource.
    /// </summary>
    public static async Task<CdUntagResourceResult> UntagResourceAsync(
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
            return new CdUntagResourceResult();
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag CodeDeploy resource");
        }
    }

    /// <summary>
    /// List tags for a CodeDeploy resource.
    /// </summary>
    public static async Task<CdListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new CdListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeDeployException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for CodeDeploy resource");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateApplicationAsync"/>.</summary>
    public static CdCreateApplicationResult CreateApplication(string applicationName, string? computePlatform = null, List<Amazon.CodeDeploy.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateApplicationAsync(applicationName, computePlatform, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteApplicationAsync"/>.</summary>
    public static CdDeleteApplicationResult DeleteApplication(string applicationName, RegionEndpoint? region = null)
        => DeleteApplicationAsync(applicationName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetApplicationAsync"/>.</summary>
    public static CdGetApplicationResult GetApplication(string applicationName, RegionEndpoint? region = null)
        => GetApplicationAsync(applicationName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListApplicationsAsync"/>.</summary>
    public static CdListApplicationsResult ListApplications(string? nextToken = null, RegionEndpoint? region = null)
        => ListApplicationsAsync(nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeploymentGroupAsync"/>.</summary>
    public static CdCreateDeploymentGroupResult CreateDeploymentGroup(CreateDeploymentGroupRequest request, RegionEndpoint? region = null)
        => CreateDeploymentGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDeploymentGroupAsync"/>.</summary>
    public static CdDeleteDeploymentGroupResult DeleteDeploymentGroup(string applicationName, string deploymentGroupName, RegionEndpoint? region = null)
        => DeleteDeploymentGroupAsync(applicationName, deploymentGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeploymentGroupAsync"/>.</summary>
    public static CdGetDeploymentGroupResult GetDeploymentGroup(string applicationName, string deploymentGroupName, RegionEndpoint? region = null)
        => GetDeploymentGroupAsync(applicationName, deploymentGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeploymentGroupsAsync"/>.</summary>
    public static CdListDeploymentGroupsResult ListDeploymentGroups(string applicationName, string? nextToken = null, RegionEndpoint? region = null)
        => ListDeploymentGroupsAsync(applicationName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDeploymentGroupAsync"/>.</summary>
    public static CdUpdateDeploymentGroupResult UpdateDeploymentGroup(UpdateDeploymentGroupRequest request, RegionEndpoint? region = null)
        => UpdateDeploymentGroupAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeploymentAsync"/>.</summary>
    public static CdCreateDeploymentResult CreateDeployment(CreateDeploymentRequest request, RegionEndpoint? region = null)
        => CreateDeploymentAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopDeploymentAsync"/>.</summary>
    public static CdStopDeploymentResult StopDeployment(string deploymentId, bool? autoRollbackEnabled = null, RegionEndpoint? region = null)
        => StopDeploymentAsync(deploymentId, autoRollbackEnabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeploymentAsync"/>.</summary>
    public static CdGetDeploymentResult GetDeployment(string deploymentId, RegionEndpoint? region = null)
        => GetDeploymentAsync(deploymentId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeploymentsAsync"/>.</summary>
    public static CdListDeploymentsResult ListDeployments(ListDeploymentsRequest request, RegionEndpoint? region = null)
        => ListDeploymentsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeploymentInstanceAsync"/>.</summary>
    public static CdGetDeploymentInstanceResult GetDeploymentInstance(string deploymentId, string instanceId, RegionEndpoint? region = null)
        => GetDeploymentInstanceAsync(deploymentId, instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeploymentInstancesAsync"/>.</summary>
    public static CdListDeploymentInstancesResult ListDeploymentInstances(ListDeploymentInstancesRequest request, RegionEndpoint? region = null)
        => ListDeploymentInstancesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ContinueDeploymentAsync"/>.</summary>
    public static CdContinueDeploymentResult ContinueDeployment(string deploymentId, string? deploymentWaitType = null, RegionEndpoint? region = null)
        => ContinueDeploymentAsync(deploymentId, deploymentWaitType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeploymentConfigAsync"/>.</summary>
    public static CdCreateDeploymentConfigResult CreateDeploymentConfig(CreateDeploymentConfigRequest request, RegionEndpoint? region = null)
        => CreateDeploymentConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDeploymentConfigAsync"/>.</summary>
    public static CdDeleteDeploymentConfigResult DeleteDeploymentConfig(string deploymentConfigName, RegionEndpoint? region = null)
        => DeleteDeploymentConfigAsync(deploymentConfigName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeploymentConfigAsync"/>.</summary>
    public static CdGetDeploymentConfigResult GetDeploymentConfig(string deploymentConfigName, RegionEndpoint? region = null)
        => GetDeploymentConfigAsync(deploymentConfigName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeploymentConfigsAsync"/>.</summary>
    public static CdListDeploymentConfigsResult ListDeploymentConfigs(string? nextToken = null, RegionEndpoint? region = null)
        => ListDeploymentConfigsAsync(nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RegisterOnPremisesInstanceAsync"/>.</summary>
    public static CdRegisterOnPremisesInstanceResult RegisterOnPremisesInstance(RegisterOnPremisesInstanceRequest request, RegionEndpoint? region = null)
        => RegisterOnPremisesInstanceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeregisterOnPremisesInstanceAsync"/>.</summary>
    public static CdDeregisterOnPremisesInstanceResult DeregisterOnPremisesInstance(string instanceName, RegionEndpoint? region = null)
        => DeregisterOnPremisesInstanceAsync(instanceName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListOnPremisesInstancesAsync"/>.</summary>
    public static CdListOnPremisesInstancesResult ListOnPremisesInstances(ListOnPremisesInstancesRequest request, RegionEndpoint? region = null)
        => ListOnPremisesInstancesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToOnPremisesInstancesAsync"/>.</summary>
    public static CdAddTagsToOnPremisesInstancesResult AddTagsToOnPremisesInstances(List<Amazon.CodeDeploy.Model.Tag> tags, List<string> instanceNames, RegionEndpoint? region = null)
        => AddTagsToOnPremisesInstancesAsync(tags, instanceNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromOnPremisesInstancesAsync"/>.</summary>
    public static CdRemoveTagsFromOnPremisesInstancesResult RemoveTagsFromOnPremisesInstances(List<Amazon.CodeDeploy.Model.Tag> tags, List<string> instanceNames, RegionEndpoint? region = null)
        => RemoveTagsFromOnPremisesInstancesAsync(tags, instanceNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetApplicationsAsync"/>.</summary>
    public static CdBatchGetApplicationsResult BatchGetApplications(List<string> applicationNames, RegionEndpoint? region = null)
        => BatchGetApplicationsAsync(applicationNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetDeploymentGroupsAsync"/>.</summary>
    public static CdBatchGetDeploymentGroupsResult BatchGetDeploymentGroups(string applicationName, List<string> deploymentGroupNames, RegionEndpoint? region = null)
        => BatchGetDeploymentGroupsAsync(applicationName, deploymentGroupNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetDeploymentsAsync"/>.</summary>
    public static CdBatchGetDeploymentsResult BatchGetDeployments(List<string> deploymentIds, RegionEndpoint? region = null)
        => BatchGetDeploymentsAsync(deploymentIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutLifecycleEventHookExecutionStatusAsync"/>.</summary>
    public static CdPutLifecycleEventHookExecutionStatusResult PutLifecycleEventHookExecutionStatus(string deploymentId, string lifecycleEventHookExecutionId, string status, RegionEndpoint? region = null)
        => PutLifecycleEventHookExecutionStatusAsync(deploymentId, lifecycleEventHookExecutionId, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static CdTagResourceResult TagResource(string resourceArn, List<Amazon.CodeDeploy.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static CdUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static CdListTagsForResourceResult ListTagsForResource(string resourceArn, string? nextToken = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, nextToken, region).GetAwaiter().GetResult();

}
