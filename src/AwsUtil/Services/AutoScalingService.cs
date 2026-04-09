using Amazon;
using Amazon.AutoScaling;
using Amazon.AutoScaling.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of CreateAutoScalingGroup (void operation).</summary>
public sealed record CreateAutoScalingGroupResult(bool Success = true);

/// <summary>Result of DeleteAutoScalingGroup (void operation).</summary>
public sealed record DeleteAutoScalingGroupResult(bool Success = true);

/// <summary>Result of DescribeAutoScalingGroups.</summary>
public sealed record DescribeAutoScalingGroupsResult(
    List<AutoScalingGroup>? AutoScalingGroups = null,
    string? NextToken = null);

/// <summary>Result of UpdateAutoScalingGroup (void operation).</summary>
public sealed record UpdateAutoScalingGroupResult(bool Success = true);

/// <summary>Result of CreateLaunchConfiguration (void operation).</summary>
public sealed record CreateLaunchConfigurationResult(bool Success = true);

/// <summary>Result of DeleteLaunchConfiguration (void operation).</summary>
public sealed record DeleteLaunchConfigurationResult(bool Success = true);

/// <summary>Result of DescribeLaunchConfigurations.</summary>
public sealed record DescribeLaunchConfigurationsResult(
    List<LaunchConfiguration>? LaunchConfigurations = null,
    string? NextToken = null);

/// <summary>Result of SetDesiredCapacity (void operation).</summary>
public sealed record SetDesiredCapacityResult(bool Success = true);

/// <summary>Result of TerminateInstanceInAutoScalingGroup.</summary>
public sealed record TerminateInstanceInAutoScalingGroupResult(
    Activity? Activity = null);

/// <summary>Result of AttachInstances (void operation).</summary>
public sealed record AttachInstancesResult(bool Success = true);

/// <summary>Result of DetachInstances.</summary>
public sealed record DetachInstancesResult(List<Activity>? Activities = null);

/// <summary>Result of EnterStandby.</summary>
public sealed record EnterStandbyResult(List<Activity>? Activities = null);

/// <summary>Result of ExitStandby.</summary>
public sealed record ExitStandbyResult(List<Activity>? Activities = null);

/// <summary>Result of SuspendProcesses (void operation).</summary>
public sealed record SuspendProcessesResult(bool Success = true);

/// <summary>Result of ResumeProcesses (void operation).</summary>
public sealed record ResumeProcessesResult(bool Success = true);

/// <summary>Result of PutScalingPolicy.</summary>
public sealed record PutScalingPolicyResult(
    string? PolicyARN = null,
    List<Alarm>? Alarms = null);

/// <summary>Result of DeletePolicy (void operation).</summary>
public sealed record DeleteScalingPolicyResult(bool Success = true);

/// <summary>Result of DescribePolicies.</summary>
public sealed record DescribePoliciesResult(
    List<ScalingPolicy>? ScalingPolicies = null,
    string? NextToken = null);

/// <summary>Result of ExecutePolicy (void operation).</summary>
public sealed record ExecutePolicyResult(bool Success = true);

/// <summary>Result of PutScheduledUpdateGroupAction (void operation).</summary>
public sealed record PutScheduledUpdateGroupActionResult(bool Success = true);

/// <summary>Result of DeleteScheduledAction (void operation).</summary>
public sealed record DeleteScheduledActionResult(bool Success = true);

/// <summary>Result of DescribeScheduledActions.</summary>
public sealed record DescribeScheduledActionsResult(
    List<ScheduledUpdateGroupAction>? ScheduledUpdateGroupActions = null,
    string? NextToken = null);

/// <summary>Result of SetInstanceProtection (void operation).</summary>
public sealed record SetInstanceProtectionResult(bool Success = true);

/// <summary>Result of SetInstanceHealth (void operation).</summary>
public sealed record SetInstanceHealthResult(bool Success = true);

/// <summary>Result of DescribeAutoScalingInstances.</summary>
public sealed record DescribeAutoScalingInstancesResult(
    List<AutoScalingInstanceDetails>? AutoScalingInstances = null,
    string? NextToken = null);

/// <summary>Result of CreateOrUpdateTags (void operation).</summary>
public sealed record CreateOrUpdateTagsResult(bool Success = true);

/// <summary>Result of DeleteTags (void operation).</summary>
public sealed record DeleteAutoScalingTagsResult(bool Success = true);

/// <summary>Result of DescribeTags.</summary>
public sealed record DescribeAutoScalingTagsResult(
    List<TagDescription>? Tags = null,
    string? NextToken = null);

/// <summary>Result of PutLifecycleHook (void operation).</summary>
public sealed record PutLifecycleHookResult(bool Success = true);

/// <summary>Result of DeleteLifecycleHook (void operation).</summary>
public sealed record DeleteLifecycleHookResult(bool Success = true);

/// <summary>Result of DescribeLifecycleHooks.</summary>
public sealed record DescribeLifecycleHooksResult(
    List<LifecycleHook>? LifecycleHooks = null);

/// <summary>Result of CompleteLifecycleAction (void operation).</summary>
public sealed record CompleteLifecycleActionResult(bool Success = true);

/// <summary>Result of RecordLifecycleActionHeartbeat (void operation).</summary>
public sealed record RecordLifecycleActionHeartbeatResult(bool Success = true);

/// <summary>Result of PutNotificationConfiguration (void operation).</summary>
public sealed record PutNotificationConfigurationResult(bool Success = true);

/// <summary>Result of DeleteNotificationConfiguration (void operation).</summary>
public sealed record DeleteNotificationConfigurationResult(bool Success = true);

/// <summary>Result of DescribeNotificationConfigurations.</summary>
public sealed record DescribeNotificationConfigurationsResult(
    List<NotificationConfiguration>? NotificationConfigurations = null,
    string? NextToken = null);

/// <summary>Result of DescribeAccountLimits.</summary>
public sealed record DescribeAccountLimitsResult(
    int? MaxNumberOfAutoScalingGroups = null,
    int? MaxNumberOfLaunchConfigurations = null,
    int? NumberOfAutoScalingGroups = null,
    int? NumberOfLaunchConfigurations = null);

/// <summary>Result of DescribeAdjustmentTypes.</summary>
public sealed record DescribeAdjustmentTypesResult(
    List<AdjustmentType>? AdjustmentTypes = null);

/// <summary>Result of DescribeMetricCollectionTypes.</summary>
public sealed record DescribeMetricCollectionTypesResult(
    List<MetricCollectionType>? Metrics = null,
    List<MetricGranularityType>? Granularities = null);

/// <summary>Result of EnableMetricsCollection (void operation).</summary>
public sealed record EnableMetricsCollectionResult(bool Success = true);

/// <summary>Result of DisableMetricsCollection (void operation).</summary>
public sealed record DisableMetricsCollectionResult(bool Success = true);

/// <summary>Result of PutWarmPool (void operation).</summary>
public sealed record PutWarmPoolResult(bool Success = true);

/// <summary>Result of DeleteWarmPool (void operation).</summary>
public sealed record DeleteWarmPoolResult(bool Success = true);

/// <summary>Result of DescribeWarmPool.</summary>
public sealed record DescribeWarmPoolResult(
    WarmPoolConfiguration? WarmPoolConfiguration = null,
    List<Instance>? Instances = null,
    string? NextToken = null);

/// <summary>Result of StartInstanceRefresh.</summary>
public sealed record StartInstanceRefreshResult(string? InstanceRefreshId = null);

/// <summary>Result of CancelInstanceRefresh.</summary>
public sealed record CancelInstanceRefreshResult(string? InstanceRefreshId = null);

/// <summary>Result of DescribeInstanceRefreshes.</summary>
public sealed record DescribeInstanceRefreshesResult(
    List<InstanceRefresh>? InstanceRefreshes = null,
    string? NextToken = null);

/// <summary>Result of RollbackInstanceRefresh.</summary>
public sealed record RollbackInstanceRefreshResult(string? InstanceRefreshId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon EC2 Auto Scaling.
/// </summary>
public static class AutoScalingService
{
    private static AmazonAutoScalingClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAutoScalingClient>(region);

    // -----------------------------------------------------------------------
    // Auto Scaling group management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new Auto Scaling group.
    /// </summary>
    public static async Task<CreateAutoScalingGroupResult> CreateAutoScalingGroupAsync(
        CreateAutoScalingGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateAutoScalingGroupAsync(request);
            return new CreateAutoScalingGroupResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create Auto Scaling group");
        }
    }

    /// <summary>
    /// Delete an Auto Scaling group.
    /// </summary>
    public static async Task<DeleteAutoScalingGroupResult> DeleteAutoScalingGroupAsync(
        string autoScalingGroupName,
        bool? forceDelete = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAutoScalingGroupRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (forceDelete.HasValue) request.ForceDelete = forceDelete.Value;

        try
        {
            await client.DeleteAutoScalingGroupAsync(request);
            return new DeleteAutoScalingGroupResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Auto Scaling group '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Describe one or more Auto Scaling groups.
    /// </summary>
    public static async Task<DescribeAutoScalingGroupsResult> DescribeAutoScalingGroupsAsync(
        DescribeAutoScalingGroupsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAutoScalingGroupsAsync(request);
            return new DescribeAutoScalingGroupsResult(
                AutoScalingGroups: resp.AutoScalingGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Auto Scaling groups");
        }
    }

    /// <summary>
    /// Update an Auto Scaling group.
    /// </summary>
    public static async Task<UpdateAutoScalingGroupResult> UpdateAutoScalingGroupAsync(
        UpdateAutoScalingGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateAutoScalingGroupAsync(request);
            return new UpdateAutoScalingGroupResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update Auto Scaling group");
        }
    }

    // -----------------------------------------------------------------------
    // Launch configurations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a launch configuration.
    /// </summary>
    public static async Task<CreateLaunchConfigurationResult> CreateLaunchConfigurationAsync(
        CreateLaunchConfigurationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateLaunchConfigurationAsync(request);
            return new CreateLaunchConfigurationResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create launch configuration");
        }
    }

    /// <summary>
    /// Delete a launch configuration.
    /// </summary>
    public static async Task<DeleteLaunchConfigurationResult> DeleteLaunchConfigurationAsync(
        string launchConfigurationName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteLaunchConfigurationAsync(new DeleteLaunchConfigurationRequest
            {
                LaunchConfigurationName = launchConfigurationName
            });
            return new DeleteLaunchConfigurationResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete launch configuration '{launchConfigurationName}'");
        }
    }

    /// <summary>
    /// Describe launch configurations.
    /// </summary>
    public static async Task<DescribeLaunchConfigurationsResult> DescribeLaunchConfigurationsAsync(
        DescribeLaunchConfigurationsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLaunchConfigurationsAsync(request);
            return new DescribeLaunchConfigurationsResult(
                LaunchConfigurations: resp.LaunchConfigurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe launch configurations");
        }
    }

    // -----------------------------------------------------------------------
    // Capacity and instance management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Set the desired capacity for an Auto Scaling group.
    /// </summary>
    public static async Task<SetDesiredCapacityResult> SetDesiredCapacityAsync(
        string autoScalingGroupName,
        int desiredCapacity,
        bool? honorCooldown = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetDesiredCapacityRequest
        {
            AutoScalingGroupName = autoScalingGroupName,
            DesiredCapacity = desiredCapacity
        };
        if (honorCooldown.HasValue) request.HonorCooldown = honorCooldown.Value;

        try
        {
            await client.SetDesiredCapacityAsync(request);
            return new SetDesiredCapacityResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set desired capacity for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Terminate an instance in an Auto Scaling group.
    /// </summary>
    public static async Task<TerminateInstanceInAutoScalingGroupResult>
        TerminateInstanceInAutoScalingGroupAsync(
            string instanceId,
            bool shouldDecrementDesiredCapacity,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TerminateInstanceInAutoScalingGroupAsync(
                new TerminateInstanceInAutoScalingGroupRequest
                {
                    InstanceId = instanceId,
                    ShouldDecrementDesiredCapacity = shouldDecrementDesiredCapacity
                });
            return new TerminateInstanceInAutoScalingGroupResult(Activity: resp.Activity);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to terminate instance '{instanceId}'");
        }
    }

    /// <summary>
    /// Attach instances to an Auto Scaling group.
    /// </summary>
    public static async Task<AttachInstancesResult> AttachInstancesAsync(
        string autoScalingGroupName,
        List<string> instanceIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachInstancesAsync(new AttachInstancesRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceIds = instanceIds
            });
            return new AttachInstancesResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach instances to '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Detach instances from an Auto Scaling group.
    /// </summary>
    public static async Task<DetachInstancesResult> DetachInstancesAsync(
        string autoScalingGroupName,
        List<string> instanceIds,
        bool shouldDecrementDesiredCapacity,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DetachInstancesAsync(new DetachInstancesRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceIds = instanceIds,
                ShouldDecrementDesiredCapacity = shouldDecrementDesiredCapacity
            });
            return new DetachInstancesResult(Activities: resp.Activities);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach instances from '{autoScalingGroupName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Standby
    // -----------------------------------------------------------------------

    /// <summary>
    /// Move instances into standby state.
    /// </summary>
    public static async Task<EnterStandbyResult> EnterStandbyAsync(
        string autoScalingGroupName,
        List<string> instanceIds,
        bool shouldDecrementDesiredCapacity,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.EnterStandbyAsync(new EnterStandbyRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceIds = instanceIds,
                ShouldDecrementDesiredCapacity = shouldDecrementDesiredCapacity
            });
            return new EnterStandbyResult(Activities: resp.Activities);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enter standby for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Move instances out of standby state.
    /// </summary>
    public static async Task<ExitStandbyResult> ExitStandbyAsync(
        string autoScalingGroupName,
        List<string> instanceIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ExitStandbyAsync(new ExitStandbyRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceIds = instanceIds
            });
            return new ExitStandbyResult(Activities: resp.Activities);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to exit standby for '{autoScalingGroupName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Process management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Suspend Auto Scaling processes for a group.
    /// </summary>
    public static async Task<SuspendProcessesResult> SuspendProcessesAsync(
        string autoScalingGroupName,
        List<string>? scalingProcesses = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SuspendProcessesRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (scalingProcesses != null) request.ScalingProcesses = scalingProcesses;

        try
        {
            await client.SuspendProcessesAsync(request);
            return new SuspendProcessesResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to suspend processes for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Resume Auto Scaling processes for a group.
    /// </summary>
    public static async Task<ResumeProcessesResult> ResumeProcessesAsync(
        string autoScalingGroupName,
        List<string>? scalingProcesses = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ResumeProcessesRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (scalingProcesses != null) request.ScalingProcesses = scalingProcesses;

        try
        {
            await client.ResumeProcessesAsync(request);
            return new ResumeProcessesResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to resume processes for '{autoScalingGroupName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Scaling policies
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create or update a scaling policy.
    /// </summary>
    public static async Task<PutScalingPolicyResult> PutScalingPolicyAsync(
        PutScalingPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutScalingPolicyAsync(request);
            return new PutScalingPolicyResult(
                PolicyARN: resp.PolicyARN,
                Alarms: resp.Alarms);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put scaling policy");
        }
    }

    /// <summary>
    /// Delete a scaling policy.
    /// </summary>
    public static async Task<DeleteScalingPolicyResult> DeletePolicyAsync(
        string autoScalingGroupName,
        string policyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePolicyAsync(new DeletePolicyRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                PolicyName = policyName
            });
            return new DeleteScalingPolicyResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete policy '{policyName}'");
        }
    }

    /// <summary>
    /// Describe scaling policies for an Auto Scaling group.
    /// </summary>
    public static async Task<DescribePoliciesResult> DescribePoliciesAsync(
        DescribePoliciesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePoliciesAsync(request);
            return new DescribePoliciesResult(
                ScalingPolicies: resp.ScalingPolicies,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe policies");
        }
    }

    /// <summary>
    /// Execute a scaling policy.
    /// </summary>
    public static async Task<ExecutePolicyResult> ExecutePolicyAsync(
        ExecutePolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ExecutePolicyAsync(request);
            return new ExecutePolicyResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to execute policy");
        }
    }

    // -----------------------------------------------------------------------
    // Scheduled actions
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create or update a scheduled scaling action.
    /// </summary>
    public static async Task<PutScheduledUpdateGroupActionResult>
        PutScheduledUpdateGroupActionAsync(
            PutScheduledUpdateGroupActionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutScheduledUpdateGroupActionAsync(request);
            return new PutScheduledUpdateGroupActionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put scheduled update group action");
        }
    }

    /// <summary>
    /// Delete a scheduled action.
    /// </summary>
    public static async Task<DeleteScheduledActionResult> DeleteScheduledActionAsync(
        string autoScalingGroupName,
        string scheduledActionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteScheduledActionAsync(new DeleteScheduledActionRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                ScheduledActionName = scheduledActionName
            });
            return new DeleteScheduledActionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete scheduled action '{scheduledActionName}'");
        }
    }

    /// <summary>
    /// Describe scheduled actions for an Auto Scaling group.
    /// </summary>
    public static async Task<DescribeScheduledActionsResult> DescribeScheduledActionsAsync(
        DescribeScheduledActionsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeScheduledActionsAsync(request);
            return new DescribeScheduledActionsResult(
                ScheduledUpdateGroupActions: resp.ScheduledUpdateGroupActions,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe scheduled actions");
        }
    }

    // -----------------------------------------------------------------------
    // Instance protection and health
    // -----------------------------------------------------------------------

    /// <summary>
    /// Set instance protection for instances in an Auto Scaling group.
    /// </summary>
    public static async Task<SetInstanceProtectionResult> SetInstanceProtectionAsync(
        string autoScalingGroupName,
        List<string> instanceIds,
        bool protectedFromScaleIn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetInstanceProtectionAsync(new SetInstanceProtectionRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                InstanceIds = instanceIds,
                ProtectedFromScaleIn = protectedFromScaleIn
            });
            return new SetInstanceProtectionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set instance protection for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Set the health status of an instance.
    /// </summary>
    public static async Task<SetInstanceHealthResult> SetInstanceHealthAsync(
        string instanceId,
        string healthStatus,
        bool? shouldRespectGracePeriod = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetInstanceHealthRequest
        {
            InstanceId = instanceId,
            HealthStatus = healthStatus
        };
        if (shouldRespectGracePeriod.HasValue)
            request.ShouldRespectGracePeriod = shouldRespectGracePeriod.Value;

        try
        {
            await client.SetInstanceHealthAsync(request);
            return new SetInstanceHealthResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set instance health for '{instanceId}'");
        }
    }

    /// <summary>
    /// Describe Auto Scaling instances.
    /// </summary>
    public static async Task<DescribeAutoScalingInstancesResult>
        DescribeAutoScalingInstancesAsync(
            List<string>? instanceIds = null,
            string? nextToken = null,
            int? maxRecords = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAutoScalingInstancesRequest();
        if (instanceIds != null) request.InstanceIds = instanceIds;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeAutoScalingInstancesAsync(request);
            return new DescribeAutoScalingInstancesResult(
                AutoScalingInstances: resp.AutoScalingInstances,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Auto Scaling instances");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create or update tags for Auto Scaling resources.
    /// </summary>
    public static async Task<CreateOrUpdateTagsResult> CreateOrUpdateTagsAsync(
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateOrUpdateTagsAsync(new CreateOrUpdateTagsRequest
            {
                Tags = tags
            });
            return new CreateOrUpdateTagsResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create or update tags");
        }
    }

    /// <summary>
    /// Delete tags from Auto Scaling resources.
    /// </summary>
    public static async Task<DeleteAutoScalingTagsResult> DeleteTagsAsync(
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTagsAsync(new DeleteTagsRequest { Tags = tags });
            return new DeleteAutoScalingTagsResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete tags");
        }
    }

    /// <summary>
    /// Describe tags for Auto Scaling resources.
    /// </summary>
    public static async Task<DescribeAutoScalingTagsResult> DescribeTagsAsync(
        DescribeTagsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTagsAsync(request);
            return new DescribeAutoScalingTagsResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe tags");
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle hooks
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create or update a lifecycle hook.
    /// </summary>
    public static async Task<PutLifecycleHookResult> PutLifecycleHookAsync(
        PutLifecycleHookRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutLifecycleHookAsync(request);
            return new PutLifecycleHookResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put lifecycle hook");
        }
    }

    /// <summary>
    /// Delete a lifecycle hook.
    /// </summary>
    public static async Task<DeleteLifecycleHookResult> DeleteLifecycleHookAsync(
        string autoScalingGroupName,
        string lifecycleHookName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteLifecycleHookAsync(new DeleteLifecycleHookRequest
            {
                AutoScalingGroupName = autoScalingGroupName,
                LifecycleHookName = lifecycleHookName
            });
            return new DeleteLifecycleHookResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete lifecycle hook '{lifecycleHookName}'");
        }
    }

    /// <summary>
    /// Describe lifecycle hooks for an Auto Scaling group.
    /// </summary>
    public static async Task<DescribeLifecycleHooksResult> DescribeLifecycleHooksAsync(
        string autoScalingGroupName,
        List<string>? lifecycleHookNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeLifecycleHooksRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (lifecycleHookNames != null) request.LifecycleHookNames = lifecycleHookNames;

        try
        {
            var resp = await client.DescribeLifecycleHooksAsync(request);
            return new DescribeLifecycleHooksResult(LifecycleHooks: resp.LifecycleHooks);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe lifecycle hooks for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Complete a lifecycle action.
    /// </summary>
    public static async Task<CompleteLifecycleActionResult> CompleteLifecycleActionAsync(
        CompleteLifecycleActionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CompleteLifecycleActionAsync(request);
            return new CompleteLifecycleActionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to complete lifecycle action");
        }
    }

    /// <summary>
    /// Record a lifecycle action heartbeat.
    /// </summary>
    public static async Task<RecordLifecycleActionHeartbeatResult>
        RecordLifecycleActionHeartbeatAsync(
            RecordLifecycleActionHeartbeatRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RecordLifecycleActionHeartbeatAsync(request);
            return new RecordLifecycleActionHeartbeatResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to record lifecycle action heartbeat");
        }
    }

    // -----------------------------------------------------------------------
    // Notification configurations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Configure notifications for an Auto Scaling group.
    /// </summary>
    public static async Task<PutNotificationConfigurationResult>
        PutNotificationConfigurationAsync(
            string autoScalingGroupName,
            string topicArn,
            List<string> notificationTypes,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutNotificationConfigurationAsync(
                new PutNotificationConfigurationRequest
                {
                    AutoScalingGroupName = autoScalingGroupName,
                    TopicARN = topicArn,
                    NotificationTypes = notificationTypes
                });
            return new PutNotificationConfigurationResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put notification configuration for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Delete a notification configuration.
    /// </summary>
    public static async Task<DeleteNotificationConfigurationResult>
        DeleteNotificationConfigurationAsync(
            string autoScalingGroupName,
            string topicArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteNotificationConfigurationAsync(
                new DeleteNotificationConfigurationRequest
                {
                    AutoScalingGroupName = autoScalingGroupName,
                    TopicARN = topicArn
                });
            return new DeleteNotificationConfigurationResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete notification configuration for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Describe notification configurations.
    /// </summary>
    public static async Task<DescribeNotificationConfigurationsResult>
        DescribeNotificationConfigurationsAsync(
            DescribeNotificationConfigurationsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeNotificationConfigurationsAsync(request);
            return new DescribeNotificationConfigurationsResult(
                NotificationConfigurations: resp.NotificationConfigurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe notification configurations");
        }
    }

    // -----------------------------------------------------------------------
    // Account limits and adjustment types
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe Auto Scaling account limits.
    /// </summary>
    public static async Task<DescribeAccountLimitsResult> DescribeAccountLimitsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccountLimitsAsync(
                new DescribeAccountLimitsRequest());
            return new DescribeAccountLimitsResult(
                MaxNumberOfAutoScalingGroups: resp.MaxNumberOfAutoScalingGroups,
                MaxNumberOfLaunchConfigurations: resp.MaxNumberOfLaunchConfigurations,
                NumberOfAutoScalingGroups: resp.NumberOfAutoScalingGroups,
                NumberOfLaunchConfigurations: resp.NumberOfLaunchConfigurations);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe account limits");
        }
    }

    /// <summary>
    /// Describe the available adjustment types for scaling policies.
    /// </summary>
    public static async Task<DescribeAdjustmentTypesResult> DescribeAdjustmentTypesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAdjustmentTypesAsync(
                new DescribeAdjustmentTypesRequest());
            return new DescribeAdjustmentTypesResult(AdjustmentTypes: resp.AdjustmentTypes);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe adjustment types");
        }
    }

    // -----------------------------------------------------------------------
    // Metrics collection
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe available metric collection types.
    /// </summary>
    public static async Task<DescribeMetricCollectionTypesResult>
        DescribeMetricCollectionTypesAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeMetricCollectionTypesAsync(
                new DescribeMetricCollectionTypesRequest());
            return new DescribeMetricCollectionTypesResult(
                Metrics: resp.Metrics,
                Granularities: resp.Granularities);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe metric collection types");
        }
    }

    /// <summary>
    /// Enable metrics collection for an Auto Scaling group.
    /// </summary>
    public static async Task<EnableMetricsCollectionResult> EnableMetricsCollectionAsync(
        string autoScalingGroupName,
        string granularity,
        List<string>? metrics = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EnableMetricsCollectionRequest
        {
            AutoScalingGroupName = autoScalingGroupName,
            Granularity = granularity
        };
        if (metrics != null) request.Metrics = metrics;

        try
        {
            await client.EnableMetricsCollectionAsync(request);
            return new EnableMetricsCollectionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable metrics collection for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Disable metrics collection for an Auto Scaling group.
    /// </summary>
    public static async Task<DisableMetricsCollectionResult> DisableMetricsCollectionAsync(
        string autoScalingGroupName,
        List<string>? metrics = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DisableMetricsCollectionRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (metrics != null) request.Metrics = metrics;

        try
        {
            await client.DisableMetricsCollectionAsync(request);
            return new DisableMetricsCollectionResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable metrics collection for '{autoScalingGroupName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Warm pool
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create or update a warm pool for an Auto Scaling group.
    /// </summary>
    public static async Task<PutWarmPoolResult> PutWarmPoolAsync(
        PutWarmPoolRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutWarmPoolAsync(request);
            return new PutWarmPoolResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put warm pool");
        }
    }

    /// <summary>
    /// Delete the warm pool for an Auto Scaling group.
    /// </summary>
    public static async Task<DeleteWarmPoolResult> DeleteWarmPoolAsync(
        string autoScalingGroupName,
        bool? forceDelete = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteWarmPoolRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (forceDelete.HasValue) request.ForceDelete = forceDelete.Value;

        try
        {
            await client.DeleteWarmPoolAsync(request);
            return new DeleteWarmPoolResult();
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete warm pool for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Describe the warm pool for an Auto Scaling group.
    /// </summary>
    public static async Task<DescribeWarmPoolResult> DescribeWarmPoolAsync(
        string autoScalingGroupName,
        string? nextToken = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeWarmPoolRequest
        {
            AutoScalingGroupName = autoScalingGroupName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeWarmPoolAsync(request);
            return new DescribeWarmPoolResult(
                WarmPoolConfiguration: resp.WarmPoolConfiguration,
                Instances: resp.Instances,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe warm pool for '{autoScalingGroupName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Instance refresh
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an instance refresh for an Auto Scaling group.
    /// </summary>
    public static async Task<StartInstanceRefreshResult> StartInstanceRefreshAsync(
        StartInstanceRefreshRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartInstanceRefreshAsync(request);
            return new StartInstanceRefreshResult(
                InstanceRefreshId: resp.InstanceRefreshId);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start instance refresh");
        }
    }

    /// <summary>
    /// Cancel an in-progress instance refresh.
    /// </summary>
    public static async Task<CancelInstanceRefreshResult> CancelInstanceRefreshAsync(
        string autoScalingGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelInstanceRefreshAsync(
                new CancelInstanceRefreshRequest
                {
                    AutoScalingGroupName = autoScalingGroupName
                });
            return new CancelInstanceRefreshResult(
                InstanceRefreshId: resp.InstanceRefreshId);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel instance refresh for '{autoScalingGroupName}'");
        }
    }

    /// <summary>
    /// Describe instance refreshes for an Auto Scaling group.
    /// </summary>
    public static async Task<DescribeInstanceRefreshesResult> DescribeInstanceRefreshesAsync(
        DescribeInstanceRefreshesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeInstanceRefreshesAsync(request);
            return new DescribeInstanceRefreshesResult(
                InstanceRefreshes: resp.InstanceRefreshes,
                NextToken: resp.NextToken);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe instance refreshes");
        }
    }

    /// <summary>
    /// Roll back an in-progress instance refresh.
    /// </summary>
    public static async Task<RollbackInstanceRefreshResult> RollbackInstanceRefreshAsync(
        string autoScalingGroupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RollbackInstanceRefreshAsync(
                new RollbackInstanceRefreshRequest
                {
                    AutoScalingGroupName = autoScalingGroupName
                });
            return new RollbackInstanceRefreshResult(
                InstanceRefreshId: resp.InstanceRefreshId);
        }
        catch (AmazonAutoScalingException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rollback instance refresh for '{autoScalingGroupName}'");
        }
    }
}
