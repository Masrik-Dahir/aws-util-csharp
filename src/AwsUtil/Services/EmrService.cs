using Amazon;
using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record RunJobFlowResult(string? JobFlowId = null);

public sealed record DescribeEmrClusterResult(
    string? Id = null,
    string? Name = null,
    string? State = null,
    string? StateChangeReasonCode = null,
    string? StateChangeReasonMessage = null,
    string? ClusterArn = null,
    int? NormalizedInstanceHours = null,
    string? ReleaseLabel = null,
    bool? TerminationProtected = null,
    bool? VisibleToAllUsers = null);

public sealed record ListEmrClustersResult(
    List<ClusterSummary>? Clusters = null,
    string? Marker = null);

public sealed record AddJobFlowStepsResult(List<string>? StepIds = null);

public sealed record ListEmrStepsResult(
    List<StepSummary>? Steps = null,
    string? Marker = null);

public sealed record DescribeEmrStepResult(
    string? Id = null,
    string? Name = null,
    string? ActionOnFailure = null,
    string? State = null);

public sealed record CancelEmrStepsResult(
    List<CancelStepsInfo>? CancelStepsInfoList = null);

public sealed record PutAutoScalingPolicyResult(
    string? ClusterId = null,
    string? InstanceGroupId = null,
    AutoScalingPolicyDescription? AutoScalingPolicy = null,
    string? ClusterArn = null);

public sealed record AddInstanceGroupsResult(
    string? JobFlowId = null,
    List<string>? InstanceGroupIds = null,
    string? ClusterArn = null);

public sealed record ListInstanceGroupsResult(
    List<InstanceGroup>? InstanceGroups = null,
    string? Marker = null);

public sealed record ListEmrInstancesResult(
    List<Instance>? Instances = null,
    string? Marker = null);

public sealed record GetManagedScalingPolicyResult(
    ManagedScalingPolicy? ManagedScalingPolicy = null);

public sealed record CreateEmrStudioResult(
    string? StudioId = null,
    string? Url = null);

public sealed record DescribeEmrStudioResult(
    string? StudioId = null,
    string? Name = null,
    string? Url = null,
    string? VpcId = null,
    string? EngineSecurityGroupId = null,
    string? WorkspaceSecurityGroupId = null,
    string? ServiceRole = null,
    string? StudioArn = null);

public sealed record ListEmrStudiosResult(
    List<StudioSummary>? Studios = null,
    string? Marker = null);

public sealed record ModifyEmrClusterResult(int? StepConcurrencyLevel = null);

public sealed record CreateSecurityConfigurationResult(
    string? Name = null,
    DateTime? CreationDateTime = null);

public sealed record DescribeSecurityConfigurationResult(
    string? Name = null,
    string? SecurityConfiguration = null,
    DateTime? CreationDateTime = null);

public sealed record ListSecurityConfigurationsResult(
    List<SecurityConfigurationSummary>? SecurityConfigurations = null,
    string? Marker = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon EMR (Elastic MapReduce).
/// </summary>
public static class EmrService
{
    private static AmazonElasticMapReduceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonElasticMapReduceClient>(region);

    // ──────────────────────────── Job Flows ────────────────────────────

    /// <summary>
    /// Run a new EMR job flow (cluster).
    /// </summary>
    public static async Task<RunJobFlowResult> RunJobFlowAsync(
        RunJobFlowRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RunJobFlowAsync(request);
            return new RunJobFlowResult(JobFlowId: resp.JobFlowId);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to run EMR job flow");
        }
    }

    /// <summary>
    /// Terminate one or more EMR job flows.
    /// </summary>
    public static async Task TerminateJobFlowsAsync(
        List<string> jobFlowIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TerminateJobFlowsAsync(new TerminateJobFlowsRequest
            {
                JobFlowIds = jobFlowIds
            });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to terminate EMR job flows");
        }
    }

    // ──────────────────────────── Clusters ─────────────────────────────

    /// <summary>
    /// Describe an EMR cluster.
    /// </summary>
    public static async Task<DescribeEmrClusterResult> DescribeClusterAsync(
        string clusterId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClusterAsync(new DescribeClusterRequest
            {
                ClusterId = clusterId
            });
            var c = resp.Cluster;
            return new DescribeEmrClusterResult(
                Id: c.Id,
                Name: c.Name,
                State: c.Status?.State?.Value,
                StateChangeReasonCode: c.Status?.StateChangeReason?.Code?.Value,
                StateChangeReasonMessage: c.Status?.StateChangeReason?.Message,
                ClusterArn: c.ClusterArn,
                NormalizedInstanceHours: c.NormalizedInstanceHours,
                ReleaseLabel: c.ReleaseLabel,
                TerminationProtected: c.TerminationProtected,
                VisibleToAllUsers: c.VisibleToAllUsers);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EMR cluster '{clusterId}'");
        }
    }

    /// <summary>
    /// List EMR clusters.
    /// </summary>
    public static async Task<ListEmrClustersResult> ListClustersAsync(
        List<string>? clusterStates = null,
        DateTime? createdAfter = null,
        DateTime? createdBefore = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListClustersRequest();
        if (clusterStates != null)
            request.ClusterStates = clusterStates;
        if (createdAfter.HasValue) request.CreatedAfter = createdAfter.Value;
        if (createdBefore.HasValue) request.CreatedBefore = createdBefore.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListClustersAsync(request);
            return new ListEmrClustersResult(
                Clusters: resp.Clusters,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list EMR clusters");
        }
    }

    // ──────────────────────────── Steps ────────────────────────────────

    /// <summary>
    /// Add steps to an EMR job flow.
    /// </summary>
    public static async Task<AddJobFlowStepsResult> AddJobFlowStepsAsync(
        string jobFlowId,
        List<StepConfig> steps,
        string? executionRoleArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AddJobFlowStepsRequest
        {
            JobFlowId = jobFlowId,
            Steps = steps
        };
        if (executionRoleArn != null) request.ExecutionRoleArn = executionRoleArn;

        try
        {
            var resp = await client.AddJobFlowStepsAsync(request);
            return new AddJobFlowStepsResult(StepIds: resp.StepIds);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add steps to EMR job flow '{jobFlowId}'");
        }
    }

    /// <summary>
    /// List steps in an EMR cluster.
    /// </summary>
    public static async Task<ListEmrStepsResult> ListStepsAsync(
        string clusterId,
        List<string>? stepStates = null,
        List<string>? stepIds = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStepsRequest { ClusterId = clusterId };
        if (stepStates != null) request.StepStates = stepStates;
        if (stepIds != null) request.StepIds = stepIds;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListStepsAsync(request);
            return new ListEmrStepsResult(
                Steps: resp.Steps,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list steps for EMR cluster '{clusterId}'");
        }
    }

    /// <summary>
    /// Describe a step in an EMR cluster.
    /// </summary>
    public static async Task<DescribeEmrStepResult> DescribeStepAsync(
        string clusterId,
        string stepId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStepAsync(new DescribeStepRequest
            {
                ClusterId = clusterId,
                StepId = stepId
            });
            var s = resp.Step;
            return new DescribeEmrStepResult(
                Id: s.Id,
                Name: s.Name,
                ActionOnFailure: s.ActionOnFailure?.Value,
                State: s.Status?.State?.Value);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe step '{stepId}' in EMR cluster '{clusterId}'");
        }
    }

    /// <summary>
    /// Cancel pending steps in an EMR cluster.
    /// </summary>
    public static async Task<CancelEmrStepsResult> CancelStepsAsync(
        string clusterId,
        List<string> stepIds,
        string? stepCancellationOption = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CancelStepsRequest
        {
            ClusterId = clusterId,
            StepIds = stepIds
        };
        if (stepCancellationOption != null)
            request.StepCancellationOption =
                new StepCancellationOption(stepCancellationOption);

        try
        {
            var resp = await client.CancelStepsAsync(request);
            return new CancelEmrStepsResult(
                CancelStepsInfoList: resp.CancelStepsInfoList);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel steps in EMR cluster '{clusterId}'");
        }
    }

    // ──────────────────────── Cluster Settings ─────────────────────────

    /// <summary>
    /// Set termination protection on EMR job flows.
    /// </summary>
    public static async Task SetTerminationProtectionAsync(
        List<string> jobFlowIds,
        bool terminationProtected,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetTerminationProtectionAsync(
                new SetTerminationProtectionRequest
                {
                    JobFlowIds = jobFlowIds,
                    TerminationProtected = terminationProtected
                });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to set termination protection on EMR job flows");
        }
    }

    /// <summary>
    /// Set whether EMR clusters are visible to all IAM users.
    /// </summary>
    public static async Task SetVisibleToAllUsersAsync(
        List<string> jobFlowIds,
        bool visibleToAllUsers,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetVisibleToAllUsersAsync(
                new SetVisibleToAllUsersRequest
                {
                    JobFlowIds = jobFlowIds,
                    VisibleToAllUsers = visibleToAllUsers
                });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to set visible-to-all-users on EMR job flows");
        }
    }

    /// <summary>
    /// Modify an EMR cluster (e.g., step concurrency level).
    /// </summary>
    public static async Task<ModifyEmrClusterResult> ModifyClusterAsync(
        string clusterId,
        int? stepConcurrencyLevel = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ModifyClusterRequest { ClusterId = clusterId };
        if (stepConcurrencyLevel.HasValue)
            request.StepConcurrencyLevel = stepConcurrencyLevel.Value;

        try
        {
            var resp = await client.ModifyClusterAsync(request);
            return new ModifyEmrClusterResult(
                StepConcurrencyLevel: resp.StepConcurrencyLevel);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to modify EMR cluster '{clusterId}'");
        }
    }

    // ────────────────────── Auto-Scaling Policies ──────────────────────

    /// <summary>
    /// Put an auto-scaling policy on an EMR instance group.
    /// </summary>
    public static async Task<PutAutoScalingPolicyResult>
        PutAutoScalingPolicyAsync(
            string clusterId,
            string instanceGroupId,
            AutoScalingPolicy autoScalingPolicy,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutAutoScalingPolicyAsync(
                new PutAutoScalingPolicyRequest
                {
                    ClusterId = clusterId,
                    InstanceGroupId = instanceGroupId,
                    AutoScalingPolicy = autoScalingPolicy
                });
            return new PutAutoScalingPolicyResult(
                ClusterId: resp.ClusterId,
                InstanceGroupId: resp.InstanceGroupId,
                AutoScalingPolicy: resp.AutoScalingPolicy,
                ClusterArn: resp.ClusterArn);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put auto-scaling policy on instance group '{instanceGroupId}'");
        }
    }

    /// <summary>
    /// Remove an auto-scaling policy from an EMR instance group.
    /// </summary>
    public static async Task RemoveAutoScalingPolicyAsync(
        string clusterId,
        string instanceGroupId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveAutoScalingPolicyAsync(
                new RemoveAutoScalingPolicyRequest
                {
                    ClusterId = clusterId,
                    InstanceGroupId = instanceGroupId
                });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove auto-scaling policy from instance group '{instanceGroupId}'");
        }
    }

    // ────────────────────── Instance Groups ────────────────────────────

    /// <summary>
    /// Add instance groups to an EMR job flow.
    /// </summary>
    public static async Task<AddInstanceGroupsResult> AddInstanceGroupsAsync(
        string jobFlowId,
        List<InstanceGroupConfig> instanceGroups,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddInstanceGroupsAsync(
                new AddInstanceGroupsRequest
                {
                    JobFlowId = jobFlowId,
                    InstanceGroups = instanceGroups
                });
            return new AddInstanceGroupsResult(
                JobFlowId: resp.JobFlowId,
                InstanceGroupIds: resp.InstanceGroupIds,
                ClusterArn: resp.ClusterArn);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add instance groups to EMR job flow '{jobFlowId}'");
        }
    }

    /// <summary>
    /// Modify instance groups in an EMR cluster.
    /// </summary>
    public static async Task ModifyInstanceGroupsAsync(
        string? clusterId,
        List<InstanceGroupModifyConfig> instanceGroups,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ModifyInstanceGroupsRequest
        {
            InstanceGroups = instanceGroups
        };
        if (clusterId != null) request.ClusterId = clusterId;

        try
        {
            await client.ModifyInstanceGroupsAsync(request);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify EMR instance groups");
        }
    }

    /// <summary>
    /// List instance groups in an EMR cluster.
    /// </summary>
    public static async Task<ListInstanceGroupsResult> ListInstanceGroupsAsync(
        string clusterId,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListInstanceGroupsRequest { ClusterId = clusterId };
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListInstanceGroupsAsync(request);
            return new ListInstanceGroupsResult(
                InstanceGroups: resp.InstanceGroups,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list instance groups for EMR cluster '{clusterId}'");
        }
    }

    // ──────────────────────── Instances ────────────────────────────────

    /// <summary>
    /// List instances in an EMR cluster.
    /// </summary>
    public static async Task<ListEmrInstancesResult> ListInstancesAsync(
        string clusterId,
        string? instanceGroupId = null,
        List<string>? instanceGroupTypes = null,
        List<string>? instanceStates = null,
        string? instanceFleetId = null,
        string? instanceFleetType = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListInstancesRequest { ClusterId = clusterId };
        if (instanceGroupId != null) request.InstanceGroupId = instanceGroupId;
        if (instanceGroupTypes != null)
            request.InstanceGroupTypes = instanceGroupTypes;
        if (instanceStates != null) request.InstanceStates = instanceStates;
        if (instanceFleetId != null) request.InstanceFleetId = instanceFleetId;
        if (instanceFleetType != null)
            request.InstanceFleetType =
                new InstanceFleetType(instanceFleetType);
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListInstancesAsync(request);
            return new ListEmrInstancesResult(
                Instances: resp.Instances,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list instances for EMR cluster '{clusterId}'");
        }
    }

    // ──────────────────────── Tags ─────────────────────────────────────

    /// <summary>
    /// Add tags to an EMR resource.
    /// </summary>
    public static async Task AddTagsAsync(
        string resourceId,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsAsync(new AddTagsRequest
            {
                ResourceId = resourceId,
                Tags = tags
            });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add tags to EMR resource '{resourceId}'");
        }
    }

    /// <summary>
    /// Remove tags from an EMR resource.
    /// </summary>
    public static async Task RemoveTagsAsync(
        string resourceId,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsAsync(new RemoveTagsRequest
            {
                ResourceId = resourceId,
                TagKeys = tagKeys
            });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove tags from EMR resource '{resourceId}'");
        }
    }

    // ────────────────────── Managed Scaling ────────────────────────────

    /// <summary>
    /// Put a managed scaling policy on an EMR cluster.
    /// </summary>
    public static async Task PutManagedScalingPolicyAsync(
        string clusterId,
        ManagedScalingPolicy managedScalingPolicy,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutManagedScalingPolicyAsync(
                new PutManagedScalingPolicyRequest
                {
                    ClusterId = clusterId,
                    ManagedScalingPolicy = managedScalingPolicy
                });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put managed scaling policy on EMR cluster '{clusterId}'");
        }
    }

    /// <summary>
    /// Remove the managed scaling policy from an EMR cluster.
    /// </summary>
    public static async Task RemoveManagedScalingPolicyAsync(
        string clusterId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveManagedScalingPolicyAsync(
                new RemoveManagedScalingPolicyRequest
                {
                    ClusterId = clusterId
                });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove managed scaling policy from EMR cluster '{clusterId}'");
        }
    }

    /// <summary>
    /// Get the managed scaling policy for an EMR cluster.
    /// </summary>
    public static async Task<GetManagedScalingPolicyResult>
        GetManagedScalingPolicyAsync(
            string clusterId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetManagedScalingPolicyAsync(
                new GetManagedScalingPolicyRequest
                {
                    ClusterId = clusterId
                });
            return new GetManagedScalingPolicyResult(
                ManagedScalingPolicy: resp.ManagedScalingPolicy);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get managed scaling policy for EMR cluster '{clusterId}'");
        }
    }

    // ──────────────────────── Studios ──────────────────────────────────

    /// <summary>
    /// Create an EMR Studio.
    /// </summary>
    public static async Task<CreateEmrStudioResult> CreateStudioAsync(
        CreateStudioRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStudioAsync(request);
            return new CreateEmrStudioResult(
                StudioId: resp.StudioId,
                Url: resp.Url);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create EMR Studio");
        }
    }

    /// <summary>
    /// Delete an EMR Studio.
    /// </summary>
    public static async Task DeleteStudioAsync(
        string studioId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteStudioAsync(new DeleteStudioRequest
            {
                StudioId = studioId
            });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EMR Studio '{studioId}'");
        }
    }

    /// <summary>
    /// Describe an EMR Studio.
    /// </summary>
    public static async Task<DescribeEmrStudioResult> DescribeStudioAsync(
        string studioId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStudioAsync(
                new DescribeStudioRequest { StudioId = studioId });
            var s = resp.Studio;
            return new DescribeEmrStudioResult(
                StudioId: s.StudioId,
                Name: s.Name,
                Url: s.Url,
                VpcId: s.VpcId,
                EngineSecurityGroupId: s.EngineSecurityGroupId,
                WorkspaceSecurityGroupId: s.WorkspaceSecurityGroupId,
                ServiceRole: s.ServiceRole,
                StudioArn: s.StudioArn);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EMR Studio '{studioId}'");
        }
    }

    /// <summary>
    /// List EMR Studios.
    /// </summary>
    public static async Task<ListEmrStudiosResult> ListStudiosAsync(
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStudiosRequest();
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListStudiosAsync(request);
            return new ListEmrStudiosResult(
                Studios: resp.Studios,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR Studios");
        }
    }

    // ──────────────────── Security Configurations ─────────────────────

    /// <summary>
    /// Create a security configuration for EMR.
    /// </summary>
    public static async Task<CreateSecurityConfigurationResult>
        CreateSecurityConfigurationAsync(
            string name,
            string securityConfiguration,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSecurityConfigurationAsync(
                new CreateSecurityConfigurationRequest
                {
                    Name = name,
                    SecurityConfiguration = securityConfiguration
                });
            return new CreateSecurityConfigurationResult(
                Name: resp.Name,
                CreationDateTime: resp.CreationDateTime);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create EMR security configuration '{name}'");
        }
    }

    /// <summary>
    /// Delete a security configuration for EMR.
    /// </summary>
    public static async Task DeleteSecurityConfigurationAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSecurityConfigurationAsync(
                new DeleteSecurityConfigurationRequest { Name = name });
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete EMR security configuration '{name}'");
        }
    }

    /// <summary>
    /// Describe a security configuration for EMR.
    /// </summary>
    public static async Task<DescribeSecurityConfigurationResult>
        DescribeSecurityConfigurationAsync(
            string name,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSecurityConfigurationAsync(
                new DescribeSecurityConfigurationRequest { Name = name });
            return new DescribeSecurityConfigurationResult(
                Name: resp.Name,
                SecurityConfiguration: resp.SecurityConfiguration,
                CreationDateTime: resp.CreationDateTime);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe EMR security configuration '{name}'");
        }
    }

    /// <summary>
    /// List security configurations for EMR.
    /// </summary>
    public static async Task<ListSecurityConfigurationsResult>
        ListSecurityConfigurationsAsync(
            string? marker = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSecurityConfigurationsRequest();
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListSecurityConfigurationsAsync(request);
            return new ListSecurityConfigurationsResult(
                SecurityConfigurations: resp.SecurityConfigurations,
                Marker: resp.Marker);
        }
        catch (AmazonElasticMapReduceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR security configurations");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="RunJobFlowAsync"/>.</summary>
    public static RunJobFlowResult RunJobFlow(RunJobFlowRequest request, RegionEndpoint? region = null)
        => RunJobFlowAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TerminateJobFlowsAsync"/>.</summary>
    public static void TerminateJobFlows(List<string> jobFlowIds, RegionEndpoint? region = null)
        => TerminateJobFlowsAsync(jobFlowIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeClusterAsync"/>.</summary>
    public static DescribeEmrClusterResult DescribeCluster(string clusterId, RegionEndpoint? region = null)
        => DescribeClusterAsync(clusterId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListClustersAsync"/>.</summary>
    public static ListEmrClustersResult ListClusters(List<string>? clusterStates = null, DateTime? createdAfter = null, DateTime? createdBefore = null, string? marker = null, RegionEndpoint? region = null)
        => ListClustersAsync(clusterStates, createdAfter, createdBefore, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddJobFlowStepsAsync"/>.</summary>
    public static AddJobFlowStepsResult AddJobFlowSteps(string jobFlowId, List<StepConfig> steps, string? executionRoleArn = null, RegionEndpoint? region = null)
        => AddJobFlowStepsAsync(jobFlowId, steps, executionRoleArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListStepsAsync"/>.</summary>
    public static ListEmrStepsResult ListSteps(string clusterId, List<string>? stepStates = null, List<string>? stepIds = null, string? marker = null, RegionEndpoint? region = null)
        => ListStepsAsync(clusterId, stepStates, stepIds, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStepAsync"/>.</summary>
    public static DescribeEmrStepResult DescribeStep(string clusterId, string stepId, RegionEndpoint? region = null)
        => DescribeStepAsync(clusterId, stepId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelStepsAsync"/>.</summary>
    public static CancelEmrStepsResult CancelSteps(string clusterId, List<string> stepIds, string? stepCancellationOption = null, RegionEndpoint? region = null)
        => CancelStepsAsync(clusterId, stepIds, stepCancellationOption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetTerminationProtectionAsync"/>.</summary>
    public static void SetTerminationProtection(List<string> jobFlowIds, bool terminationProtected, RegionEndpoint? region = null)
        => SetTerminationProtectionAsync(jobFlowIds, terminationProtected, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetVisibleToAllUsersAsync"/>.</summary>
    public static void SetVisibleToAllUsers(List<string> jobFlowIds, bool visibleToAllUsers, RegionEndpoint? region = null)
        => SetVisibleToAllUsersAsync(jobFlowIds, visibleToAllUsers, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyClusterAsync"/>.</summary>
    public static ModifyEmrClusterResult ModifyCluster(string clusterId, int? stepConcurrencyLevel = null, RegionEndpoint? region = null)
        => ModifyClusterAsync(clusterId, stepConcurrencyLevel, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutAutoScalingPolicyAsync"/>.</summary>
    public static PutAutoScalingPolicyResult PutAutoScalingPolicy(string clusterId, string instanceGroupId, AutoScalingPolicy autoScalingPolicy, RegionEndpoint? region = null)
        => PutAutoScalingPolicyAsync(clusterId, instanceGroupId, autoScalingPolicy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveAutoScalingPolicyAsync"/>.</summary>
    public static void RemoveAutoScalingPolicy(string clusterId, string instanceGroupId, RegionEndpoint? region = null)
        => RemoveAutoScalingPolicyAsync(clusterId, instanceGroupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddInstanceGroupsAsync"/>.</summary>
    public static AddInstanceGroupsResult AddInstanceGroups(string jobFlowId, List<InstanceGroupConfig> instanceGroups, RegionEndpoint? region = null)
        => AddInstanceGroupsAsync(jobFlowId, instanceGroups, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModifyInstanceGroupsAsync"/>.</summary>
    public static void ModifyInstanceGroups(string? clusterId, List<InstanceGroupModifyConfig> instanceGroups, RegionEndpoint? region = null)
        => ModifyInstanceGroupsAsync(clusterId, instanceGroups, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInstanceGroupsAsync"/>.</summary>
    public static ListInstanceGroupsResult ListInstanceGroups(string clusterId, string? marker = null, RegionEndpoint? region = null)
        => ListInstanceGroupsAsync(clusterId, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInstancesAsync"/>.</summary>
    public static ListEmrInstancesResult ListInstances(string clusterId, string? instanceGroupId = null, List<string>? instanceGroupTypes = null, List<string>? instanceStates = null, string? instanceFleetId = null, string? instanceFleetType = null, string? marker = null, RegionEndpoint? region = null)
        => ListInstancesAsync(clusterId, instanceGroupId, instanceGroupTypes, instanceStates, instanceFleetId, instanceFleetType, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsAsync"/>.</summary>
    public static void AddTags(string resourceId, List<Tag> tags, RegionEndpoint? region = null)
        => AddTagsAsync(resourceId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsAsync"/>.</summary>
    public static void RemoveTags(string resourceId, List<string> tagKeys, RegionEndpoint? region = null)
        => RemoveTagsAsync(resourceId, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutManagedScalingPolicyAsync"/>.</summary>
    public static void PutManagedScalingPolicy(string clusterId, ManagedScalingPolicy managedScalingPolicy, RegionEndpoint? region = null)
        => PutManagedScalingPolicyAsync(clusterId, managedScalingPolicy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveManagedScalingPolicyAsync"/>.</summary>
    public static void RemoveManagedScalingPolicy(string clusterId, RegionEndpoint? region = null)
        => RemoveManagedScalingPolicyAsync(clusterId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetManagedScalingPolicyAsync"/>.</summary>
    public static GetManagedScalingPolicyResult GetManagedScalingPolicy(string clusterId, RegionEndpoint? region = null)
        => GetManagedScalingPolicyAsync(clusterId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateStudioAsync"/>.</summary>
    public static CreateEmrStudioResult CreateStudio(CreateStudioRequest request, RegionEndpoint? region = null)
        => CreateStudioAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteStudioAsync"/>.</summary>
    public static void DeleteStudio(string studioId, RegionEndpoint? region = null)
        => DeleteStudioAsync(studioId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStudioAsync"/>.</summary>
    public static DescribeEmrStudioResult DescribeStudio(string studioId, RegionEndpoint? region = null)
        => DescribeStudioAsync(studioId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListStudiosAsync"/>.</summary>
    public static ListEmrStudiosResult ListStudios(string? marker = null, RegionEndpoint? region = null)
        => ListStudiosAsync(marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSecurityConfigurationAsync"/>.</summary>
    public static CreateSecurityConfigurationResult CreateSecurityConfiguration(string name, string securityConfiguration, RegionEndpoint? region = null)
        => CreateSecurityConfigurationAsync(name, securityConfiguration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSecurityConfigurationAsync"/>.</summary>
    public static void DeleteSecurityConfiguration(string name, RegionEndpoint? region = null)
        => DeleteSecurityConfigurationAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeSecurityConfigurationAsync"/>.</summary>
    public static DescribeSecurityConfigurationResult DescribeSecurityConfiguration(string name, RegionEndpoint? region = null)
        => DescribeSecurityConfigurationAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSecurityConfigurationsAsync"/>.</summary>
    public static ListSecurityConfigurationsResult ListSecurityConfigurations(string? marker = null, RegionEndpoint? region = null)
        => ListSecurityConfigurationsAsync(marker, region).GetAwaiter().GetResult();

}
