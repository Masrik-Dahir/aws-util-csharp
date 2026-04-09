using Amazon;
using Amazon.EMRContainers;
using Amazon.EMRContainers.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record EmrcCreateVirtualClusterResult(
    string? Id = null,
    string? Name = null,
    string? Arn = null);

public sealed record EmrcDeleteVirtualClusterResult(string? Id = null);

public sealed record EmrcDescribeVirtualClusterResult(
    VirtualCluster? VirtualCluster = null);

public sealed record EmrcListVirtualClustersResult(
    List<VirtualCluster>? VirtualClusters = null,
    string? NextToken = null);

public sealed record EmrcStartJobRunResult(
    string? Id = null,
    string? Name = null,
    string? Arn = null,
    string? VirtualClusterId = null);

public sealed record EmrcCancelJobRunResult(
    string? Id = null,
    string? VirtualClusterId = null);

public sealed record EmrcDescribeJobRunResult(
    JobRun? JobRun = null);

public sealed record EmrcListJobRunsResult(
    List<JobRun>? JobRuns = null,
    string? NextToken = null);

public sealed record EmrcCreateManagedEndpointResult(
    string? Id = null,
    string? Name = null,
    string? Arn = null,
    string? VirtualClusterId = null);

public sealed record EmrcDeleteManagedEndpointResult(
    string? Id = null,
    string? VirtualClusterId = null);

public sealed record EmrcDescribeManagedEndpointResult(
    Endpoint? Endpoint = null);

public sealed record EmrcListManagedEndpointsResult(
    List<Endpoint>? Endpoints = null,
    string? NextToken = null);

public sealed record EmrcCreateJobTemplateResult(
    string? Id = null,
    string? Name = null,
    string? Arn = null);

public sealed record EmrcDeleteJobTemplateResult(string? Id = null);

public sealed record EmrcDescribeJobTemplateResult(
    JobTemplate? JobTemplate = null);

public sealed record EmrcListJobTemplatesResult(
    List<JobTemplate>? Templates = null,
    string? NextToken = null);

public sealed record EmrcGetManagedEndpointSessionCredentialsResult(
    string? Id = null,
    Credentials? Credentials = null,
    string? ExpiresAt = null);

public sealed record EmrcTagResourceResult(bool Success = true);
public sealed record EmrcUntagResourceResult(bool Success = true);

public sealed record EmrcListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

public sealed record EmrcCreateSecurityConfigurationResult(
    string? Id = null,
    string? Name = null,
    string? Arn = null);

public sealed record EmrcDescribeSecurityConfigurationResult(
    SecurityConfiguration? SecurityConfiguration = null);

public sealed record EmrcListSecurityConfigurationsResult(
    List<SecurityConfiguration>? SecurityConfigurations = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon EMR on EKS (EMR Containers).
/// </summary>
public static class EmrContainersService
{
    private static AmazonEMRContainersClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEMRContainersClient>(region);

    /// <summary>Create a virtual cluster.</summary>
    public static async Task<EmrcCreateVirtualClusterResult>
        CreateVirtualClusterAsync(
            CreateVirtualClusterRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVirtualClusterAsync(request);
            return new EmrcCreateVirtualClusterResult(
                Id: resp.Id, Name: resp.Name, Arn: resp.Arn);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create EMR Containers virtual cluster");
        }
    }

    /// <summary>Delete a virtual cluster.</summary>
    public static async Task<EmrcDeleteVirtualClusterResult>
        DeleteVirtualClusterAsync(
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteVirtualClusterAsync(
                new DeleteVirtualClusterRequest { Id = id });
            return new EmrcDeleteVirtualClusterResult(Id: resp.Id);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete virtual cluster '{id}'");
        }
    }

    /// <summary>Describe a virtual cluster.</summary>
    public static async Task<EmrcDescribeVirtualClusterResult>
        DescribeVirtualClusterAsync(
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeVirtualClusterAsync(
                new DescribeVirtualClusterRequest { Id = id });
            return new EmrcDescribeVirtualClusterResult(
                VirtualCluster: resp.VirtualCluster);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe virtual cluster '{id}'");
        }
    }

    /// <summary>List virtual clusters.</summary>
    public static async Task<EmrcListVirtualClustersResult>
        ListVirtualClustersAsync(
            string? containerProviderId = null,
            string? containerProviderType = null,
            List<string>? states = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVirtualClustersRequest();
        if (containerProviderId != null)
            request.ContainerProviderId = containerProviderId;
        if (containerProviderType != null)
            request.ContainerProviderType = containerProviderType;
        if (states != null) request.States = states;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListVirtualClustersAsync(request);
            return new EmrcListVirtualClustersResult(
                VirtualClusters: resp.VirtualClusters,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR Containers virtual clusters");
        }
    }

    /// <summary>Start a job run on a virtual cluster.</summary>
    public static async Task<EmrcStartJobRunResult> StartJobRunAsync(
        StartJobRunRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartJobRunAsync(request);
            return new EmrcStartJobRunResult(
                Id: resp.Id,
                Name: resp.Name,
                Arn: resp.Arn,
                VirtualClusterId: resp.VirtualClusterId);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start EMR Containers job run");
        }
    }

    /// <summary>Cancel a job run.</summary>
    public static async Task<EmrcCancelJobRunResult> CancelJobRunAsync(
        string virtualClusterId,
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelJobRunAsync(new CancelJobRunRequest
            {
                VirtualClusterId = virtualClusterId,
                Id = id
            });
            return new EmrcCancelJobRunResult(
                Id: resp.Id,
                VirtualClusterId: resp.VirtualClusterId);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel job run '{id}'");
        }
    }

    /// <summary>Describe a job run.</summary>
    public static async Task<EmrcDescribeJobRunResult> DescribeJobRunAsync(
        string virtualClusterId,
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobRunAsync(new DescribeJobRunRequest
            {
                VirtualClusterId = virtualClusterId,
                Id = id
            });
            return new EmrcDescribeJobRunResult(JobRun: resp.JobRun);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe job run '{id}'");
        }
    }

    /// <summary>List job runs.</summary>
    public static async Task<EmrcListJobRunsResult> ListJobRunsAsync(
        string virtualClusterId,
        string? name = null,
        List<string>? states = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListJobRunsRequest
        {
            VirtualClusterId = virtualClusterId
        };
        if (name != null) request.Name = name;
        if (states != null) request.States = states;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListJobRunsAsync(request);
            return new EmrcListJobRunsResult(
                JobRuns: resp.JobRuns,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list EMR Containers job runs");
        }
    }

    /// <summary>Create a managed endpoint.</summary>
    public static async Task<EmrcCreateManagedEndpointResult>
        CreateManagedEndpointAsync(
            CreateManagedEndpointRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateManagedEndpointAsync(request);
            return new EmrcCreateManagedEndpointResult(
                Id: resp.Id,
                Name: resp.Name,
                Arn: resp.Arn,
                VirtualClusterId: resp.VirtualClusterId);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create managed endpoint");
        }
    }

    /// <summary>Delete a managed endpoint.</summary>
    public static async Task<EmrcDeleteManagedEndpointResult>
        DeleteManagedEndpointAsync(
            string virtualClusterId,
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteManagedEndpointAsync(
                new DeleteManagedEndpointRequest
                {
                    VirtualClusterId = virtualClusterId,
                    Id = id
                });
            return new EmrcDeleteManagedEndpointResult(
                Id: resp.Id,
                VirtualClusterId: resp.VirtualClusterId);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete managed endpoint '{id}'");
        }
    }

    /// <summary>Describe a managed endpoint.</summary>
    public static async Task<EmrcDescribeManagedEndpointResult>
        DescribeManagedEndpointAsync(
            string virtualClusterId,
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeManagedEndpointAsync(
                new DescribeManagedEndpointRequest
                {
                    VirtualClusterId = virtualClusterId,
                    Id = id
                });
            return new EmrcDescribeManagedEndpointResult(Endpoint: resp.Endpoint);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe managed endpoint '{id}'");
        }
    }

    /// <summary>List managed endpoints.</summary>
    public static async Task<EmrcListManagedEndpointsResult>
        ListManagedEndpointsAsync(
            string virtualClusterId,
            List<string>? states = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListManagedEndpointsRequest
        {
            VirtualClusterId = virtualClusterId
        };
        if (states != null) request.States = states;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListManagedEndpointsAsync(request);
            return new EmrcListManagedEndpointsResult(
                Endpoints: resp.Endpoints,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list managed endpoints");
        }
    }

    /// <summary>Create a job template.</summary>
    public static async Task<EmrcCreateJobTemplateResult> CreateJobTemplateAsync(
        CreateJobTemplateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateJobTemplateAsync(request);
            return new EmrcCreateJobTemplateResult(
                Id: resp.Id, Name: resp.Name, Arn: resp.Arn);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create job template");
        }
    }

    /// <summary>Delete a job template.</summary>
    public static async Task<EmrcDeleteJobTemplateResult> DeleteJobTemplateAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteJobTemplateAsync(
                new DeleteJobTemplateRequest { Id = id });
            return new EmrcDeleteJobTemplateResult(Id: resp.Id);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete job template '{id}'");
        }
    }

    /// <summary>Describe a job template.</summary>
    public static async Task<EmrcDescribeJobTemplateResult> DescribeJobTemplateAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobTemplateAsync(
                new DescribeJobTemplateRequest { Id = id });
            return new EmrcDescribeJobTemplateResult(JobTemplate: resp.JobTemplate);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe job template '{id}'");
        }
    }

    /// <summary>List job templates.</summary>
    public static async Task<EmrcListJobTemplatesResult> ListJobTemplatesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListJobTemplatesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListJobTemplatesAsync(request);
            return new EmrcListJobTemplatesResult(
                Templates: resp.Templates,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list job templates");
        }
    }

    /// <summary>Get session credentials for a managed endpoint.</summary>
    public static async Task<EmrcGetManagedEndpointSessionCredentialsResult>
        GetManagedEndpointSessionCredentialsAsync(
            string endpointIdentifier,
            string virtualClusterIdentifier,
            string executionRoleArn,
            string credentialType,
            int? durationInSeconds = null,
            string? logContext = null,
            string? clientToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetManagedEndpointSessionCredentialsRequest
        {
            EndpointIdentifier = endpointIdentifier,
            VirtualClusterIdentifier = virtualClusterIdentifier,
            ExecutionRoleArn = executionRoleArn,
            CredentialType = credentialType
        };
        if (durationInSeconds.HasValue)
            request.DurationInSeconds = durationInSeconds.Value;
        if (logContext != null) request.LogContext = logContext;
        if (clientToken != null) request.ClientToken = clientToken;

        try
        {
            var resp = await client
                .GetManagedEndpointSessionCredentialsAsync(request);
            return new EmrcGetManagedEndpointSessionCredentialsResult(
                Id: resp.Id,
                Credentials: resp.Credentials,
                ExpiresAt: resp.ExpiresAt?.ToString());
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get managed endpoint session credentials");
        }
    }

    /// <summary>Tag an EMR Containers resource.</summary>
    public static async Task<EmrcTagResourceResult> TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
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
            return new EmrcTagResourceResult();
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag EMR Containers resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from an EMR Containers resource.</summary>
    public static async Task<EmrcUntagResourceResult> UntagResourceAsync(
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
            return new EmrcUntagResourceResult();
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag EMR Containers resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for an EMR Containers resource.</summary>
    public static async Task<EmrcListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new EmrcListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for EMR Containers resource '{resourceArn}'");
        }
    }

    /// <summary>Create a security configuration.</summary>
    public static async Task<EmrcCreateSecurityConfigurationResult>
        CreateSecurityConfigurationAsync(
            CreateSecurityConfigurationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateSecurityConfigurationAsync(request);
            return new EmrcCreateSecurityConfigurationResult(
                Id: resp.Id, Name: resp.Name, Arn: resp.Arn);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create security configuration");
        }
    }

    /// <summary>Describe a security configuration.</summary>
    public static async Task<EmrcDescribeSecurityConfigurationResult>
        DescribeSecurityConfigurationAsync(
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSecurityConfigurationAsync(
                new DescribeSecurityConfigurationRequest { Id = id });
            return new EmrcDescribeSecurityConfigurationResult(
                SecurityConfiguration: resp.SecurityConfiguration);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe security configuration '{id}'");
        }
    }

    /// <summary>List security configurations.</summary>
    public static async Task<EmrcListSecurityConfigurationsResult>
        ListSecurityConfigurationsAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSecurityConfigurationsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSecurityConfigurationsAsync(request);
            return new EmrcListSecurityConfigurationsResult(
                SecurityConfigurations: resp.SecurityConfigurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonEMRContainersException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list security configurations");
        }
    }
}
