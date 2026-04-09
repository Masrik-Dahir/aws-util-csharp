using Amazon;
using Amazon.Greengrass;
using Amazon.Greengrass.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateGreengrassGroupResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record GetGreengrassGroupResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record ListGreengrassGroupsResult(
    List<GroupInformation>? Groups = null,
    string? NextToken = null);

public sealed record CreateCoreDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record GetCoreDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record ListCoreDefinitionsResult(
    List<DefinitionInformation>? Definitions = null,
    string? NextToken = null);

public sealed record CreateCoreDefinitionVersionResult(
    string? Id = null,
    string? Arn = null,
    string? Version = null);

public sealed record CreateDeviceDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record GetDeviceDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record ListDeviceDefinitionsResult(
    List<DefinitionInformation>? Definitions = null,
    string? NextToken = null);

public sealed record CreateDeviceDefinitionVersionResult(
    string? Id = null,
    string? Arn = null,
    string? Version = null);

public sealed record CreateFunctionDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record GetFunctionDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record ListFunctionDefinitionsResult(
    List<DefinitionInformation>? Definitions = null,
    string? NextToken = null);

public sealed record CreateFunctionDefinitionVersionResult(
    string? Id = null,
    string? Arn = null,
    string? Version = null);

public sealed record CreateSubscriptionDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record GetSubscriptionDefinitionResult(
    string? Id = null,
    string? Arn = null,
    string? Name = null,
    string? LatestVersion = null,
    string? LatestVersionArn = null);

public sealed record ListSubscriptionDefinitionsResult(
    List<DefinitionInformation>? Definitions = null,
    string? NextToken = null);

public sealed record CreateGreengrassDeploymentResult(
    string? DeploymentArn = null,
    string? DeploymentId = null);

public sealed record GetDeploymentStatusResult(
    string? DeploymentStatus = null,
    string? DeploymentType = null,
    List<ErrorDetail>? ErrorDetails = null,
    string? ErrorMessage = null,
    string? UpdatedAt = null);

public sealed record ListGreengrassDeploymentsResult(
    List<Deployment>? Deployments = null,
    string? NextToken = null);

public sealed record ResetDeploymentsResult(
    string? DeploymentArn = null,
    string? DeploymentId = null);

public sealed record CreateGroupVersionResult(
    string? Id = null,
    string? Arn = null,
    string? Version = null);

public sealed record ListGroupVersionsResult(
    List<VersionInformation>? Versions = null,
    string? NextToken = null);

public sealed record GetGroupVersionResult(
    string? Id = null,
    string? Arn = null,
    string? Version = null,
    GroupVersion? Definition = null);

public sealed record GetAssociatedRoleResult(
    string? RoleArn = null,
    string? AssociatedAt = null);

public sealed record GreengrassListTagsResult(
    Dictionary<string, string>? Tags = null);

/// <summary>
/// Utility helpers for AWS IoT Greengrass V1.
/// </summary>
public static class IoTGreengrassService
{
    private static AmazonGreengrassClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonGreengrassClient>(region);

    // ── Group operations ────────────────────────────────────────────

    /// <summary>
    /// Create a Greengrass group.
    /// </summary>
    public static async Task<CreateGreengrassGroupResult> CreateGroupAsync(
        string name,
        GroupVersion? initialVersion = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGroupRequest { Name = name };
        if (initialVersion != null) request.InitialVersion = initialVersion;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateGroupAsync(request);
            return new CreateGreengrassGroupResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Greengrass group '{name}'");
        }
    }

    /// <summary>
    /// Delete a Greengrass group.
    /// </summary>
    public static async Task DeleteGroupAsync(
        string groupId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteGroupAsync(
                new DeleteGroupRequest { GroupId = groupId });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Greengrass group '{groupId}'");
        }
    }

    /// <summary>
    /// Get a Greengrass group.
    /// </summary>
    public static async Task<GetGreengrassGroupResult> GetGroupAsync(
        string groupId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGroupAsync(
                new GetGroupRequest { GroupId = groupId });
            return new GetGreengrassGroupResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Greengrass group '{groupId}'");
        }
    }

    /// <summary>
    /// List Greengrass groups with optional pagination.
    /// </summary>
    public static async Task<ListGreengrassGroupsResult> ListGroupsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListGroupsAsync(request);
            return new ListGreengrassGroupsResult(
                Groups: resp.Groups,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Greengrass groups");
        }
    }

    // ── Core Definition operations ──────────────────────────────────

    /// <summary>
    /// Create a Greengrass core definition.
    /// </summary>
    public static async Task<CreateCoreDefinitionResult> CreateCoreDefinitionAsync(
        string name,
        CoreDefinitionVersion? initialVersion = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCoreDefinitionRequest { Name = name };
        if (initialVersion != null) request.InitialVersion = initialVersion;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCoreDefinitionAsync(request);
            return new CreateCoreDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create core definition '{name}'");
        }
    }

    /// <summary>
    /// Delete a Greengrass core definition.
    /// </summary>
    public static async Task DeleteCoreDefinitionAsync(
        string coreDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCoreDefinitionAsync(
                new DeleteCoreDefinitionRequest
                {
                    CoreDefinitionId = coreDefinitionId
                });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete core definition '{coreDefinitionId}'");
        }
    }

    /// <summary>
    /// Get a Greengrass core definition.
    /// </summary>
    public static async Task<GetCoreDefinitionResult> GetCoreDefinitionAsync(
        string coreDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCoreDefinitionAsync(
                new GetCoreDefinitionRequest
                {
                    CoreDefinitionId = coreDefinitionId
                });
            return new GetCoreDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get core definition '{coreDefinitionId}'");
        }
    }

    /// <summary>
    /// List Greengrass core definitions with optional pagination.
    /// </summary>
    public static async Task<ListCoreDefinitionsResult> ListCoreDefinitionsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCoreDefinitionsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListCoreDefinitionsAsync(request);
            return new ListCoreDefinitionsResult(
                Definitions: resp.Definitions,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list core definitions");
        }
    }

    /// <summary>
    /// Create a new version of a Greengrass core definition.
    /// </summary>
    public static async Task<CreateCoreDefinitionVersionResult>
        CreateCoreDefinitionVersionAsync(
        string coreDefinitionId,
        List<Core> cores,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateCoreDefinitionVersionAsync(
                new CreateCoreDefinitionVersionRequest
                {
                    CoreDefinitionId = coreDefinitionId,
                    Cores = cores
                });
            return new CreateCoreDefinitionVersionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Version: resp.Version);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create core definition version for '{coreDefinitionId}'");
        }
    }

    // ── Device Definition operations ────────────────────────────────

    /// <summary>
    /// Create a Greengrass device definition.
    /// </summary>
    public static async Task<CreateDeviceDefinitionResult> CreateDeviceDefinitionAsync(
        string name,
        DeviceDefinitionVersion? initialVersion = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDeviceDefinitionRequest { Name = name };
        if (initialVersion != null) request.InitialVersion = initialVersion;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDeviceDefinitionAsync(request);
            return new CreateDeviceDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create device definition '{name}'");
        }
    }

    /// <summary>
    /// Delete a Greengrass device definition.
    /// </summary>
    public static async Task DeleteDeviceDefinitionAsync(
        string deviceDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDeviceDefinitionAsync(
                new DeleteDeviceDefinitionRequest
                {
                    DeviceDefinitionId = deviceDefinitionId
                });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete device definition '{deviceDefinitionId}'");
        }
    }

    /// <summary>
    /// Get a Greengrass device definition.
    /// </summary>
    public static async Task<GetDeviceDefinitionResult> GetDeviceDefinitionAsync(
        string deviceDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeviceDefinitionAsync(
                new GetDeviceDefinitionRequest
                {
                    DeviceDefinitionId = deviceDefinitionId
                });
            return new GetDeviceDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get device definition '{deviceDefinitionId}'");
        }
    }

    /// <summary>
    /// List Greengrass device definitions with optional pagination.
    /// </summary>
    public static async Task<ListDeviceDefinitionsResult> ListDeviceDefinitionsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeviceDefinitionsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDeviceDefinitionsAsync(request);
            return new ListDeviceDefinitionsResult(
                Definitions: resp.Definitions,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list device definitions");
        }
    }

    /// <summary>
    /// Create a new version of a Greengrass device definition.
    /// </summary>
    public static async Task<CreateDeviceDefinitionVersionResult>
        CreateDeviceDefinitionVersionAsync(
        string deviceDefinitionId,
        List<Device> devices,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDeviceDefinitionVersionAsync(
                new CreateDeviceDefinitionVersionRequest
                {
                    DeviceDefinitionId = deviceDefinitionId,
                    Devices = devices
                });
            return new CreateDeviceDefinitionVersionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Version: resp.Version);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create device definition version for '{deviceDefinitionId}'");
        }
    }

    // ── Function Definition operations ──────────────────────────────

    /// <summary>
    /// Create a Greengrass function definition.
    /// </summary>
    public static async Task<CreateFunctionDefinitionResult>
        CreateFunctionDefinitionAsync(
        string name,
        FunctionDefinitionVersion? initialVersion = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFunctionDefinitionRequest { Name = name };
        if (initialVersion != null) request.InitialVersion = initialVersion;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateFunctionDefinitionAsync(request);
            return new CreateFunctionDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create function definition '{name}'");
        }
    }

    /// <summary>
    /// Delete a Greengrass function definition.
    /// </summary>
    public static async Task DeleteFunctionDefinitionAsync(
        string functionDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFunctionDefinitionAsync(
                new DeleteFunctionDefinitionRequest
                {
                    FunctionDefinitionId = functionDefinitionId
                });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete function definition '{functionDefinitionId}'");
        }
    }

    /// <summary>
    /// Get a Greengrass function definition.
    /// </summary>
    public static async Task<GetFunctionDefinitionResult> GetFunctionDefinitionAsync(
        string functionDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFunctionDefinitionAsync(
                new GetFunctionDefinitionRequest
                {
                    FunctionDefinitionId = functionDefinitionId
                });
            return new GetFunctionDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get function definition '{functionDefinitionId}'");
        }
    }

    /// <summary>
    /// List Greengrass function definitions with optional pagination.
    /// </summary>
    public static async Task<ListFunctionDefinitionsResult>
        ListFunctionDefinitionsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFunctionDefinitionsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListFunctionDefinitionsAsync(request);
            return new ListFunctionDefinitionsResult(
                Definitions: resp.Definitions,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list function definitions");
        }
    }

    /// <summary>
    /// Create a new version of a Greengrass function definition.
    /// </summary>
    public static async Task<CreateFunctionDefinitionVersionResult>
        CreateFunctionDefinitionVersionAsync(
        string functionDefinitionId,
        List<Function>? functions = null,
        FunctionDefaultConfig? defaultConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFunctionDefinitionVersionRequest
        {
            FunctionDefinitionId = functionDefinitionId
        };
        if (functions != null) request.Functions = functions;
        if (defaultConfig != null) request.DefaultConfig = defaultConfig;

        try
        {
            var resp = await client.CreateFunctionDefinitionVersionAsync(request);
            return new CreateFunctionDefinitionVersionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Version: resp.Version);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create function definition version for '{functionDefinitionId}'");
        }
    }

    // ── Subscription Definition operations ──────────────────────────

    /// <summary>
    /// Create a Greengrass subscription definition.
    /// </summary>
    public static async Task<CreateSubscriptionDefinitionResult>
        CreateSubscriptionDefinitionAsync(
        string name,
        SubscriptionDefinitionVersion? initialVersion = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSubscriptionDefinitionRequest { Name = name };
        if (initialVersion != null) request.InitialVersion = initialVersion;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSubscriptionDefinitionAsync(request);
            return new CreateSubscriptionDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create subscription definition '{name}'");
        }
    }

    /// <summary>
    /// Delete a Greengrass subscription definition.
    /// </summary>
    public static async Task DeleteSubscriptionDefinitionAsync(
        string subscriptionDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSubscriptionDefinitionAsync(
                new DeleteSubscriptionDefinitionRequest
                {
                    SubscriptionDefinitionId = subscriptionDefinitionId
                });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete subscription definition '{subscriptionDefinitionId}'");
        }
    }

    /// <summary>
    /// Get a Greengrass subscription definition.
    /// </summary>
    public static async Task<GetSubscriptionDefinitionResult>
        GetSubscriptionDefinitionAsync(
        string subscriptionDefinitionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSubscriptionDefinitionAsync(
                new GetSubscriptionDefinitionRequest
                {
                    SubscriptionDefinitionId = subscriptionDefinitionId
                });
            return new GetSubscriptionDefinitionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Name: resp.Name,
                LatestVersion: resp.LatestVersion,
                LatestVersionArn: resp.LatestVersionArn);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get subscription definition '{subscriptionDefinitionId}'");
        }
    }

    /// <summary>
    /// List Greengrass subscription definitions with optional pagination.
    /// </summary>
    public static async Task<ListSubscriptionDefinitionsResult>
        ListSubscriptionDefinitionsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSubscriptionDefinitionsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListSubscriptionDefinitionsAsync(request);
            return new ListSubscriptionDefinitionsResult(
                Definitions: resp.Definitions,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list subscription definitions");
        }
    }

    // ── Deployment operations ───────────────────────────────────────

    /// <summary>
    /// Create a Greengrass deployment.
    /// </summary>
    public static async Task<CreateGreengrassDeploymentResult> CreateDeploymentAsync(
        string groupId,
        DeploymentType deploymentType,
        string? groupVersionId = null,
        string? deploymentId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDeploymentRequest
        {
            GroupId = groupId,
            DeploymentType = deploymentType
        };
        if (groupVersionId != null) request.GroupVersionId = groupVersionId;
        if (deploymentId != null) request.DeploymentId = deploymentId;

        try
        {
            var resp = await client.CreateDeploymentAsync(request);
            return new CreateGreengrassDeploymentResult(
                DeploymentArn: resp.DeploymentArn,
                DeploymentId: resp.DeploymentId);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create deployment for group '{groupId}'");
        }
    }

    /// <summary>
    /// Get the status of a Greengrass deployment.
    /// </summary>
    public static async Task<GetDeploymentStatusResult> GetDeploymentStatusAsync(
        string groupId,
        string deploymentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDeploymentStatusAsync(
                new GetDeploymentStatusRequest
                {
                    GroupId = groupId,
                    DeploymentId = deploymentId
                });
            return new GetDeploymentStatusResult(
                DeploymentStatus: resp.DeploymentStatus,
                DeploymentType: resp.DeploymentType,
                ErrorDetails: resp.ErrorDetails,
                ErrorMessage: resp.ErrorMessage,
                UpdatedAt: resp.UpdatedAt);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get deployment status for '{deploymentId}'");
        }
    }

    /// <summary>
    /// List Greengrass deployments for a group.
    /// </summary>
    public static async Task<ListGreengrassDeploymentsResult> ListDeploymentsAsync(
        string groupId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeploymentsRequest { GroupId = groupId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDeploymentsAsync(request);
            return new ListGreengrassDeploymentsResult(
                Deployments: resp.Deployments,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list deployments for group '{groupId}'");
        }
    }

    /// <summary>
    /// Reset deployments for a Greengrass group.
    /// </summary>
    public static async Task<ResetDeploymentsResult> ResetDeploymentsAsync(
        string groupId,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ResetDeploymentsRequest { GroupId = groupId };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.ResetDeploymentsAsync(request);
            return new ResetDeploymentsResult(
                DeploymentArn: resp.DeploymentArn,
                DeploymentId: resp.DeploymentId);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reset deployments for group '{groupId}'");
        }
    }

    // ── Group Version operations ────────────────────────────────────

    /// <summary>
    /// Create a new version of a Greengrass group.
    /// </summary>
    public static async Task<CreateGroupVersionResult> CreateGroupVersionAsync(
        string groupId,
        string? coreDefinitionVersionArn = null,
        string? deviceDefinitionVersionArn = null,
        string? functionDefinitionVersionArn = null,
        string? subscriptionDefinitionVersionArn = null,
        string? loggerDefinitionVersionArn = null,
        string? resourceDefinitionVersionArn = null,
        string? connectorDefinitionVersionArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateGroupVersionRequest { GroupId = groupId };
        if (coreDefinitionVersionArn != null)
            request.CoreDefinitionVersionArn = coreDefinitionVersionArn;
        if (deviceDefinitionVersionArn != null)
            request.DeviceDefinitionVersionArn = deviceDefinitionVersionArn;
        if (functionDefinitionVersionArn != null)
            request.FunctionDefinitionVersionArn = functionDefinitionVersionArn;
        if (subscriptionDefinitionVersionArn != null)
            request.SubscriptionDefinitionVersionArn = subscriptionDefinitionVersionArn;
        if (loggerDefinitionVersionArn != null)
            request.LoggerDefinitionVersionArn = loggerDefinitionVersionArn;
        if (resourceDefinitionVersionArn != null)
            request.ResourceDefinitionVersionArn = resourceDefinitionVersionArn;
        if (connectorDefinitionVersionArn != null)
            request.ConnectorDefinitionVersionArn = connectorDefinitionVersionArn;

        try
        {
            var resp = await client.CreateGroupVersionAsync(request);
            return new CreateGroupVersionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Version: resp.Version);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create group version for '{groupId}'");
        }
    }

    /// <summary>
    /// List versions of a Greengrass group.
    /// </summary>
    public static async Task<ListGroupVersionsResult> ListGroupVersionsAsync(
        string groupId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGroupVersionsRequest { GroupId = groupId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListGroupVersionsAsync(request);
            return new ListGroupVersionsResult(
                Versions: resp.Versions,
                NextToken: resp.NextToken);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list group versions for '{groupId}'");
        }
    }

    /// <summary>
    /// Get a specific version of a Greengrass group.
    /// </summary>
    public static async Task<GetGroupVersionResult> GetGroupVersionAsync(
        string groupId,
        string groupVersionId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetGroupVersionAsync(
                new GetGroupVersionRequest
                {
                    GroupId = groupId,
                    GroupVersionId = groupVersionId
                });
            return new GetGroupVersionResult(
                Id: resp.Id,
                Arn: resp.Arn,
                Version: resp.Version,
                Definition: resp.Definition);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get group version '{groupVersionId}'");
        }
    }

    // ── Role operations ─────────────────────────────────────────────

    /// <summary>
    /// Associate a role with a Greengrass group.
    /// </summary>
    public static async Task AssociateRoleToGroupAsync(
        string groupId,
        string roleArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateRoleToGroupAsync(
                new AssociateRoleToGroupRequest
                {
                    GroupId = groupId,
                    RoleArn = roleArn
                });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate role to group '{groupId}'");
        }
    }

    /// <summary>
    /// Disassociate a role from a Greengrass group.
    /// </summary>
    public static async Task DisassociateRoleFromGroupAsync(
        string groupId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateRoleFromGroupAsync(
                new DisassociateRoleFromGroupRequest { GroupId = groupId });
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate role from group '{groupId}'");
        }
    }

    /// <summary>
    /// Get the role associated with a Greengrass group.
    /// </summary>
    public static async Task<GetAssociatedRoleResult> GetAssociatedRoleAsync(
        string groupId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAssociatedRoleAsync(
                new GetAssociatedRoleRequest { GroupId = groupId });
            return new GetAssociatedRoleResult(
                RoleArn: resp.RoleArn,
                AssociatedAt: resp.AssociatedAt);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get associated role for group '{groupId}'");
        }
    }

    // ── Tagging operations ──────────────────────────────────────────

    /// <summary>
    /// Add tags to a Greengrass resource.
    /// </summary>
    public static async Task TagResourceAsync(
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
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Greengrass resource.
    /// </summary>
    public static async Task UntagResourceAsync(
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
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Greengrass resource.
    /// </summary>
    public static async Task<GreengrassListTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new GreengrassListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonGreengrassException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateGroupAsync"/>.</summary>
    public static CreateGreengrassGroupResult CreateGroup(string name, GroupVersion? initialVersion = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateGroupAsync(name, initialVersion, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteGroupAsync"/>.</summary>
    public static void DeleteGroup(string groupId, RegionEndpoint? region = null)
        => DeleteGroupAsync(groupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetGroupAsync"/>.</summary>
    public static GetGreengrassGroupResult GetGroup(string groupId, RegionEndpoint? region = null)
        => GetGroupAsync(groupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGroupsAsync"/>.</summary>
    public static ListGreengrassGroupsResult ListGroups(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListGroupsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCoreDefinitionAsync"/>.</summary>
    public static CreateCoreDefinitionResult CreateCoreDefinition(string name, CoreDefinitionVersion? initialVersion = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateCoreDefinitionAsync(name, initialVersion, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCoreDefinitionAsync"/>.</summary>
    public static void DeleteCoreDefinition(string coreDefinitionId, RegionEndpoint? region = null)
        => DeleteCoreDefinitionAsync(coreDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCoreDefinitionAsync"/>.</summary>
    public static GetCoreDefinitionResult GetCoreDefinition(string coreDefinitionId, RegionEndpoint? region = null)
        => GetCoreDefinitionAsync(coreDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCoreDefinitionsAsync"/>.</summary>
    public static ListCoreDefinitionsResult ListCoreDefinitions(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListCoreDefinitionsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCoreDefinitionVersionAsync"/>.</summary>
    public static CreateCoreDefinitionVersionResult CreateCoreDefinitionVersion(string coreDefinitionId, List<Core> cores, RegionEndpoint? region = null)
        => CreateCoreDefinitionVersionAsync(coreDefinitionId, cores, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeviceDefinitionAsync"/>.</summary>
    public static CreateDeviceDefinitionResult CreateDeviceDefinition(string name, DeviceDefinitionVersion? initialVersion = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateDeviceDefinitionAsync(name, initialVersion, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDeviceDefinitionAsync"/>.</summary>
    public static void DeleteDeviceDefinition(string deviceDefinitionId, RegionEndpoint? region = null)
        => DeleteDeviceDefinitionAsync(deviceDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeviceDefinitionAsync"/>.</summary>
    public static GetDeviceDefinitionResult GetDeviceDefinition(string deviceDefinitionId, RegionEndpoint? region = null)
        => GetDeviceDefinitionAsync(deviceDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeviceDefinitionsAsync"/>.</summary>
    public static ListDeviceDefinitionsResult ListDeviceDefinitions(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListDeviceDefinitionsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeviceDefinitionVersionAsync"/>.</summary>
    public static CreateDeviceDefinitionVersionResult CreateDeviceDefinitionVersion(string deviceDefinitionId, List<Device> devices, RegionEndpoint? region = null)
        => CreateDeviceDefinitionVersionAsync(deviceDefinitionId, devices, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateFunctionDefinitionAsync"/>.</summary>
    public static CreateFunctionDefinitionResult CreateFunctionDefinition(string name, FunctionDefinitionVersion? initialVersion = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateFunctionDefinitionAsync(name, initialVersion, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFunctionDefinitionAsync"/>.</summary>
    public static void DeleteFunctionDefinition(string functionDefinitionId, RegionEndpoint? region = null)
        => DeleteFunctionDefinitionAsync(functionDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFunctionDefinitionAsync"/>.</summary>
    public static GetFunctionDefinitionResult GetFunctionDefinition(string functionDefinitionId, RegionEndpoint? region = null)
        => GetFunctionDefinitionAsync(functionDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFunctionDefinitionsAsync"/>.</summary>
    public static ListFunctionDefinitionsResult ListFunctionDefinitions(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListFunctionDefinitionsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateFunctionDefinitionVersionAsync"/>.</summary>
    public static CreateFunctionDefinitionVersionResult CreateFunctionDefinitionVersion(string functionDefinitionId, List<Function>? functions = null, FunctionDefaultConfig? defaultConfig = null, RegionEndpoint? region = null)
        => CreateFunctionDefinitionVersionAsync(functionDefinitionId, functions, defaultConfig, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSubscriptionDefinitionAsync"/>.</summary>
    public static CreateSubscriptionDefinitionResult CreateSubscriptionDefinition(string name, SubscriptionDefinitionVersion? initialVersion = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateSubscriptionDefinitionAsync(name, initialVersion, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSubscriptionDefinitionAsync"/>.</summary>
    public static void DeleteSubscriptionDefinition(string subscriptionDefinitionId, RegionEndpoint? region = null)
        => DeleteSubscriptionDefinitionAsync(subscriptionDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSubscriptionDefinitionAsync"/>.</summary>
    public static GetSubscriptionDefinitionResult GetSubscriptionDefinition(string subscriptionDefinitionId, RegionEndpoint? region = null)
        => GetSubscriptionDefinitionAsync(subscriptionDefinitionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSubscriptionDefinitionsAsync"/>.</summary>
    public static ListSubscriptionDefinitionsResult ListSubscriptionDefinitions(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListSubscriptionDefinitionsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDeploymentAsync"/>.</summary>
    public static CreateGreengrassDeploymentResult CreateDeployment(string groupId, DeploymentType deploymentType, string? groupVersionId = null, string? deploymentId = null, RegionEndpoint? region = null)
        => CreateDeploymentAsync(groupId, deploymentType, groupVersionId, deploymentId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDeploymentStatusAsync"/>.</summary>
    public static GetDeploymentStatusResult GetDeploymentStatus(string groupId, string deploymentId, RegionEndpoint? region = null)
        => GetDeploymentStatusAsync(groupId, deploymentId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeploymentsAsync"/>.</summary>
    public static ListGreengrassDeploymentsResult ListDeployments(string groupId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListDeploymentsAsync(groupId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ResetDeploymentsAsync"/>.</summary>
    public static ResetDeploymentsResult ResetDeployments(string groupId, bool? force = null, RegionEndpoint? region = null)
        => ResetDeploymentsAsync(groupId, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateGroupVersionAsync"/>.</summary>
    public static CreateGroupVersionResult CreateGroupVersion(string groupId, string? coreDefinitionVersionArn = null, string? deviceDefinitionVersionArn = null, string? functionDefinitionVersionArn = null, string? subscriptionDefinitionVersionArn = null, string? loggerDefinitionVersionArn = null, string? resourceDefinitionVersionArn = null, string? connectorDefinitionVersionArn = null, RegionEndpoint? region = null)
        => CreateGroupVersionAsync(groupId, coreDefinitionVersionArn, deviceDefinitionVersionArn, functionDefinitionVersionArn, subscriptionDefinitionVersionArn, loggerDefinitionVersionArn, resourceDefinitionVersionArn, connectorDefinitionVersionArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGroupVersionsAsync"/>.</summary>
    public static ListGroupVersionsResult ListGroupVersions(string groupId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListGroupVersionsAsync(groupId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetGroupVersionAsync"/>.</summary>
    public static GetGroupVersionResult GetGroupVersion(string groupId, string groupVersionId, RegionEndpoint? region = null)
        => GetGroupVersionAsync(groupId, groupVersionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateRoleToGroupAsync"/>.</summary>
    public static void AssociateRoleToGroup(string groupId, string roleArn, RegionEndpoint? region = null)
        => AssociateRoleToGroupAsync(groupId, roleArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateRoleFromGroupAsync"/>.</summary>
    public static void DisassociateRoleFromGroup(string groupId, RegionEndpoint? region = null)
        => DisassociateRoleFromGroupAsync(groupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAssociatedRoleAsync"/>.</summary>
    public static GetAssociatedRoleResult GetAssociatedRole(string groupId, RegionEndpoint? region = null)
        => GetAssociatedRoleAsync(groupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static GreengrassListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}
