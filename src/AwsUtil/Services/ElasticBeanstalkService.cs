using Amazon;
using Amazon.ElasticBeanstalk;
using Amazon.ElasticBeanstalk.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record BeanstalkApplicationInfo(
    string? ApplicationName = null,
    string? ApplicationArn = null,
    string? Description = null,
    DateTime? DateCreated = null,
    DateTime? DateUpdated = null);

public sealed record BeanstalkApplicationVersionInfo(
    string? ApplicationName = null,
    string? VersionLabel = null,
    string? ApplicationVersionArn = null,
    string? Description = null,
    string? Status = null,
    DateTime? DateCreated = null,
    DateTime? DateUpdated = null);

public sealed record BeanstalkEnvironmentInfo(
    string? EnvironmentName = null,
    string? EnvironmentId = null,
    string? EnvironmentArn = null,
    string? ApplicationName = null,
    string? VersionLabel = null,
    string? SolutionStackName = null,
    string? PlatformArn = null,
    string? Status = null,
    string? Health = null,
    string? HealthStatus = null,
    string? CNAME = null,
    string? EndpointURL = null,
    DateTime? DateCreated = null,
    DateTime? DateUpdated = null);

public sealed record BeanstalkEnvironmentResourceInfo(
    string? EnvironmentName = null,
    List<string>? AutoScalingGroups = null,
    List<string>? Instances = null,
    List<string>? LaunchConfigurations = null,
    List<string>? LoadBalancers = null,
    List<string>? Queues = null,
    List<string>? Triggers = null);

public sealed record BeanstalkEnvironmentHealthInfo(
    string? EnvironmentName = null,
    string? HealthStatus = null,
    string? Status = null,
    string? Color = null,
    DateTime? RefreshedAt = null);

public sealed record BeanstalkInstanceHealthInfo(
    string? InstanceId = null,
    string? HealthStatus = null,
    string? Color = null,
    string? InstanceType = null,
    string? AvailabilityZone = null,
    DateTime? LaunchedAt = null);

public sealed record BeanstalkConfigSettingsInfo(
    string? SolutionStackName = null,
    string? PlatformArn = null,
    string? ApplicationName = null,
    string? TemplateName = null,
    string? EnvironmentName = null,
    string? Description = null,
    List<ConfigurationOptionSetting>? OptionSettings = null,
    DateTime? DateCreated = null,
    DateTime? DateUpdated = null);

public sealed record BeanstalkConfigOptionInfo(
    string? Namespace = null,
    string? Name = null,
    string? DefaultValue = null,
    string? ChangeSeverity = null,
    bool? UserDefined = null,
    string? ValueType = null);

public sealed record BeanstalkValidationMessage(
    string? Message = null,
    string? Severity = null,
    string? Namespace = null,
    string? OptionName = null);

public sealed record BeanstalkSolutionStackInfo(
    string? SolutionStackName = null,
    List<string>? PermittedFileTypes = null);

public sealed record BeanstalkPlatformSummary(
    string? PlatformArn = null,
    string? PlatformStatus = null,
    string? PlatformCategory = null,
    string? OperatingSystemName = null,
    string? OperatingSystemVersion = null);

public sealed record BeanstalkPlatformDescription(
    string? PlatformArn = null,
    string? PlatformName = null,
    string? PlatformVersion = null,
    string? PlatformStatus = null,
    string? PlatformCategory = null,
    string? OperatingSystemName = null,
    string? OperatingSystemVersion = null,
    string? SolutionStackName = null,
    string? Description = null,
    DateTime? DateCreated = null,
    DateTime? DateUpdated = null);

public sealed record BeanstalkStorageLocationResult(
    string? S3Bucket = null);

public sealed record BeanstalkEventInfo(
    string? EventDate = null,
    string? Message = null,
    string? ApplicationName = null,
    string? EnvironmentName = null,
    string? VersionLabel = null,
    string? Severity = null);

public sealed record BeanstalkTagResult(bool Success = true);

/// <summary>
/// Utility helpers for AWS Elastic Beanstalk.
/// </summary>
public static class ElasticBeanstalkService
{
    private static AmazonElasticBeanstalkClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonElasticBeanstalkClient>(region);

    // ── Application operations ───────────────────────────────────────

    /// <summary>
    /// Create a new Elastic Beanstalk application.
    /// </summary>
    public static async Task<BeanstalkApplicationInfo> CreateApplicationAsync(
        string applicationName,
        string? description = null,
        List<Amazon.ElasticBeanstalk.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateApplicationAsync(request);
            var a = resp.Application;
            return new BeanstalkApplicationInfo(
                ApplicationName: a.ApplicationName,
                ApplicationArn: a.ApplicationArn,
                Description: a.Description,
                DateCreated: a.DateCreated,
                DateUpdated: a.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create application '{applicationName}'");
        }
    }

    /// <summary>
    /// Delete an Elastic Beanstalk application.
    /// </summary>
    public static async Task DeleteApplicationAsync(
        string applicationName,
        bool? terminateEnvByForce = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (terminateEnvByForce.HasValue)
            request.TerminateEnvByForce = terminateEnvByForce.Value;

        try
        {
            await client.DeleteApplicationAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete application '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe Elastic Beanstalk applications.
    /// </summary>
    public static async Task<List<BeanstalkApplicationInfo>> DescribeApplicationsAsync(
        List<string>? applicationNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeApplicationsRequest();
        if (applicationNames != null) request.ApplicationNames = applicationNames;

        try
        {
            var resp = await client.DescribeApplicationsAsync(request);
            return resp.Applications.Select(a => new BeanstalkApplicationInfo(
                ApplicationName: a.ApplicationName,
                ApplicationArn: a.ApplicationArn,
                Description: a.Description,
                DateCreated: a.DateCreated,
                DateUpdated: a.DateUpdated)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe applications");
        }
    }

    /// <summary>
    /// Update an Elastic Beanstalk application.
    /// </summary>
    public static async Task<BeanstalkApplicationInfo> UpdateApplicationAsync(
        string applicationName,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateApplicationRequest
        {
            ApplicationName = applicationName
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateApplicationAsync(request);
            var a = resp.Application;
            return new BeanstalkApplicationInfo(
                ApplicationName: a.ApplicationName,
                ApplicationArn: a.ApplicationArn,
                Description: a.Description,
                DateCreated: a.DateCreated,
                DateUpdated: a.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update application '{applicationName}'");
        }
    }

    // ── Application Version operations ───────────────────────────────

    /// <summary>
    /// Create an application version.
    /// </summary>
    public static async Task<BeanstalkApplicationVersionInfo>
        CreateApplicationVersionAsync(
            CreateApplicationVersionRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateApplicationVersionAsync(request);
            var v = resp.ApplicationVersion;
            return new BeanstalkApplicationVersionInfo(
                ApplicationName: v.ApplicationName,
                VersionLabel: v.VersionLabel,
                ApplicationVersionArn: v.ApplicationVersionArn,
                Description: v.Description,
                Status: v.Status?.Value,
                DateCreated: v.DateCreated,
                DateUpdated: v.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create application version");
        }
    }

    /// <summary>
    /// Delete an application version.
    /// </summary>
    public static async Task DeleteApplicationVersionAsync(
        string applicationName,
        string versionLabel,
        bool? deleteSourceBundle = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteApplicationVersionRequest
        {
            ApplicationName = applicationName,
            VersionLabel = versionLabel
        };
        if (deleteSourceBundle.HasValue)
            request.DeleteSourceBundle = deleteSourceBundle.Value;

        try
        {
            await client.DeleteApplicationVersionAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete version '{versionLabel}' of application '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe application versions.
    /// </summary>
    public static async Task<List<BeanstalkApplicationVersionInfo>>
        DescribeApplicationVersionsAsync(
            string? applicationName = null,
            List<string>? versionLabels = null,
            int? maxRecords = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeApplicationVersionsRequest();
        if (applicationName != null) request.ApplicationName = applicationName;
        if (versionLabels != null) request.VersionLabels = versionLabels;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeApplicationVersionsAsync(request);
            return resp.ApplicationVersions.Select(v =>
                new BeanstalkApplicationVersionInfo(
                    ApplicationName: v.ApplicationName,
                    VersionLabel: v.VersionLabel,
                    ApplicationVersionArn: v.ApplicationVersionArn,
                    Description: v.Description,
                    Status: v.Status?.Value,
                    DateCreated: v.DateCreated,
                    DateUpdated: v.DateUpdated)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe application versions");
        }
    }

    /// <summary>
    /// Update an application version.
    /// </summary>
    public static async Task<BeanstalkApplicationVersionInfo>
        UpdateApplicationVersionAsync(
            string applicationName,
            string versionLabel,
            string? description = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateApplicationVersionRequest
        {
            ApplicationName = applicationName,
            VersionLabel = versionLabel
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateApplicationVersionAsync(request);
            var v = resp.ApplicationVersion;
            return new BeanstalkApplicationVersionInfo(
                ApplicationName: v.ApplicationName,
                VersionLabel: v.VersionLabel,
                ApplicationVersionArn: v.ApplicationVersionArn,
                Description: v.Description,
                Status: v.Status?.Value,
                DateCreated: v.DateCreated,
                DateUpdated: v.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update version '{versionLabel}'");
        }
    }

    // ── Environment operations ───────────────────────────────────────

    /// <summary>
    /// Create an environment.
    /// </summary>
    public static async Task<BeanstalkEnvironmentInfo> CreateEnvironmentAsync(
        CreateEnvironmentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateEnvironmentAsync(request);
            return new BeanstalkEnvironmentInfo(
                EnvironmentName: resp.EnvironmentName,
                EnvironmentId: resp.EnvironmentId,
                EnvironmentArn: resp.EnvironmentArn,
                ApplicationName: resp.ApplicationName,
                VersionLabel: resp.VersionLabel,
                SolutionStackName: resp.SolutionStackName,
                PlatformArn: resp.PlatformArn,
                Status: resp.Status?.Value,
                Health: resp.Health?.Value,
                HealthStatus: resp.HealthStatus?.Value,
                CNAME: resp.CNAME,
                EndpointURL: resp.EndpointURL,
                DateCreated: resp.DateCreated,
                DateUpdated: resp.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create environment");
        }
    }

    /// <summary>
    /// Terminate an environment.
    /// </summary>
    public static async Task<BeanstalkEnvironmentInfo> TerminateEnvironmentAsync(
        string? environmentId = null,
        string? environmentName = null,
        bool? terminateResources = null,
        bool? forceTerminate = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TerminateEnvironmentRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;
        if (terminateResources.HasValue)
            request.TerminateResources = terminateResources.Value;
        if (forceTerminate.HasValue) request.ForceTerminate = forceTerminate.Value;

        try
        {
            var resp = await client.TerminateEnvironmentAsync(request);
            return new BeanstalkEnvironmentInfo(
                EnvironmentName: resp.EnvironmentName,
                EnvironmentId: resp.EnvironmentId,
                EnvironmentArn: resp.EnvironmentArn,
                ApplicationName: resp.ApplicationName,
                VersionLabel: resp.VersionLabel,
                SolutionStackName: resp.SolutionStackName,
                PlatformArn: resp.PlatformArn,
                Status: resp.Status?.Value,
                Health: resp.Health?.Value,
                HealthStatus: resp.HealthStatus?.Value,
                CNAME: resp.CNAME,
                EndpointURL: resp.EndpointURL,
                DateCreated: resp.DateCreated,
                DateUpdated: resp.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to terminate environment '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Describe environments.
    /// </summary>
    public static async Task<List<BeanstalkEnvironmentInfo>>
        DescribeEnvironmentsAsync(
            DescribeEnvironmentsRequest? request = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEnvironmentsRequest();

        try
        {
            var resp = await client.DescribeEnvironmentsAsync(request);
            return resp.Environments.Select(e => new BeanstalkEnvironmentInfo(
                EnvironmentName: e.EnvironmentName,
                EnvironmentId: e.EnvironmentId,
                EnvironmentArn: e.EnvironmentArn,
                ApplicationName: e.ApplicationName,
                VersionLabel: e.VersionLabel,
                SolutionStackName: e.SolutionStackName,
                PlatformArn: e.PlatformArn,
                Status: e.Status?.Value,
                Health: e.Health?.Value,
                HealthStatus: e.HealthStatus?.Value,
                CNAME: e.CNAME,
                EndpointURL: e.EndpointURL,
                DateCreated: e.DateCreated,
                DateUpdated: e.DateUpdated)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe environments");
        }
    }

    /// <summary>
    /// Update an environment.
    /// </summary>
    public static async Task<BeanstalkEnvironmentInfo> UpdateEnvironmentAsync(
        UpdateEnvironmentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateEnvironmentAsync(request);
            return new BeanstalkEnvironmentInfo(
                EnvironmentName: resp.EnvironmentName,
                EnvironmentId: resp.EnvironmentId,
                EnvironmentArn: resp.EnvironmentArn,
                ApplicationName: resp.ApplicationName,
                VersionLabel: resp.VersionLabel,
                SolutionStackName: resp.SolutionStackName,
                PlatformArn: resp.PlatformArn,
                Status: resp.Status?.Value,
                Health: resp.Health?.Value,
                HealthStatus: resp.HealthStatus?.Value,
                CNAME: resp.CNAME,
                EndpointURL: resp.EndpointURL,
                DateCreated: resp.DateCreated,
                DateUpdated: resp.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update environment");
        }
    }

    /// <summary>
    /// Restart the app server for an environment.
    /// </summary>
    public static async Task RestartAppServerAsync(
        string? environmentId = null,
        string? environmentName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RestartAppServerRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;

        try
        {
            await client.RestartAppServerAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to restart app server for '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Rebuild an environment.
    /// </summary>
    public static async Task RebuildEnvironmentAsync(
        string? environmentId = null,
        string? environmentName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RebuildEnvironmentRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;

        try
        {
            await client.RebuildEnvironmentAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rebuild environment '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Swap environment CNAMEs.
    /// </summary>
    public static async Task SwapEnvironmentCNAMEsAsync(
        string? sourceEnvironmentId = null,
        string? sourceEnvironmentName = null,
        string? destinationEnvironmentId = null,
        string? destinationEnvironmentName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SwapEnvironmentCNAMEsRequest();
        if (sourceEnvironmentId != null)
            request.SourceEnvironmentId = sourceEnvironmentId;
        if (sourceEnvironmentName != null)
            request.SourceEnvironmentName = sourceEnvironmentName;
        if (destinationEnvironmentId != null)
            request.DestinationEnvironmentId = destinationEnvironmentId;
        if (destinationEnvironmentName != null)
            request.DestinationEnvironmentName = destinationEnvironmentName;

        try
        {
            await client.SwapEnvironmentCNAMEsAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to swap environment CNAMEs");
        }
    }

    /// <summary>
    /// Describe environment resources.
    /// </summary>
    public static async Task<BeanstalkEnvironmentResourceInfo>
        DescribeEnvironmentResourcesAsync(
            string? environmentId = null,
            string? environmentName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEnvironmentResourcesRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;

        try
        {
            var resp = await client.DescribeEnvironmentResourcesAsync(request);
            var r = resp.EnvironmentResources;
            return new BeanstalkEnvironmentResourceInfo(
                EnvironmentName: r.EnvironmentName,
                AutoScalingGroups: r.AutoScalingGroups?
                    .Select(a => a.Name).ToList(),
                Instances: r.Instances?
                    .Select(i => i.Id).ToList(),
                LaunchConfigurations: r.LaunchConfigurations?
                    .Select(l => l.Name).ToList(),
                LoadBalancers: r.LoadBalancers?
                    .Select(l => l.Name).ToList(),
                Queues: r.Queues?
                    .Select(q => q.URL).ToList(),
                Triggers: r.Triggers?
                    .Select(t => t.Name).ToList());
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe environment resources for '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Describe environment health.
    /// </summary>
    public static async Task<BeanstalkEnvironmentHealthInfo>
        DescribeEnvironmentHealthAsync(
            string? environmentId = null,
            string? environmentName = null,
            List<string>? attributeNames = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEnvironmentHealthRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;
        if (attributeNames != null)
            request.AttributeNames = attributeNames; // In SDK v4, attribute names are plain strings

        try
        {
            var resp = await client.DescribeEnvironmentHealthAsync(request);
            return new BeanstalkEnvironmentHealthInfo(
                EnvironmentName: resp.EnvironmentName,
                HealthStatus: resp.HealthStatus,
                Status: resp.Status?.Value,
                Color: resp.Color,
                RefreshedAt: resp.RefreshedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe environment health for '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Describe instances health.
    /// </summary>
    public static async Task<List<BeanstalkInstanceHealthInfo>>
        DescribeInstancesHealthAsync(
            string? environmentId = null,
            string? environmentName = null,
            List<string>? attributeNames = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeInstancesHealthRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;
        if (attributeNames != null)
            request.AttributeNames = attributeNames; // In SDK v4, attribute names are plain strings
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeInstancesHealthAsync(request);
            return resp.InstanceHealthList.Select(h =>
                new BeanstalkInstanceHealthInfo(
                    InstanceId: h.InstanceId,
                    HealthStatus: h.HealthStatus,
                    Color: h.Color,
                    InstanceType: h.InstanceType,
                    AvailabilityZone: h.AvailabilityZone,
                    LaunchedAt: h.LaunchedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe instances health for '{environmentName ?? environmentId}'");
        }
    }

    // ── Configuration operations ─────────────────────────────────────

    /// <summary>
    /// Describe configuration settings.
    /// </summary>
    public static async Task<List<BeanstalkConfigSettingsInfo>>
        DescribeConfigurationSettingsAsync(
            string applicationName,
            string? environmentName = null,
            string? templateName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConfigurationSettingsRequest
        {
            ApplicationName = applicationName
        };
        if (environmentName != null) request.EnvironmentName = environmentName;
        if (templateName != null) request.TemplateName = templateName;

        try
        {
            var resp = await client.DescribeConfigurationSettingsAsync(request);
            return resp.ConfigurationSettings.Select(c =>
                new BeanstalkConfigSettingsInfo(
                    SolutionStackName: c.SolutionStackName,
                    PlatformArn: c.PlatformArn,
                    ApplicationName: c.ApplicationName,
                    TemplateName: c.TemplateName,
                    EnvironmentName: c.EnvironmentName,
                    Description: c.Description,
                    OptionSettings: c.OptionSettings,
                    DateCreated: c.DateCreated,
                    DateUpdated: c.DateUpdated)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe configuration settings for '{applicationName}'");
        }
    }

    /// <summary>
    /// Describe configuration options.
    /// </summary>
    public static async Task<List<BeanstalkConfigOptionInfo>>
        DescribeConfigurationOptionsAsync(
            DescribeConfigurationOptionsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeConfigurationOptionsAsync(request);
            return resp.Options.Select(o => new BeanstalkConfigOptionInfo(
                Namespace: o.Namespace,
                Name: o.Name,
                DefaultValue: o.DefaultValue,
                ChangeSeverity: o.ChangeSeverity,
                UserDefined: o.UserDefined,
                ValueType: o.ValueType?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe configuration options");
        }
    }

    /// <summary>
    /// Validate configuration settings.
    /// </summary>
    public static async Task<List<BeanstalkValidationMessage>>
        ValidateConfigurationSettingsAsync(
            string applicationName,
            List<ConfigurationOptionSetting> optionSettings,
            string? environmentName = null,
            string? templateName = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ValidateConfigurationSettingsRequest
        {
            ApplicationName = applicationName,
            OptionSettings = optionSettings
        };
        if (environmentName != null) request.EnvironmentName = environmentName;
        if (templateName != null) request.TemplateName = templateName;

        try
        {
            var resp = await client.ValidateConfigurationSettingsAsync(request);
            return resp.Messages.Select(m => new BeanstalkValidationMessage(
                Message: m.Message,
                Severity: m.Severity?.Value,
                Namespace: m.Namespace,
                OptionName: m.OptionName)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to validate configuration settings for '{applicationName}'");
        }
    }

    /// <summary>
    /// Update a configuration template.
    /// </summary>
    public static async Task<BeanstalkConfigSettingsInfo>
        UpdateConfigurationTemplateAsync(
            UpdateConfigurationTemplateRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateConfigurationTemplateAsync(request);
            return new BeanstalkConfigSettingsInfo(
                SolutionStackName: resp.SolutionStackName,
                PlatformArn: resp.PlatformArn,
                ApplicationName: resp.ApplicationName,
                TemplateName: resp.TemplateName,
                EnvironmentName: resp.EnvironmentName,
                Description: resp.Description,
                OptionSettings: resp.OptionSettings,
                DateCreated: resp.DateCreated,
                DateUpdated: resp.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update configuration template");
        }
    }

    /// <summary>
    /// Create a configuration template.
    /// </summary>
    public static async Task<BeanstalkConfigSettingsInfo>
        CreateConfigurationTemplateAsync(
            CreateConfigurationTemplateRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateConfigurationTemplateAsync(request);
            return new BeanstalkConfigSettingsInfo(
                SolutionStackName: resp.SolutionStackName,
                PlatformArn: resp.PlatformArn,
                ApplicationName: resp.ApplicationName,
                TemplateName: resp.TemplateName,
                EnvironmentName: resp.EnvironmentName,
                Description: resp.Description,
                OptionSettings: resp.OptionSettings,
                DateCreated: resp.DateCreated,
                DateUpdated: resp.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create configuration template");
        }
    }

    /// <summary>
    /// Delete a configuration template.
    /// </summary>
    public static async Task DeleteConfigurationTemplateAsync(
        string applicationName,
        string templateName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConfigurationTemplateAsync(
                new DeleteConfigurationTemplateRequest
                {
                    ApplicationName = applicationName,
                    TemplateName = templateName
                });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete configuration template '{templateName}'");
        }
    }

    // ── Platform operations ──────────────────────────────────────────

    /// <summary>
    /// List available solution stacks.
    /// </summary>
    public static async Task<List<BeanstalkSolutionStackInfo>>
        ListAvailableSolutionStacksAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListAvailableSolutionStacksAsync(
                new ListAvailableSolutionStacksRequest());
            return resp.SolutionStackDetails.Select(s =>
                new BeanstalkSolutionStackInfo(
                    SolutionStackName: s.SolutionStackName,
                    PermittedFileTypes: s.PermittedFileTypes)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list available solution stacks");
        }
    }

    /// <summary>
    /// List platform versions.
    /// </summary>
    public static async Task<List<BeanstalkPlatformSummary>>
        ListPlatformVersionsAsync(
            List<PlatformFilter>? filters = null,
            int? maxRecords = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPlatformVersionsRequest();
        if (filters != null) request.Filters = filters;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListPlatformVersionsAsync(request);
            return resp.PlatformSummaryList.Select(p =>
                new BeanstalkPlatformSummary(
                    PlatformArn: p.PlatformArn,
                    PlatformStatus: p.PlatformStatus?.Value,
                    PlatformCategory: p.PlatformCategory,
                    OperatingSystemName: p.OperatingSystemName,
                    OperatingSystemVersion: p.OperatingSystemVersion)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list platform versions");
        }
    }

    /// <summary>
    /// Describe a platform version.
    /// </summary>
    public static async Task<BeanstalkPlatformDescription>
        DescribePlatformVersionAsync(
            string platformArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePlatformVersionAsync(
                new DescribePlatformVersionRequest
                {
                    PlatformArn = platformArn
                });
            var p = resp.PlatformDescription;
            return new BeanstalkPlatformDescription(
                PlatformArn: p.PlatformArn,
                PlatformName: p.PlatformName,
                PlatformVersion: p.PlatformVersion,
                PlatformStatus: p.PlatformStatus?.Value,
                PlatformCategory: p.PlatformCategory,
                OperatingSystemName: p.OperatingSystemName,
                OperatingSystemVersion: p.OperatingSystemVersion,
                SolutionStackName: p.SolutionStackName,
                Description: p.Description,
                DateCreated: p.DateCreated,
                DateUpdated: p.DateUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe platform version '{platformArn}'");
        }
    }

    // ── Storage & Misc ───────────────────────────────────────────────

    /// <summary>
    /// Create the S3 storage location for Elastic Beanstalk.
    /// </summary>
    public static async Task<BeanstalkStorageLocationResult>
        CreateStorageLocationAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStorageLocationAsync(
                new CreateStorageLocationRequest());
            return new BeanstalkStorageLocationResult(S3Bucket: resp.S3Bucket);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create storage location");
        }
    }

    /// <summary>
    /// Abort an in-progress environment update.
    /// </summary>
    public static async Task AbortEnvironmentUpdateAsync(
        string? environmentId = null,
        string? environmentName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AbortEnvironmentUpdateRequest();
        if (environmentId != null) request.EnvironmentId = environmentId;
        if (environmentName != null) request.EnvironmentName = environmentName;

        try
        {
            await client.AbortEnvironmentUpdateAsync(request);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to abort environment update for '{environmentName ?? environmentId}'");
        }
    }

    /// <summary>
    /// Compose environments from a group of application versions.
    /// </summary>
    public static async Task<List<BeanstalkEnvironmentInfo>>
        ComposeEnvironmentsAsync(
            string? applicationName = null,
            string? groupName = null,
            List<string>? versionLabels = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ComposeEnvironmentsRequest();
        if (applicationName != null) request.ApplicationName = applicationName;
        if (groupName != null) request.GroupName = groupName;
        if (versionLabels != null) request.VersionLabels = versionLabels;

        try
        {
            var resp = await client.ComposeEnvironmentsAsync(request);
            return resp.Environments.Select(e => new BeanstalkEnvironmentInfo(
                EnvironmentName: e.EnvironmentName,
                EnvironmentId: e.EnvironmentId,
                EnvironmentArn: e.EnvironmentArn,
                ApplicationName: e.ApplicationName,
                VersionLabel: e.VersionLabel,
                SolutionStackName: e.SolutionStackName,
                PlatformArn: e.PlatformArn,
                Status: e.Status?.Value,
                Health: e.Health?.Value,
                HealthStatus: e.HealthStatus?.Value,
                CNAME: e.CNAME,
                EndpointURL: e.EndpointURL,
                DateCreated: e.DateCreated,
                DateUpdated: e.DateUpdated)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to compose environments");
        }
    }

    /// <summary>
    /// Describe events for an application or environment.
    /// </summary>
    public static async Task<List<BeanstalkEventInfo>> DescribeEventsAsync(
        DescribeEventsRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new DescribeEventsRequest();

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return resp.Events.Select(e => new BeanstalkEventInfo(
                EventDate: e.EventDate.ToString(),
                Message: e.Message,
                ApplicationName: e.ApplicationName,
                EnvironmentName: e.EnvironmentName,
                VersionLabel: e.VersionLabel,
                Severity: e.Severity?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe events");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// List tags for a Beanstalk resource.
    /// </summary>
    public static async Task<List<Amazon.ElasticBeanstalk.Model.Tag>>
        ListTagsForResourceAsync(
            string resourceArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest
                {
                    ResourceArn = resourceArn
                });
            return resp.ResourceTags;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Add or update tags for a Beanstalk resource.
    /// </summary>
    public static async Task<BeanstalkTagResult> UpdateTagsForResourceAsync(
        string resourceArn,
        List<Amazon.ElasticBeanstalk.Model.Tag>? tagsToAdd = null,
        List<string>? tagsToRemove = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (tagsToAdd != null) request.TagsToAdd = tagsToAdd;
        if (tagsToRemove != null) request.TagsToRemove = tagsToRemove;

        try
        {
            await client.UpdateTagsForResourceAsync(request);
            return new BeanstalkTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update tags for resource '{resourceArn}'");
        }
    }
}
