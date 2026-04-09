using Amazon;
using Amazon.AppRunner;
using Amazon.AppRunner.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record AppRunnerServiceInfo(
    string? ServiceArn = null,
    string? ServiceName = null,
    string? ServiceId = null,
    string? ServiceUrl = null,
    string? Status = null,
    DateTime? CreatedAt = null,
    DateTime? UpdatedAt = null);

public sealed record AppRunnerAutoScalingConfig(
    string? AutoScalingConfigurationArn = null,
    string? AutoScalingConfigurationName = null,
    int? AutoScalingConfigurationRevision = null,
    string? Status = null,
    int? MaxConcurrency = null,
    int? MinSize = null,
    int? MaxSize = null);

public sealed record AppRunnerConnectionInfo(
    string? ConnectionArn = null,
    string? ConnectionName = null,
    string? ProviderType = null,
    string? Status = null);

public sealed record AppRunnerVpcConnectorInfo(
    string? VpcConnectorArn = null,
    string? VpcConnectorName = null,
    string? Status = null,
    int? VpcConnectorRevision = null);

public sealed record AppRunnerObservabilityConfig(
    string? ObservabilityConfigurationArn = null,
    string? ObservabilityConfigurationName = null,
    int? ObservabilityConfigurationRevision = null,
    string? Status = null);

public sealed record AppRunnerVpcIngressConnectionInfo(
    string? VpcIngressConnectionArn = null,
    string? VpcIngressConnectionName = null,
    string? Status = null,
    string? ServiceArn = null);

public sealed record AppRunnerOperationInfo(
    string? Id = null,
    string? Type = null,
    string? Status = null,
    DateTime? StartedAt = null,
    DateTime? EndedAt = null);

public sealed record AppRunnerDeploymentInfo(
    string? OperationId = null);

public sealed record AppRunnerTagResult(bool Success = true);

public sealed record AppRunnerListTagsResult(
    List<Tag>? Tags = null);

/// <summary>
/// Utility helpers for AWS App Runner.
/// </summary>
public static class AppRunnerService
{
    private static AmazonAppRunnerClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAppRunnerClient>(region);

    // ── Service operations ───────────────────────────────────────────

    /// <summary>
    /// Create a new App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> CreateServiceAsync(
        CreateServiceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServiceAsync(request);
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create App Runner service");
        }
    }

    /// <summary>
    /// Delete an App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> DeleteServiceAsync(
        string serviceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteServiceAsync(new DeleteServiceRequest
            {
                ServiceArn = serviceArn
            });
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete App Runner service '{serviceArn}'");
        }
    }

    /// <summary>
    /// Describe an App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> DescribeServiceAsync(
        string serviceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeServiceAsync(new DescribeServiceRequest
            {
                ServiceArn = serviceArn
            });
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe App Runner service '{serviceArn}'");
        }
    }

    /// <summary>
    /// List App Runner services.
    /// </summary>
    public static async Task<List<AppRunnerServiceInfo>> ListServicesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServicesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListServicesAsync(request);
            return resp.ServiceSummaryList.Select(s => new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list App Runner services");
        }
    }

    /// <summary>
    /// Update an App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> UpdateServiceAsync(
        UpdateServiceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServiceAsync(request);
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update App Runner service");
        }
    }

    /// <summary>
    /// Pause an App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> PauseServiceAsync(
        string serviceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PauseServiceAsync(new PauseServiceRequest
            {
                ServiceArn = serviceArn
            });
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to pause App Runner service '{serviceArn}'");
        }
    }

    /// <summary>
    /// Resume an App Runner service.
    /// </summary>
    public static async Task<AppRunnerServiceInfo> ResumeServiceAsync(
        string serviceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ResumeServiceAsync(new ResumeServiceRequest
            {
                ServiceArn = serviceArn
            });
            var s = resp.Service;
            return new AppRunnerServiceInfo(
                ServiceArn: s.ServiceArn,
                ServiceName: s.ServiceName,
                ServiceId: s.ServiceId,
                ServiceUrl: s.ServiceUrl,
                Status: s.Status?.Value,
                CreatedAt: s.CreatedAt,
                UpdatedAt: s.UpdatedAt);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to resume App Runner service '{serviceArn}'");
        }
    }

    // ── Auto Scaling Configuration ───────────────────────────────────

    /// <summary>
    /// Create an auto scaling configuration.
    /// </summary>
    public static async Task<AppRunnerAutoScalingConfig> CreateAutoScalingConfigurationAsync(
        string autoScalingConfigurationName,
        int? maxConcurrency = null,
        int? minSize = null,
        int? maxSize = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAutoScalingConfigurationRequest
        {
            AutoScalingConfigurationName = autoScalingConfigurationName
        };
        if (maxConcurrency.HasValue) request.MaxConcurrency = maxConcurrency.Value;
        if (minSize.HasValue) request.MinSize = minSize.Value;
        if (maxSize.HasValue) request.MaxSize = maxSize.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAutoScalingConfigurationAsync(request);
            var c = resp.AutoScalingConfiguration;
            return new AppRunnerAutoScalingConfig(
                AutoScalingConfigurationArn: c.AutoScalingConfigurationArn,
                AutoScalingConfigurationName: c.AutoScalingConfigurationName,
                AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
                Status: c.Status?.Value,
                MaxConcurrency: c.MaxConcurrency,
                MinSize: c.MinSize,
                MaxSize: c.MaxSize);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create auto scaling configuration '{autoScalingConfigurationName}'");
        }
    }

    /// <summary>
    /// Delete an auto scaling configuration.
    /// </summary>
    public static async Task<AppRunnerAutoScalingConfig> DeleteAutoScalingConfigurationAsync(
        string autoScalingConfigurationArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteAutoScalingConfigurationAsync(
                new DeleteAutoScalingConfigurationRequest
                {
                    AutoScalingConfigurationArn = autoScalingConfigurationArn
                });
            var c = resp.AutoScalingConfiguration;
            return new AppRunnerAutoScalingConfig(
                AutoScalingConfigurationArn: c.AutoScalingConfigurationArn,
                AutoScalingConfigurationName: c.AutoScalingConfigurationName,
                AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
                Status: c.Status?.Value,
                MaxConcurrency: c.MaxConcurrency,
                MinSize: c.MinSize,
                MaxSize: c.MaxSize);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete auto scaling configuration '{autoScalingConfigurationArn}'");
        }
    }

    /// <summary>
    /// Describe an auto scaling configuration.
    /// </summary>
    public static async Task<AppRunnerAutoScalingConfig> DescribeAutoScalingConfigurationAsync(
        string autoScalingConfigurationArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAutoScalingConfigurationAsync(
                new DescribeAutoScalingConfigurationRequest
                {
                    AutoScalingConfigurationArn = autoScalingConfigurationArn
                });
            var c = resp.AutoScalingConfiguration;
            return new AppRunnerAutoScalingConfig(
                AutoScalingConfigurationArn: c.AutoScalingConfigurationArn,
                AutoScalingConfigurationName: c.AutoScalingConfigurationName,
                AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
                Status: c.Status?.Value,
                MaxConcurrency: c.MaxConcurrency,
                MinSize: c.MinSize,
                MaxSize: c.MaxSize);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe auto scaling configuration '{autoScalingConfigurationArn}'");
        }
    }

    /// <summary>
    /// List auto scaling configurations.
    /// </summary>
    public static async Task<List<AppRunnerAutoScalingConfig>> ListAutoScalingConfigurationsAsync(
        string? autoScalingConfigurationName = null,
        bool? latestOnly = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAutoScalingConfigurationsRequest();
        if (autoScalingConfigurationName != null)
            request.AutoScalingConfigurationName = autoScalingConfigurationName;
        if (latestOnly.HasValue) request.LatestOnly = latestOnly.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListAutoScalingConfigurationsAsync(request);
            return resp.AutoScalingConfigurationSummaryList.Select(c =>
                new AppRunnerAutoScalingConfig(
                    AutoScalingConfigurationArn: c.AutoScalingConfigurationArn,
                    AutoScalingConfigurationName: c.AutoScalingConfigurationName,
                    AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
                    Status: c.Status?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list auto scaling configurations");
        }
    }

    // ── Connection operations ────────────────────────────────────────

    /// <summary>
    /// Create a connection to a source code provider.
    /// </summary>
    public static async Task<AppRunnerConnectionInfo> CreateConnectionAsync(
        string connectionName,
        string providerType,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateConnectionRequest
        {
            ConnectionName = connectionName,
            ProviderType = providerType
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateConnectionAsync(request);
            var c = resp.Connection;
            return new AppRunnerConnectionInfo(
                ConnectionArn: c.ConnectionArn,
                ConnectionName: c.ConnectionName,
                ProviderType: c.ProviderType?.Value,
                Status: c.Status?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create connection '{connectionName}'");
        }
    }

    /// <summary>
    /// Delete a connection.
    /// </summary>
    public static async Task<AppRunnerConnectionInfo> DeleteConnectionAsync(
        string connectionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteConnectionAsync(new DeleteConnectionRequest
            {
                ConnectionArn = connectionArn
            });
            var c = resp.Connection;
            return new AppRunnerConnectionInfo(
                ConnectionArn: c.ConnectionArn,
                ConnectionName: c.ConnectionName,
                ProviderType: c.ProviderType?.Value,
                Status: c.Status?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete connection '{connectionArn}'");
        }
    }

    /// <summary>
    /// List connections, optionally filtered by connection name.
    /// </summary>
    public static async Task<List<AppRunnerConnectionInfo>> ListConnectionsAsync(
        string? connectionName = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConnectionsRequest();
        if (connectionName != null) request.ConnectionName = connectionName;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListConnectionsAsync(request);
            return resp.ConnectionSummaryList.Select(c =>
                new AppRunnerConnectionInfo(
                    ConnectionArn: c.ConnectionArn,
                    ConnectionName: c.ConnectionName,
                    ProviderType: c.ProviderType?.Value,
                    Status: c.Status?.Value)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list connections");
        }
    }

    // ── VPC Connector operations ─────────────────────────────────────

    /// <summary>
    /// Create a VPC connector.
    /// </summary>
    public static async Task<AppRunnerVpcConnectorInfo> CreateVpcConnectorAsync(
        CreateVpcConnectorRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVpcConnectorAsync(request);
            var v = resp.VpcConnector;
            return new AppRunnerVpcConnectorInfo(
                VpcConnectorArn: v.VpcConnectorArn,
                VpcConnectorName: v.VpcConnectorName,
                Status: v.Status?.Value,
                VpcConnectorRevision: v.VpcConnectorRevision);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC connector");
        }
    }

    /// <summary>
    /// Delete a VPC connector.
    /// </summary>
    public static async Task<AppRunnerVpcConnectorInfo> DeleteVpcConnectorAsync(
        string vpcConnectorArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteVpcConnectorAsync(new DeleteVpcConnectorRequest
            {
                VpcConnectorArn = vpcConnectorArn
            });
            var v = resp.VpcConnector;
            return new AppRunnerVpcConnectorInfo(
                VpcConnectorArn: v.VpcConnectorArn,
                VpcConnectorName: v.VpcConnectorName,
                Status: v.Status?.Value,
                VpcConnectorRevision: v.VpcConnectorRevision);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete VPC connector '{vpcConnectorArn}'");
        }
    }

    /// <summary>
    /// Describe a VPC connector.
    /// </summary>
    public static async Task<AppRunnerVpcConnectorInfo> DescribeVpcConnectorAsync(
        string vpcConnectorArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeVpcConnectorAsync(
                new DescribeVpcConnectorRequest
                {
                    VpcConnectorArn = vpcConnectorArn
                });
            var v = resp.VpcConnector;
            return new AppRunnerVpcConnectorInfo(
                VpcConnectorArn: v.VpcConnectorArn,
                VpcConnectorName: v.VpcConnectorName,
                Status: v.Status?.Value,
                VpcConnectorRevision: v.VpcConnectorRevision);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe VPC connector '{vpcConnectorArn}'");
        }
    }

    /// <summary>
    /// List VPC connectors.
    /// </summary>
    public static async Task<List<AppRunnerVpcConnectorInfo>> ListVpcConnectorsAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVpcConnectorsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListVpcConnectorsAsync(request);
            return resp.VpcConnectors.Select(v =>
                new AppRunnerVpcConnectorInfo(
                    VpcConnectorArn: v.VpcConnectorArn,
                    VpcConnectorName: v.VpcConnectorName,
                    Status: v.Status?.Value,
                    VpcConnectorRevision: v.VpcConnectorRevision)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC connectors");
        }
    }

    // ── Observability Configuration ──────────────────────────────────

    /// <summary>
    /// Create an observability configuration.
    /// </summary>
    public static async Task<AppRunnerObservabilityConfig> CreateObservabilityConfigurationAsync(
        CreateObservabilityConfigurationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateObservabilityConfigurationAsync(request);
            var c = resp.ObservabilityConfiguration;
            return new AppRunnerObservabilityConfig(
                ObservabilityConfigurationArn: c.ObservabilityConfigurationArn,
                ObservabilityConfigurationName: c.ObservabilityConfigurationName,
                ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision,
                Status: c.Status?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create observability configuration");
        }
    }

    /// <summary>
    /// Delete an observability configuration.
    /// </summary>
    public static async Task<AppRunnerObservabilityConfig> DeleteObservabilityConfigurationAsync(
        string observabilityConfigurationArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteObservabilityConfigurationAsync(
                new DeleteObservabilityConfigurationRequest
                {
                    ObservabilityConfigurationArn = observabilityConfigurationArn
                });
            var c = resp.ObservabilityConfiguration;
            return new AppRunnerObservabilityConfig(
                ObservabilityConfigurationArn: c.ObservabilityConfigurationArn,
                ObservabilityConfigurationName: c.ObservabilityConfigurationName,
                ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision,
                Status: c.Status?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete observability configuration '{observabilityConfigurationArn}'");
        }
    }

    /// <summary>
    /// Describe an observability configuration.
    /// </summary>
    public static async Task<AppRunnerObservabilityConfig> DescribeObservabilityConfigurationAsync(
        string observabilityConfigurationArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeObservabilityConfigurationAsync(
                new DescribeObservabilityConfigurationRequest
                {
                    ObservabilityConfigurationArn = observabilityConfigurationArn
                });
            var c = resp.ObservabilityConfiguration;
            return new AppRunnerObservabilityConfig(
                ObservabilityConfigurationArn: c.ObservabilityConfigurationArn,
                ObservabilityConfigurationName: c.ObservabilityConfigurationName,
                ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision,
                Status: c.Status?.Value);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe observability configuration '{observabilityConfigurationArn}'");
        }
    }

    /// <summary>
    /// List observability configurations.
    /// </summary>
    public static async Task<List<AppRunnerObservabilityConfig>> ListObservabilityConfigurationsAsync(
        string? observabilityConfigurationName = null,
        bool? latestOnly = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListObservabilityConfigurationsRequest();
        if (observabilityConfigurationName != null)
            request.ObservabilityConfigurationName = observabilityConfigurationName;
        if (latestOnly.HasValue) request.LatestOnly = latestOnly.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListObservabilityConfigurationsAsync(request);
            return resp.ObservabilityConfigurationSummaryList.Select(c =>
                new AppRunnerObservabilityConfig(
                    ObservabilityConfigurationArn: c.ObservabilityConfigurationArn,
                    ObservabilityConfigurationName: c.ObservabilityConfigurationName,
                    ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision))
                .ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list observability configurations");
        }
    }

    // ── VPC Ingress Connection ───────────────────────────────────────

    /// <summary>
    /// Create a VPC ingress connection.
    /// </summary>
    public static async Task<AppRunnerVpcIngressConnectionInfo> CreateVpcIngressConnectionAsync(
        CreateVpcIngressConnectionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateVpcIngressConnectionAsync(request);
            var v = resp.VpcIngressConnection;
            return new AppRunnerVpcIngressConnectionInfo(
                VpcIngressConnectionArn: v.VpcIngressConnectionArn,
                VpcIngressConnectionName: v.VpcIngressConnectionName,
                Status: v.Status?.Value,
                ServiceArn: v.ServiceArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create VPC ingress connection");
        }
    }

    /// <summary>
    /// Delete a VPC ingress connection.
    /// </summary>
    public static async Task<AppRunnerVpcIngressConnectionInfo> DeleteVpcIngressConnectionAsync(
        string vpcIngressConnectionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteVpcIngressConnectionAsync(
                new DeleteVpcIngressConnectionRequest
                {
                    VpcIngressConnectionArn = vpcIngressConnectionArn
                });
            var v = resp.VpcIngressConnection;
            return new AppRunnerVpcIngressConnectionInfo(
                VpcIngressConnectionArn: v.VpcIngressConnectionArn,
                VpcIngressConnectionName: v.VpcIngressConnectionName,
                Status: v.Status?.Value,
                ServiceArn: v.ServiceArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete VPC ingress connection '{vpcIngressConnectionArn}'");
        }
    }

    /// <summary>
    /// Describe a VPC ingress connection.
    /// </summary>
    public static async Task<AppRunnerVpcIngressConnectionInfo> DescribeVpcIngressConnectionAsync(
        string vpcIngressConnectionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeVpcIngressConnectionAsync(
                new DescribeVpcIngressConnectionRequest
                {
                    VpcIngressConnectionArn = vpcIngressConnectionArn
                });
            var v = resp.VpcIngressConnection;
            return new AppRunnerVpcIngressConnectionInfo(
                VpcIngressConnectionArn: v.VpcIngressConnectionArn,
                VpcIngressConnectionName: v.VpcIngressConnectionName,
                Status: v.Status?.Value,
                ServiceArn: v.ServiceArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe VPC ingress connection '{vpcIngressConnectionArn}'");
        }
    }

    /// <summary>
    /// List VPC ingress connections.
    /// </summary>
    public static async Task<List<AppRunnerVpcIngressConnectionInfo>> ListVpcIngressConnectionsAsync(
        ListVpcIngressConnectionsFilter? filter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVpcIngressConnectionsRequest();
        if (filter != null) request.Filter = filter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListVpcIngressConnectionsAsync(request);
            return resp.VpcIngressConnectionSummaryList.Select(v =>
                new AppRunnerVpcIngressConnectionInfo(
                    VpcIngressConnectionArn: v.VpcIngressConnectionArn,
                    ServiceArn: v.ServiceArn)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list VPC ingress connections");
        }
    }

    /// <summary>
    /// Update a VPC ingress connection.
    /// </summary>
    public static async Task<AppRunnerVpcIngressConnectionInfo> UpdateVpcIngressConnectionAsync(
        UpdateVpcIngressConnectionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateVpcIngressConnectionAsync(request);
            var v = resp.VpcIngressConnection;
            return new AppRunnerVpcIngressConnectionInfo(
                VpcIngressConnectionArn: v.VpcIngressConnectionArn,
                VpcIngressConnectionName: v.VpcIngressConnectionName,
                Status: v.Status?.Value,
                ServiceArn: v.ServiceArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update VPC ingress connection");
        }
    }

    // ── Deployment & Operations ──────────────────────────────────────

    /// <summary>
    /// Start a manual deployment for a service.
    /// </summary>
    public static async Task<AppRunnerDeploymentInfo> StartDeploymentAsync(
        string serviceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartDeploymentAsync(new StartDeploymentRequest
            {
                ServiceArn = serviceArn
            });
            return new AppRunnerDeploymentInfo(OperationId: resp.OperationId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start deployment for service '{serviceArn}'");
        }
    }

    /// <summary>
    /// List operations for a service.
    /// </summary>
    public static async Task<List<AppRunnerOperationInfo>> ListOperationsAsync(
        string serviceArn,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListOperationsRequest { ServiceArn = serviceArn };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListOperationsAsync(request);
            return resp.OperationSummaryList.Select(o =>
                new AppRunnerOperationInfo(
                    Id: o.Id,
                    Type: o.Type?.Value,
                    Status: o.Status?.Value,
                    StartedAt: o.StartedAt,
                    EndedAt: o.EndedAt)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list operations for service '{serviceArn}'");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Tag an App Runner resource.
    /// </summary>
    public static async Task<AppRunnerTagResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
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
            return new AppRunnerTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an App Runner resource.
    /// </summary>
    public static async Task<AppRunnerTagResult> UntagResourceAsync(
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
            return new AppRunnerTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an App Runner resource.
    /// </summary>
    public static async Task<AppRunnerListTagsResult> ListTagsForResourceAsync(
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
            return new AppRunnerListTagsResult(Tags: resp.Tags);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}
