using Amazon;
using Amazon.Kafka;
using Amazon.Kafka.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record MskCreateClusterResult(
    string? ClusterArn = null,
    string? ClusterName = null,
    string? State = null);

public sealed record MskDeleteClusterResult(
    string? ClusterArn = null,
    string? State = null);

public sealed record MskDescribeClusterResult(
    ClusterInfo? ClusterInfo = null);

public sealed record MskListClustersResult(
    List<ClusterInfo>? ClusterInfoList = null,
    string? NextToken = null);

public sealed record MskUpdateBrokerCountResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateBrokerStorageResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateBrokerTypeResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateClusterConfigurationResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateClusterKafkaVersionResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateConnectivityResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateMonitoringResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskUpdateSecurityResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskRebootBrokerResult(
    string? ClusterArn = null,
    string? ClusterOperationArn = null);

public sealed record MskGetBootstrapBrokersResult(
    string? BootstrapBrokerString = null,
    string? BootstrapBrokerStringTls = null,
    string? BootstrapBrokerStringSaslScram = null,
    string? BootstrapBrokerStringSaslIam = null,
    string? BootstrapBrokerStringPublicTls = null,
    string? BootstrapBrokerStringPublicSaslScram = null,
    string? BootstrapBrokerStringPublicSaslIam = null,
    string? BootstrapBrokerStringVpcConnectivityTls = null,
    string? BootstrapBrokerStringVpcConnectivitySaslScram = null,
    string? BootstrapBrokerStringVpcConnectivitySaslIam = null);

public sealed record MskListNodesResult(
    List<NodeInfo>? NodeInfoList = null,
    string? NextToken = null);

public sealed record MskCreateConfigurationResult(
    string? Arn = null,
    string? Name = null,
    ConfigurationRevision? LatestRevision = null);

public sealed record MskDeleteConfigurationResult(
    string? Arn = null,
    string? State = null);

public sealed record MskDescribeConfigurationResult(
    string? Arn = null,
    string? Name = null,
    string? Description = null,
    ConfigurationRevision? LatestRevision = null);

public sealed record MskListConfigurationsResult(
    List<Configuration>? Configurations = null,
    string? NextToken = null);

public sealed record MskDescribeConfigurationRevisionResult(
    string? Arn = null,
    long? Revision = null,
    string? Description = null,
    byte[]? ServerProperties = null);

public sealed record MskListConfigurationRevisionsResult(
    List<ConfigurationRevision>? Revisions = null,
    string? NextToken = null);

public sealed record MskUpdateConfigurationResult(
    string? Arn = null,
    ConfigurationRevision? LatestRevision = null);

public sealed record MskCreateClusterV2Result(
    string? ClusterArn = null,
    string? ClusterName = null,
    string? State = null,
    string? ClusterType = null);

public sealed record MskDescribeClusterV2Result(
    Cluster? ClusterInfo = null);

public sealed record MskListClustersV2Result(
    List<Cluster>? ClusterInfoList = null,
    string? NextToken = null);

public sealed record MskBatchAssociateScramSecretResult(
    string? ClusterArn = null,
    List<UnprocessedScramSecret>? UnprocessedScramSecrets = null);

public sealed record MskBatchDisassociateScramSecretResult(
    string? ClusterArn = null,
    List<UnprocessedScramSecret>? UnprocessedScramSecrets = null);

public sealed record MskListScramSecretsResult(
    List<string>? SecretArnList = null,
    string? NextToken = null);

public sealed record MskGetCompatibleKafkaVersionsResult(
    List<CompatibleKafkaVersion>? CompatibleKafkaVersions = null);

public sealed record MskTagResourceResult(bool Success = true);
public sealed record MskUntagResourceResult(bool Success = true);

public sealed record MskListTagsForResourceResult(
    Dictionary<string, string>? Tags = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Managed Streaming for Apache Kafka (MSK).
/// </summary>
public static class MskService
{
    private static AmazonKafkaClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKafkaClient>(region);

    /// <summary>Create an MSK cluster.</summary>
    public static async Task<MskCreateClusterResult> CreateClusterAsync(
        CreateClusterRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterAsync(request);
            return new MskCreateClusterResult(
                ClusterArn: resp.ClusterArn,
                ClusterName: resp.ClusterName,
                State: resp.State?.Value);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create MSK cluster");
        }
    }

    /// <summary>Delete an MSK cluster.</summary>
    public static async Task<MskDeleteClusterResult> DeleteClusterAsync(
        string clusterArn,
        string? currentVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteClusterRequest { ClusterArn = clusterArn };
        if (currentVersion != null) request.CurrentVersion = currentVersion;

        try
        {
            var resp = await client.DeleteClusterAsync(request);
            return new MskDeleteClusterResult(
                ClusterArn: resp.ClusterArn,
                State: resp.State?.Value);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MSK cluster '{clusterArn}'");
        }
    }

    /// <summary>Describe an MSK cluster.</summary>
    public static async Task<MskDescribeClusterResult> DescribeClusterAsync(
        string clusterArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClusterAsync(
                new DescribeClusterRequest { ClusterArn = clusterArn });
            return new MskDescribeClusterResult(ClusterInfo: resp.ClusterInfo);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe MSK cluster '{clusterArn}'");
        }
    }

    /// <summary>List MSK clusters.</summary>
    public static async Task<MskListClustersResult> ListClustersAsync(
        string? clusterNameFilter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListClustersRequest();
        if (clusterNameFilter != null) request.ClusterNameFilter = clusterNameFilter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListClustersAsync(request);
            return new MskListClustersResult(
                ClusterInfoList: resp.ClusterInfoList,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MSK clusters");
        }
    }

    /// <summary>Update the number of brokers in an MSK cluster.</summary>
    public static async Task<MskUpdateBrokerCountResult> UpdateBrokerCountAsync(
        string clusterArn,
        string currentVersion,
        int targetNumberOfBrokerNodes,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateBrokerCountAsync(
                new UpdateBrokerCountRequest
                {
                    ClusterArn = clusterArn,
                    CurrentVersion = currentVersion,
                    TargetNumberOfBrokerNodes = targetNumberOfBrokerNodes
                });
            return new MskUpdateBrokerCountResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update broker count for '{clusterArn}'");
        }
    }

    /// <summary>Update broker storage for an MSK cluster.</summary>
    public static async Task<MskUpdateBrokerStorageResult> UpdateBrokerStorageAsync(
        string clusterArn,
        string currentVersion,
        List<BrokerEBSVolumeInfo> targetBrokerEBSVolumeInfo,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateBrokerStorageAsync(
                new UpdateBrokerStorageRequest
                {
                    ClusterArn = clusterArn,
                    CurrentVersion = currentVersion,
                    TargetBrokerEBSVolumeInfo = targetBrokerEBSVolumeInfo
                });
            return new MskUpdateBrokerStorageResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update broker storage for '{clusterArn}'");
        }
    }

    /// <summary>Update broker type for an MSK cluster.</summary>
    public static async Task<MskUpdateBrokerTypeResult> UpdateBrokerTypeAsync(
        string clusterArn,
        string currentVersion,
        string targetInstanceType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateBrokerTypeAsync(
                new UpdateBrokerTypeRequest
                {
                    ClusterArn = clusterArn,
                    CurrentVersion = currentVersion,
                    TargetInstanceType = targetInstanceType
                });
            return new MskUpdateBrokerTypeResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update broker type for '{clusterArn}'");
        }
    }

    /// <summary>Update cluster configuration for an MSK cluster.</summary>
    public static async Task<MskUpdateClusterConfigurationResult>
        UpdateClusterConfigurationAsync(
            string clusterArn,
            string currentVersion,
            ConfigurationInfo configurationInfo,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateClusterConfigurationAsync(
                new UpdateClusterConfigurationRequest
                {
                    ClusterArn = clusterArn,
                    CurrentVersion = currentVersion,
                    ConfigurationInfo = configurationInfo
                });
            return new MskUpdateClusterConfigurationResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update cluster configuration for '{clusterArn}'");
        }
    }

    /// <summary>Update the Kafka version for an MSK cluster.</summary>
    public static async Task<MskUpdateClusterKafkaVersionResult>
        UpdateClusterKafkaVersionAsync(
            string clusterArn,
            string currentVersion,
            string targetKafkaVersion,
            ConfigurationInfo? configurationInfo = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateClusterKafkaVersionRequest
        {
            ClusterArn = clusterArn,
            CurrentVersion = currentVersion,
            TargetKafkaVersion = targetKafkaVersion
        };
        if (configurationInfo != null) request.ConfigurationInfo = configurationInfo;

        try
        {
            var resp = await client.UpdateClusterKafkaVersionAsync(request);
            return new MskUpdateClusterKafkaVersionResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Kafka version for '{clusterArn}'");
        }
    }

    /// <summary>Update connectivity settings for an MSK cluster.</summary>
    public static async Task<MskUpdateConnectivityResult> UpdateConnectivityAsync(
        string clusterArn,
        string currentVersion,
        ConnectivityInfo connectivityInfo,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateConnectivityAsync(
                new UpdateConnectivityRequest
                {
                    ClusterArn = clusterArn,
                    CurrentVersion = currentVersion,
                    ConnectivityInfo = connectivityInfo
                });
            return new MskUpdateConnectivityResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update connectivity for '{clusterArn}'");
        }
    }

    /// <summary>Update monitoring for an MSK cluster.</summary>
    public static async Task<MskUpdateMonitoringResult> UpdateMonitoringAsync(
        UpdateMonitoringRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateMonitoringAsync(request);
            return new MskUpdateMonitoringResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update monitoring for MSK cluster");
        }
    }

    /// <summary>Update security settings for an MSK cluster.</summary>
    public static async Task<MskUpdateSecurityResult> UpdateSecurityAsync(
        UpdateSecurityRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateSecurityAsync(request);
            return new MskUpdateSecurityResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update security for MSK cluster");
        }
    }

    /// <summary>Reboot an MSK broker.</summary>
    public static async Task<MskRebootBrokerResult> RebootBrokerAsync(
        string clusterArn,
        List<string> brokerIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RebootBrokerAsync(new RebootBrokerRequest
            {
                ClusterArn = clusterArn,
                BrokerIds = brokerIds
            });
            return new MskRebootBrokerResult(
                ClusterArn: resp.ClusterArn,
                ClusterOperationArn: resp.ClusterOperationArn);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reboot broker in '{clusterArn}'");
        }
    }

    /// <summary>Get bootstrap broker strings for an MSK cluster.</summary>
    public static async Task<MskGetBootstrapBrokersResult> GetBootstrapBrokersAsync(
        string clusterArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBootstrapBrokersAsync(
                new GetBootstrapBrokersRequest { ClusterArn = clusterArn });
            return new MskGetBootstrapBrokersResult(
                BootstrapBrokerString: resp.BootstrapBrokerString,
                BootstrapBrokerStringTls: resp.BootstrapBrokerStringTls,
                BootstrapBrokerStringSaslScram: resp.BootstrapBrokerStringSaslScram,
                BootstrapBrokerStringSaslIam: resp.BootstrapBrokerStringSaslIam,
                BootstrapBrokerStringPublicTls: resp.BootstrapBrokerStringPublicTls,
                BootstrapBrokerStringPublicSaslScram:
                    resp.BootstrapBrokerStringPublicSaslScram,
                BootstrapBrokerStringPublicSaslIam:
                    resp.BootstrapBrokerStringPublicSaslIam,
                BootstrapBrokerStringVpcConnectivityTls:
                    resp.BootstrapBrokerStringVpcConnectivityTls,
                BootstrapBrokerStringVpcConnectivitySaslScram:
                    resp.BootstrapBrokerStringVpcConnectivitySaslScram,
                BootstrapBrokerStringVpcConnectivitySaslIam:
                    resp.BootstrapBrokerStringVpcConnectivitySaslIam);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bootstrap brokers for '{clusterArn}'");
        }
    }

    /// <summary>List nodes in an MSK cluster.</summary>
    public static async Task<MskListNodesResult> ListNodesAsync(
        string clusterArn,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListNodesRequest { ClusterArn = clusterArn };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListNodesAsync(request);
            return new MskListNodesResult(
                NodeInfoList: resp.NodeInfoList,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list nodes for '{clusterArn}'");
        }
    }

    /// <summary>Create an MSK configuration.</summary>
    public static async Task<MskCreateConfigurationResult> CreateConfigurationAsync(
        string name,
        MemoryStream serverProperties,
        string? description = null,
        List<string>? kafkaVersions = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateConfigurationRequest
        {
            Name = name,
            ServerProperties = serverProperties
        };
        if (description != null) request.Description = description;
        if (kafkaVersions != null) request.KafkaVersions = kafkaVersions;

        try
        {
            var resp = await client.CreateConfigurationAsync(request);
            return new MskCreateConfigurationResult(
                Arn: resp.Arn,
                Name: resp.Name,
                LatestRevision: resp.LatestRevision);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create MSK configuration '{name}'");
        }
    }

    /// <summary>Delete an MSK configuration.</summary>
    public static async Task<MskDeleteConfigurationResult> DeleteConfigurationAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteConfigurationAsync(
                new DeleteConfigurationRequest { Arn = arn });
            return new MskDeleteConfigurationResult(
                Arn: resp.Arn,
                State: resp.State?.Value);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete MSK configuration '{arn}'");
        }
    }

    /// <summary>Describe an MSK configuration.</summary>
    public static async Task<MskDescribeConfigurationResult> DescribeConfigurationAsync(
        string arn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeConfigurationAsync(
                new DescribeConfigurationRequest { Arn = arn });
            return new MskDescribeConfigurationResult(
                Arn: resp.Arn,
                Name: resp.Name,
                Description: resp.Description,
                LatestRevision: resp.LatestRevision);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe MSK configuration '{arn}'");
        }
    }

    /// <summary>List MSK configurations.</summary>
    public static async Task<MskListConfigurationsResult> ListConfigurationsAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConfigurationsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListConfigurationsAsync(request);
            return new MskListConfigurationsResult(
                Configurations: resp.Configurations,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MSK configurations");
        }
    }

    /// <summary>Describe a specific configuration revision.</summary>
    public static async Task<MskDescribeConfigurationRevisionResult>
        DescribeConfigurationRevisionAsync(
            string arn,
            long revision,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeConfigurationRevisionAsync(
                new DescribeConfigurationRevisionRequest
                {
                    Arn = arn,
                    Revision = revision
                });
            return new MskDescribeConfigurationRevisionResult(
                Arn: resp.Arn,
                Revision: resp.Revision,
                Description: resp.Description,
                ServerProperties: resp.ServerProperties?.ToArray());
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe configuration revision {revision} for '{arn}'");
        }
    }

    /// <summary>List configuration revisions.</summary>
    public static async Task<MskListConfigurationRevisionsResult>
        ListConfigurationRevisionsAsync(
            string arn,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConfigurationRevisionsRequest { Arn = arn };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListConfigurationRevisionsAsync(request);
            return new MskListConfigurationRevisionsResult(
                Revisions: resp.Revisions,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list configuration revisions for '{arn}'");
        }
    }

    /// <summary>Update an MSK configuration.</summary>
    public static async Task<MskUpdateConfigurationResult> UpdateConfigurationAsync(
        string arn,
        MemoryStream serverProperties,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateConfigurationRequest
        {
            Arn = arn,
            ServerProperties = serverProperties
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateConfigurationAsync(request);
            return new MskUpdateConfigurationResult(
                Arn: resp.Arn,
                LatestRevision: resp.LatestRevision);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update MSK configuration '{arn}'");
        }
    }

    /// <summary>Create an MSK cluster (V2 API with serverless support).</summary>
    public static async Task<MskCreateClusterV2Result> CreateClusterV2Async(
        CreateClusterV2Request request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateClusterV2Async(request);
            return new MskCreateClusterV2Result(
                ClusterArn: resp.ClusterArn,
                ClusterName: resp.ClusterName,
                State: resp.State?.Value,
                ClusterType: resp.ClusterType?.Value);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create MSK cluster (V2)");
        }
    }

    /// <summary>Describe an MSK cluster (V2 API).</summary>
    public static async Task<MskDescribeClusterV2Result> DescribeClusterV2Async(
        string clusterArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeClusterV2Async(
                new DescribeClusterV2Request { ClusterArn = clusterArn });
            return new MskDescribeClusterV2Result(ClusterInfo: resp.ClusterInfo);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe MSK cluster V2 '{clusterArn}'");
        }
    }

    /// <summary>List MSK clusters (V2 API).</summary>
    public static async Task<MskListClustersV2Result> ListClustersV2Async(
        string? clusterNameFilter = null,
        string? clusterTypeFilter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListClustersV2Request();
        if (clusterNameFilter != null) request.ClusterNameFilter = clusterNameFilter;
        if (clusterTypeFilter != null) request.ClusterTypeFilter = clusterTypeFilter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListClustersV2Async(request);
            return new MskListClustersV2Result(
                ClusterInfoList: resp.ClusterInfoList,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list MSK clusters (V2)");
        }
    }

    /// <summary>Associate SCRAM secrets with an MSK cluster.</summary>
    public static async Task<MskBatchAssociateScramSecretResult>
        BatchAssociateScramSecretAsync(
            string clusterArn,
            List<string> secretArnList,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchAssociateScramSecretAsync(
                new BatchAssociateScramSecretRequest
                {
                    ClusterArn = clusterArn,
                    SecretArnList = secretArnList
                });
            return new MskBatchAssociateScramSecretResult(
                ClusterArn: resp.ClusterArn,
                UnprocessedScramSecrets: resp.UnprocessedScramSecrets);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate SCRAM secrets with '{clusterArn}'");
        }
    }

    /// <summary>Disassociate SCRAM secrets from an MSK cluster.</summary>
    public static async Task<MskBatchDisassociateScramSecretResult>
        BatchDisassociateScramSecretAsync(
            string clusterArn,
            List<string> secretArnList,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchDisassociateScramSecretAsync(
                new BatchDisassociateScramSecretRequest
                {
                    ClusterArn = clusterArn,
                    SecretArnList = secretArnList
                });
            return new MskBatchDisassociateScramSecretResult(
                ClusterArn: resp.ClusterArn,
                UnprocessedScramSecrets: resp.UnprocessedScramSecrets);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate SCRAM secrets from '{clusterArn}'");
        }
    }

    /// <summary>List SCRAM secrets for an MSK cluster.</summary>
    public static async Task<MskListScramSecretsResult> ListScramSecretsAsync(
        string clusterArn,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListScramSecretsRequest { ClusterArn = clusterArn };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListScramSecretsAsync(request);
            return new MskListScramSecretsResult(
                SecretArnList: resp.SecretArnList,
                NextToken: resp.NextToken);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list SCRAM secrets for '{clusterArn}'");
        }
    }

    /// <summary>Get compatible Kafka versions for an MSK cluster.</summary>
    public static async Task<MskGetCompatibleKafkaVersionsResult>
        GetCompatibleKafkaVersionsAsync(
            string? clusterArn = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetCompatibleKafkaVersionsRequest();
        if (clusterArn != null) request.ClusterArn = clusterArn;

        try
        {
            var resp = await client.GetCompatibleKafkaVersionsAsync(request);
            return new MskGetCompatibleKafkaVersionsResult(
                CompatibleKafkaVersions: resp.CompatibleKafkaVersions);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get compatible Kafka versions");
        }
    }

    /// <summary>Tag an MSK resource.</summary>
    public static async Task<MskTagResourceResult> TagResourceAsync(
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
            return new MskTagResourceResult();
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag MSK resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from an MSK resource.</summary>
    public static async Task<MskUntagResourceResult> UntagResourceAsync(
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
            return new MskUntagResourceResult();
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag MSK resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for an MSK resource.</summary>
    public static async Task<MskListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new MskListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonKafkaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for MSK resource '{resourceArn}'");
        }
    }
}
