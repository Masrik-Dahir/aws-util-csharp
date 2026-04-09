using Amazon;
using Amazon.ConfigService;
using Amazon.ConfigService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CfgPutConfigRuleResult(bool Success = true);
public sealed record CfgDeleteConfigRuleResult(bool Success = true);

public sealed record CfgDescribeConfigRulesResult(
    List<ConfigRule>? ConfigRules = null,
    string? NextToken = null);

public sealed record CfgPutConfigurationRecorderResult(bool Success = true);
public sealed record CfgDeleteConfigurationRecorderResult(bool Success = true);

public sealed record CfgDescribeConfigurationRecordersResult(
    List<ConfigurationRecorder>? ConfigurationRecorders = null);

public sealed record CfgStartConfigurationRecorderResult(bool Success = true);
public sealed record CfgStopConfigurationRecorderResult(bool Success = true);

public sealed record CfgPutDeliveryChannelResult(bool Success = true);
public sealed record CfgDeleteDeliveryChannelResult(bool Success = true);

public sealed record CfgDescribeDeliveryChannelsResult(
    List<DeliveryChannel>? DeliveryChannels = null);

public sealed record CfgGetComplianceDetailsByConfigRuleResult(
    List<EvaluationResult>? EvaluationResults = null,
    string? NextToken = null);

public sealed record CfgGetComplianceDetailsByResourceResult(
    List<EvaluationResult>? EvaluationResults = null,
    string? NextToken = null);

public sealed record CfgGetComplianceSummaryByConfigRuleResult(
    ComplianceSummary? ComplianceSummary = null);

public sealed record CfgGetComplianceSummaryByResourceTypeResult(
    List<ComplianceSummaryByResourceType>? ComplianceSummariesByResourceType = null);

public sealed record CfgDescribeComplianceByConfigRuleResult(
    List<ComplianceByConfigRule>? ComplianceByConfigRules = null,
    string? NextToken = null);

public sealed record CfgDescribeComplianceByResourceResult(
    List<ComplianceByResource>? ComplianceByResources = null,
    string? NextToken = null);

public sealed record CfgPutRemediationConfigurationsResult(
    List<FailedRemediationBatch>? FailedBatches = null);

public sealed record CfgDeleteRemediationConfigurationResult(bool Success = true);

public sealed record CfgDescribeRemediationConfigurationsResult(
    List<RemediationConfiguration>? RemediationConfigurations = null);

public sealed record CfgStartRemediationExecutionResult(
    string? FailureMessage = null,
    List<ResourceKey>? FailedItems = null);

public sealed record CfgPutConformancePackResult(string? ConformancePackArn = null);
public sealed record CfgDeleteConformancePackResult(bool Success = true);

public sealed record CfgDescribeConformancePacksResult(
    List<ConformancePackDetail>? ConformancePackDetails = null,
    string? NextToken = null);

public sealed record CfgGetConformancePackComplianceSummaryResult(
    List<ConformancePackComplianceSummary>?
        ConformancePackComplianceSummaryList = null,
    string? NextToken = null);

public sealed record CfgPutOrganizationConfigRuleResult(
    string? OrganizationConfigRuleArn = null);

public sealed record CfgDeleteOrganizationConfigRuleResult(bool Success = true);

public sealed record CfgDescribeOrganizationConfigRulesResult(
    List<OrganizationConfigRule>? OrganizationConfigRules = null,
    string? NextToken = null);

public sealed record CfgPutOrganizationConformancePackResult(
    string? OrganizationConformancePackArn = null);

public sealed record CfgDeleteOrganizationConformancePackResult(
    bool Success = true);

public sealed record CfgDescribeOrganizationConformancePacksResult(
    List<OrganizationConformancePack>? OrganizationConformancePacks = null,
    string? NextToken = null);

public sealed record CfgPutAggregationAuthorizationResult(
    AggregationAuthorization? AggregationAuthorization = null);

public sealed record CfgDeleteAggregationAuthorizationResult(bool Success = true);

public sealed record CfgDescribeAggregationAuthorizationsResult(
    List<AggregationAuthorization>? AggregationAuthorizations = null,
    string? NextToken = null);

public sealed record CfgPutConfigurationAggregatorResult(
    ConfigurationAggregator? ConfigurationAggregator = null);

public sealed record CfgDeleteConfigurationAggregatorResult(bool Success = true);

public sealed record CfgDescribeConfigurationAggregatorsResult(
    List<ConfigurationAggregator>? ConfigurationAggregators = null,
    string? NextToken = null);

public sealed record CfgGetResourceConfigHistoryResult(
    List<ConfigurationItem>? ConfigurationItems = null,
    string? NextToken = null);

public sealed record CfgSelectResourceConfigResult(
    List<string>? Results = null,
    QueryInfo? QueryInfo = null,
    string? NextToken = null);

public sealed record CfgTagResourceResult(bool Success = true);
public sealed record CfgUntagResourceResult(bool Success = true);

public sealed record CfgListTagsForResourceResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Config.
/// </summary>
public static class ConfigServiceService
{
    private static AmazonConfigServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonConfigServiceClient>(region);

    /// <summary>Put a Config rule.</summary>
    public static async Task<CfgPutConfigRuleResult> PutConfigRuleAsync(
        ConfigRule configRule,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutConfigRuleRequest { ConfigRule = configRule };
        if (tags != null) request.Tags = tags;

        try
        {
            await client.PutConfigRuleAsync(request);
            return new CfgPutConfigRuleResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put Config rule");
        }
    }

    /// <summary>Delete a Config rule.</summary>
    public static async Task<CfgDeleteConfigRuleResult> DeleteConfigRuleAsync(
        string configRuleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConfigRuleAsync(
                new DeleteConfigRuleRequest { ConfigRuleName = configRuleName });
            return new CfgDeleteConfigRuleResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Config rule '{configRuleName}'");
        }
    }

    /// <summary>Describe Config rules.</summary>
    public static async Task<CfgDescribeConfigRulesResult> DescribeConfigRulesAsync(
        List<string>? configRuleNames = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConfigRulesRequest();
        if (configRuleNames != null) request.ConfigRuleNames = configRuleNames;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeConfigRulesAsync(request);
            return new CfgDescribeConfigRulesResult(
                ConfigRules: resp.ConfigRules,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Config rules");
        }
    }

    /// <summary>Put a configuration recorder.</summary>
    public static async Task<CfgPutConfigurationRecorderResult>
        PutConfigurationRecorderAsync(
            ConfigurationRecorder configurationRecorder,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutConfigurationRecorderAsync(
                new PutConfigurationRecorderRequest
                {
                    ConfigurationRecorder = configurationRecorder
                });
            return new CfgPutConfigurationRecorderResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put configuration recorder");
        }
    }

    /// <summary>Delete a configuration recorder.</summary>
    public static async Task<CfgDeleteConfigurationRecorderResult>
        DeleteConfigurationRecorderAsync(
            string configurationRecorderName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConfigurationRecorderAsync(
                new DeleteConfigurationRecorderRequest
                {
                    ConfigurationRecorderName = configurationRecorderName
                });
            return new CfgDeleteConfigurationRecorderResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete configuration recorder '{configurationRecorderName}'");
        }
    }

    /// <summary>Describe configuration recorders.</summary>
    public static async Task<CfgDescribeConfigurationRecordersResult>
        DescribeConfigurationRecordersAsync(
            List<string>? configurationRecorderNames = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConfigurationRecordersRequest();
        if (configurationRecorderNames != null)
            request.ConfigurationRecorderNames = configurationRecorderNames;

        try
        {
            var resp = await client.DescribeConfigurationRecordersAsync(request);
            return new CfgDescribeConfigurationRecordersResult(
                ConfigurationRecorders: resp.ConfigurationRecorders);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe configuration recorders");
        }
    }

    /// <summary>Start a configuration recorder.</summary>
    public static async Task<CfgStartConfigurationRecorderResult>
        StartConfigurationRecorderAsync(
            string configurationRecorderName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartConfigurationRecorderAsync(
                new StartConfigurationRecorderRequest
                {
                    ConfigurationRecorderName = configurationRecorderName
                });
            return new CfgStartConfigurationRecorderResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start configuration recorder '{configurationRecorderName}'");
        }
    }

    /// <summary>Stop a configuration recorder.</summary>
    public static async Task<CfgStopConfigurationRecorderResult>
        StopConfigurationRecorderAsync(
            string configurationRecorderName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopConfigurationRecorderAsync(
                new StopConfigurationRecorderRequest
                {
                    ConfigurationRecorderName = configurationRecorderName
                });
            return new CfgStopConfigurationRecorderResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop configuration recorder '{configurationRecorderName}'");
        }
    }

    /// <summary>Put a delivery channel.</summary>
    public static async Task<CfgPutDeliveryChannelResult> PutDeliveryChannelAsync(
        DeliveryChannel deliveryChannel,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutDeliveryChannelAsync(
                new PutDeliveryChannelRequest
                {
                    DeliveryChannel = deliveryChannel
                });
            return new CfgPutDeliveryChannelResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put delivery channel");
        }
    }

    /// <summary>Delete a delivery channel.</summary>
    public static async Task<CfgDeleteDeliveryChannelResult>
        DeleteDeliveryChannelAsync(
            string deliveryChannelName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDeliveryChannelAsync(
                new DeleteDeliveryChannelRequest
                {
                    DeliveryChannelName = deliveryChannelName
                });
            return new CfgDeleteDeliveryChannelResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete delivery channel '{deliveryChannelName}'");
        }
    }

    /// <summary>Describe delivery channels.</summary>
    public static async Task<CfgDescribeDeliveryChannelsResult>
        DescribeDeliveryChannelsAsync(
            List<string>? deliveryChannelNames = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDeliveryChannelsRequest();
        if (deliveryChannelNames != null)
            request.DeliveryChannelNames = deliveryChannelNames;

        try
        {
            var resp = await client.DescribeDeliveryChannelsAsync(request);
            return new CfgDescribeDeliveryChannelsResult(
                DeliveryChannels: resp.DeliveryChannels);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe delivery channels");
        }
    }

    /// <summary>Get compliance details by Config rule.</summary>
    public static async Task<CfgGetComplianceDetailsByConfigRuleResult>
        GetComplianceDetailsByConfigRuleAsync(
            string configRuleName,
            List<string>? complianceTypes = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetComplianceDetailsByConfigRuleRequest
        {
            ConfigRuleName = configRuleName
        };
        if (complianceTypes != null) request.ComplianceTypes = complianceTypes;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetComplianceDetailsByConfigRuleAsync(request);
            return new CfgGetComplianceDetailsByConfigRuleResult(
                EvaluationResults: resp.EvaluationResults,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get compliance details for rule '{configRuleName}'");
        }
    }

    /// <summary>Get compliance details by resource.</summary>
    public static async Task<CfgGetComplianceDetailsByResourceResult>
        GetComplianceDetailsByResourceAsync(
            string resourceType,
            string resourceId,
            List<string>? complianceTypes = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetComplianceDetailsByResourceRequest
        {
            ResourceType = resourceType,
            ResourceId = resourceId
        };
        if (complianceTypes != null) request.ComplianceTypes = complianceTypes;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetComplianceDetailsByResourceAsync(request);
            return new CfgGetComplianceDetailsByResourceResult(
                EvaluationResults: resp.EvaluationResults,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get compliance details for resource '{resourceId}'");
        }
    }

    /// <summary>Get compliance summary by Config rule.</summary>
    public static async Task<CfgGetComplianceSummaryByConfigRuleResult>
        GetComplianceSummaryByConfigRuleAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetComplianceSummaryByConfigRuleAsync(
                new GetComplianceSummaryByConfigRuleRequest());
            return new CfgGetComplianceSummaryByConfigRuleResult(
                ComplianceSummary: resp.ComplianceSummary);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get compliance summary by Config rule");
        }
    }

    /// <summary>Get compliance summary by resource type.</summary>
    public static async Task<CfgGetComplianceSummaryByResourceTypeResult>
        GetComplianceSummaryByResourceTypeAsync(
            List<string>? resourceTypes = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetComplianceSummaryByResourceTypeRequest();
        if (resourceTypes != null) request.ResourceTypes = resourceTypes;

        try
        {
            var resp = await client
                .GetComplianceSummaryByResourceTypeAsync(request);
            return new CfgGetComplianceSummaryByResourceTypeResult(
                ComplianceSummariesByResourceType:
                    resp.ComplianceSummariesByResourceType);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get compliance summary by resource type");
        }
    }

    /// <summary>Describe compliance by Config rule.</summary>
    public static async Task<CfgDescribeComplianceByConfigRuleResult>
        DescribeComplianceByConfigRuleAsync(
            List<string>? configRuleNames = null,
            List<string>? complianceTypes = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeComplianceByConfigRuleRequest();
        if (configRuleNames != null) request.ConfigRuleNames = configRuleNames;
        if (complianceTypes != null) request.ComplianceTypes = complianceTypes;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeComplianceByConfigRuleAsync(request);
            return new CfgDescribeComplianceByConfigRuleResult(
                ComplianceByConfigRules: resp.ComplianceByConfigRules,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe compliance by Config rule");
        }
    }

    /// <summary>Describe compliance by resource.</summary>
    public static async Task<CfgDescribeComplianceByResourceResult>
        DescribeComplianceByResourceAsync(
            string? resourceType = null,
            string? resourceId = null,
            List<string>? complianceTypes = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeComplianceByResourceRequest();
        if (resourceType != null) request.ResourceType = resourceType;
        if (resourceId != null) request.ResourceId = resourceId;
        if (complianceTypes != null) request.ComplianceTypes = complianceTypes;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeComplianceByResourceAsync(request);
            return new CfgDescribeComplianceByResourceResult(
                ComplianceByResources: resp.ComplianceByResources,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe compliance by resource");
        }
    }

    /// <summary>Put remediation configurations.</summary>
    public static async Task<CfgPutRemediationConfigurationsResult>
        PutRemediationConfigurationsAsync(
            List<RemediationConfiguration> remediationConfigurations,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutRemediationConfigurationsAsync(
                new PutRemediationConfigurationsRequest
                {
                    RemediationConfigurations = remediationConfigurations
                });
            return new CfgPutRemediationConfigurationsResult(
                FailedBatches: resp.FailedBatches);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put remediation configurations");
        }
    }

    /// <summary>Delete a remediation configuration.</summary>
    public static async Task<CfgDeleteRemediationConfigurationResult>
        DeleteRemediationConfigurationAsync(
            string configRuleName,
            string? resourceType = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRemediationConfigurationRequest
        {
            ConfigRuleName = configRuleName
        };
        if (resourceType != null) request.ResourceType = resourceType;

        try
        {
            await client.DeleteRemediationConfigurationAsync(request);
            return new CfgDeleteRemediationConfigurationResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete remediation configuration for '{configRuleName}'");
        }
    }

    /// <summary>Describe remediation configurations.</summary>
    public static async Task<CfgDescribeRemediationConfigurationsResult>
        DescribeRemediationConfigurationsAsync(
            List<string> configRuleNames,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRemediationConfigurationsAsync(
                new DescribeRemediationConfigurationsRequest
                {
                    ConfigRuleNames = configRuleNames
                });
            return new CfgDescribeRemediationConfigurationsResult(
                RemediationConfigurations: resp.RemediationConfigurations);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe remediation configurations");
        }
    }

    /// <summary>Start remediation execution.</summary>
    public static async Task<CfgStartRemediationExecutionResult>
        StartRemediationExecutionAsync(
            string configRuleName,
            List<ResourceKey> resourceKeys,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartRemediationExecutionAsync(
                new StartRemediationExecutionRequest
                {
                    ConfigRuleName = configRuleName,
                    ResourceKeys = resourceKeys
                });
            return new CfgStartRemediationExecutionResult(
                FailureMessage: resp.FailureMessage,
                FailedItems: resp.FailedItems);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start remediation execution for '{configRuleName}'");
        }
    }

    /// <summary>Put a conformance pack.</summary>
    public static async Task<CfgPutConformancePackResult> PutConformancePackAsync(
        PutConformancePackRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutConformancePackAsync(request);
            return new CfgPutConformancePackResult(
                ConformancePackArn: resp.ConformancePackArn);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put conformance pack");
        }
    }

    /// <summary>Delete a conformance pack.</summary>
    public static async Task<CfgDeleteConformancePackResult>
        DeleteConformancePackAsync(
            string conformancePackName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConformancePackAsync(
                new DeleteConformancePackRequest
                {
                    ConformancePackName = conformancePackName
                });
            return new CfgDeleteConformancePackResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete conformance pack '{conformancePackName}'");
        }
    }

    /// <summary>Describe conformance packs.</summary>
    public static async Task<CfgDescribeConformancePacksResult>
        DescribeConformancePacksAsync(
            List<string>? conformancePackNames = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConformancePacksRequest();
        if (conformancePackNames != null)
            request.ConformancePackNames = conformancePackNames;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeConformancePacksAsync(request);
            return new CfgDescribeConformancePacksResult(
                ConformancePackDetails: resp.ConformancePackDetails,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe conformance packs");
        }
    }

    /// <summary>Get conformance pack compliance summary.</summary>
    public static async Task<CfgGetConformancePackComplianceSummaryResult>
        GetConformancePackComplianceSummaryAsync(
            List<string> conformancePackNames,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetConformancePackComplianceSummaryRequest
        {
            ConformancePackNames = conformancePackNames
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client
                .GetConformancePackComplianceSummaryAsync(request);
            return new CfgGetConformancePackComplianceSummaryResult(
                ConformancePackComplianceSummaryList:
                    resp.ConformancePackComplianceSummaryList,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get conformance pack compliance summary");
        }
    }

    /// <summary>Put an organization Config rule.</summary>
    public static async Task<CfgPutOrganizationConfigRuleResult>
        PutOrganizationConfigRuleAsync(
            PutOrganizationConfigRuleRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutOrganizationConfigRuleAsync(request);
            return new CfgPutOrganizationConfigRuleResult(
                OrganizationConfigRuleArn: resp.OrganizationConfigRuleArn);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put organization Config rule");
        }
    }

    /// <summary>Delete an organization Config rule.</summary>
    public static async Task<CfgDeleteOrganizationConfigRuleResult>
        DeleteOrganizationConfigRuleAsync(
            string organizationConfigRuleName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteOrganizationConfigRuleAsync(
                new DeleteOrganizationConfigRuleRequest
                {
                    OrganizationConfigRuleName = organizationConfigRuleName
                });
            return new CfgDeleteOrganizationConfigRuleResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete organization Config rule '{organizationConfigRuleName}'");
        }
    }

    /// <summary>Describe organization Config rules.</summary>
    public static async Task<CfgDescribeOrganizationConfigRulesResult>
        DescribeOrganizationConfigRulesAsync(
            List<string>? organizationConfigRuleNames = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeOrganizationConfigRulesRequest();
        if (organizationConfigRuleNames != null)
            request.OrganizationConfigRuleNames = organizationConfigRuleNames;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client
                .DescribeOrganizationConfigRulesAsync(request);
            return new CfgDescribeOrganizationConfigRulesResult(
                OrganizationConfigRules: resp.OrganizationConfigRules,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe organization Config rules");
        }
    }

    /// <summary>Put an organization conformance pack.</summary>
    public static async Task<CfgPutOrganizationConformancePackResult>
        PutOrganizationConformancePackAsync(
            PutOrganizationConformancePackRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client
                .PutOrganizationConformancePackAsync(request);
            return new CfgPutOrganizationConformancePackResult(
                OrganizationConformancePackArn:
                    resp.OrganizationConformancePackArn);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put organization conformance pack");
        }
    }

    /// <summary>Delete an organization conformance pack.</summary>
    public static async Task<CfgDeleteOrganizationConformancePackResult>
        DeleteOrganizationConformancePackAsync(
            string organizationConformancePackName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteOrganizationConformancePackAsync(
                new DeleteOrganizationConformancePackRequest
                {
                    OrganizationConformancePackName =
                        organizationConformancePackName
                });
            return new CfgDeleteOrganizationConformancePackResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete organization conformance pack " +
                $"'{organizationConformancePackName}'");
        }
    }

    /// <summary>Describe organization conformance packs.</summary>
    public static async Task<CfgDescribeOrganizationConformancePacksResult>
        DescribeOrganizationConformancePacksAsync(
            List<string>? organizationConformancePackNames = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeOrganizationConformancePacksRequest();
        if (organizationConformancePackNames != null)
            request.OrganizationConformancePackNames =
                organizationConformancePackNames;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client
                .DescribeOrganizationConformancePacksAsync(request);
            return new CfgDescribeOrganizationConformancePacksResult(
                OrganizationConformancePacks:
                    resp.OrganizationConformancePacks,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe organization conformance packs");
        }
    }

    /// <summary>Put an aggregation authorization.</summary>
    public static async Task<CfgPutAggregationAuthorizationResult>
        PutAggregationAuthorizationAsync(
            string authorizedAccountId,
            string authorizedAwsRegion,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutAggregationAuthorizationRequest
        {
            AuthorizedAccountId = authorizedAccountId,
            AuthorizedAwsRegion = authorizedAwsRegion
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.PutAggregationAuthorizationAsync(request);
            return new CfgPutAggregationAuthorizationResult(
                AggregationAuthorization: resp.AggregationAuthorization);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put aggregation authorization");
        }
    }

    /// <summary>Delete an aggregation authorization.</summary>
    public static async Task<CfgDeleteAggregationAuthorizationResult>
        DeleteAggregationAuthorizationAsync(
            string authorizedAccountId,
            string authorizedAwsRegion,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAggregationAuthorizationAsync(
                new DeleteAggregationAuthorizationRequest
                {
                    AuthorizedAccountId = authorizedAccountId,
                    AuthorizedAwsRegion = authorizedAwsRegion
                });
            return new CfgDeleteAggregationAuthorizationResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete aggregation authorization");
        }
    }

    /// <summary>Describe aggregation authorizations.</summary>
    public static async Task<CfgDescribeAggregationAuthorizationsResult>
        DescribeAggregationAuthorizationsAsync(
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAggregationAuthorizationsRequest();
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client
                .DescribeAggregationAuthorizationsAsync(request);
            return new CfgDescribeAggregationAuthorizationsResult(
                AggregationAuthorizations: resp.AggregationAuthorizations,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe aggregation authorizations");
        }
    }

    /// <summary>Put a configuration aggregator.</summary>
    public static async Task<CfgPutConfigurationAggregatorResult>
        PutConfigurationAggregatorAsync(
            PutConfigurationAggregatorRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutConfigurationAggregatorAsync(request);
            return new CfgPutConfigurationAggregatorResult(
                ConfigurationAggregator: resp.ConfigurationAggregator);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put configuration aggregator");
        }
    }

    /// <summary>Delete a configuration aggregator.</summary>
    public static async Task<CfgDeleteConfigurationAggregatorResult>
        DeleteConfigurationAggregatorAsync(
            string configurationAggregatorName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConfigurationAggregatorAsync(
                new DeleteConfigurationAggregatorRequest
                {
                    ConfigurationAggregatorName = configurationAggregatorName
                });
            return new CfgDeleteConfigurationAggregatorResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete configuration aggregator " +
                $"'{configurationAggregatorName}'");
        }
    }

    /// <summary>Describe configuration aggregators.</summary>
    public static async Task<CfgDescribeConfigurationAggregatorsResult>
        DescribeConfigurationAggregatorsAsync(
            List<string>? configurationAggregatorNames = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeConfigurationAggregatorsRequest();
        if (configurationAggregatorNames != null)
            request.ConfigurationAggregatorNames = configurationAggregatorNames;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client
                .DescribeConfigurationAggregatorsAsync(request);
            return new CfgDescribeConfigurationAggregatorsResult(
                ConfigurationAggregators: resp.ConfigurationAggregators,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe configuration aggregators");
        }
    }

    /// <summary>Get resource configuration history.</summary>
    public static async Task<CfgGetResourceConfigHistoryResult>
        GetResourceConfigHistoryAsync(
            string resourceType,
            string resourceId,
            DateTime? laterTime = null,
            DateTime? earlierTime = null,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetResourceConfigHistoryRequest
        {
            ResourceType = resourceType,
            ResourceId = resourceId
        };
        if (laterTime.HasValue) request.LaterTime = laterTime.Value;
        if (earlierTime.HasValue) request.EarlierTime = earlierTime.Value;
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetResourceConfigHistoryAsync(request);
            return new CfgGetResourceConfigHistoryResult(
                ConfigurationItems: resp.ConfigurationItems,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get resource config history for '{resourceId}'");
        }
    }

    /// <summary>Select resource configuration using SQL-like queries.</summary>
    public static async Task<CfgSelectResourceConfigResult>
        SelectResourceConfigAsync(
            string expression,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SelectResourceConfigRequest
        {
            Expression = expression
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.SelectResourceConfigAsync(request);
            return new CfgSelectResourceConfigResult(
                Results: resp.Results,
                QueryInfo: resp.QueryInfo,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to select resource config");
        }
    }

    /// <summary>Tag a Config resource.</summary>
    public static async Task<CfgTagResourceResult> TagResourceAsync(
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
            return new CfgTagResourceResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Config resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a Config resource.</summary>
    public static async Task<CfgUntagResourceResult> UntagResourceAsync(
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
            return new CfgUntagResourceResult();
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Config resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a Config resource.</summary>
    public static async Task<CfgListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            int? limit = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (limit.HasValue) request.Limit = limit.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new CfgListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonConfigServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Config resource '{resourceArn}'");
        }
    }
}
