using Amazon;
using Amazon.Route53;
using Amazon.Route53.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────────────

public sealed record CreateHostedZoneResult(
    string? HostedZoneId = null,
    string? NameServers = null,
    HostedZone? HostedZone = null,
    DelegationSet? DelegationSet = null,
    ChangeInfo? ChangeInfo = null);

public sealed record GetHostedZoneResult(
    HostedZone? HostedZone = null,
    DelegationSet? DelegationSet = null,
    List<VPC>? VPCs = null);

public sealed record ListHostedZonesResult(
    List<HostedZone>? HostedZones = null,
    string? NextMarker = null,
    bool IsTruncated = false);

public sealed record ChangeResourceRecordSetsResult(ChangeInfo? ChangeInfo = null);

public sealed record ListResourceRecordSetsResult(
    List<ResourceRecordSet>? ResourceRecordSets = null,
    bool IsTruncated = false,
    string? NextRecordName = null,
    string? NextRecordType = null,
    string? NextRecordIdentifier = null);

public sealed record GetHostedZoneCountResult(long HostedZoneCount);

public sealed record CreateHealthCheckResult(
    HealthCheck? HealthCheck = null,
    string? Location = null);

public sealed record GetHealthCheckResult(HealthCheck? HealthCheck = null);
public sealed record ListHealthChecksResult(
    List<HealthCheck>? HealthChecks = null,
    string? NextMarker = null,
    bool IsTruncated = false);

public sealed record GetHealthCheckStatusResult(
    List<HealthCheckObservation>? HealthCheckObservations = null);

public sealed record TestDNSAnswerResult(
    string? Nameserver = null,
    string? RecordName = null,
    string? RecordType = null,
    List<string>? RecordData = null,
    string? ResponseCode = null,
    string? Protocol = null);

public sealed record Route53TagsResult(ResourceTagSet? ResourceTagSet = null);

public sealed record CreateQueryLoggingConfigResult(
    QueryLoggingConfig? QueryLoggingConfig = null,
    string? Location = null);

public sealed record GetQueryLoggingConfigResult(QueryLoggingConfig? QueryLoggingConfig = null);
public sealed record ListQueryLoggingConfigsResult(
    List<QueryLoggingConfig>? QueryLoggingConfigs = null,
    string? NextToken = null);

public sealed record CreateReusableDelegationSetResult(
    DelegationSet? DelegationSet = null,
    string? Location = null);

public sealed record GetReusableDelegationSetResult(DelegationSet? DelegationSet = null);
public sealed record ListReusableDelegationSetsResult(
    List<DelegationSet>? DelegationSets = null,
    string? NextMarker = null,
    bool IsTruncated = false);

public sealed record CreateTrafficPolicyResult(
    TrafficPolicy? TrafficPolicy = null,
    string? Location = null);

public sealed record GetTrafficPolicyResult(TrafficPolicy? TrafficPolicy = null);
public sealed record ListTrafficPoliciesResult(
    List<TrafficPolicySummary>? TrafficPolicySummaries = null,
    bool IsTruncated = false,
    string? TrafficPolicyIdMarker = null);

public sealed record GetDNSSECResult(
    DNSSECStatus? Status = null,
    List<KeySigningKey>? KeySigningKeys = null);

// ── Service ─────────────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Route 53.
/// </summary>
public static class Route53Service
{
    private static AmazonRoute53Client GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRoute53Client>(region);

    // ── Hosted Zones ────────────────────────────────────────────────────

    /// <summary>
    /// Create a hosted zone.
    /// </summary>
    public static async Task<CreateHostedZoneResult> CreateHostedZoneAsync(
        CreateHostedZoneRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateHostedZoneAsync(request);
            return new CreateHostedZoneResult(
                HostedZoneId: resp.HostedZone?.Id,
                HostedZone: resp.HostedZone,
                DelegationSet: resp.DelegationSet,
                ChangeInfo: resp.ChangeInfo);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create hosted zone");
        }
    }

    /// <summary>
    /// Delete a hosted zone.
    /// </summary>
    public static async Task DeleteHostedZoneAsync(
        string hostedZoneId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteHostedZoneAsync(new DeleteHostedZoneRequest
            {
                Id = hostedZoneId
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete hosted zone '{hostedZoneId}'");
        }
    }

    /// <summary>
    /// Get a hosted zone.
    /// </summary>
    public static async Task<GetHostedZoneResult> GetHostedZoneAsync(
        string hostedZoneId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetHostedZoneAsync(new GetHostedZoneRequest
            {
                Id = hostedZoneId
            });
            return new GetHostedZoneResult(
                HostedZone: resp.HostedZone,
                DelegationSet: resp.DelegationSet,
                VPCs: resp.VPCs);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get hosted zone '{hostedZoneId}'");
        }
    }

    /// <summary>
    /// List hosted zones.
    /// </summary>
    public static async Task<ListHostedZonesResult> ListHostedZonesAsync(
        string? marker = null,
        int? maxItems = null,
        string? delegationSetId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListHostedZonesRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();
        if (delegationSetId != null) request.DelegationSetId = delegationSetId;

        try
        {
            var resp = await client.ListHostedZonesAsync(request);
            return new ListHostedZonesResult(
                HostedZones: resp.HostedZones,
                NextMarker: resp.NextMarker,
                IsTruncated: resp.IsTruncated ?? false);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list hosted zones");
        }
    }

    // ── Record Sets ─────────────────────────────────────────────────────

    /// <summary>
    /// Change resource record sets (create, update, or delete DNS records).
    /// </summary>
    public static async Task<ChangeResourceRecordSetsResult> ChangeResourceRecordSetsAsync(
        ChangeResourceRecordSetsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ChangeResourceRecordSetsAsync(request);
            return new ChangeResourceRecordSetsResult(ChangeInfo: resp.ChangeInfo);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to change resource record sets");
        }
    }

    /// <summary>
    /// List resource record sets in a hosted zone.
    /// </summary>
    public static async Task<ListResourceRecordSetsResult> ListResourceRecordSetsAsync(
        string hostedZoneId,
        string? startRecordName = null,
        string? startRecordType = null,
        string? startRecordIdentifier = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListResourceRecordSetsRequest
        {
            HostedZoneId = hostedZoneId
        };
        if (startRecordName != null) request.StartRecordName = startRecordName;
        if (startRecordType != null) request.StartRecordType = startRecordType;
        if (startRecordIdentifier != null) request.StartRecordIdentifier = startRecordIdentifier;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListResourceRecordSetsAsync(request);
            return new ListResourceRecordSetsResult(
                ResourceRecordSets: resp.ResourceRecordSets,
                IsTruncated: resp.IsTruncated ?? false,
                NextRecordName: resp.NextRecordName,
                NextRecordType: resp.NextRecordType,
                NextRecordIdentifier: resp.NextRecordIdentifier);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list resource record sets for zone '{hostedZoneId}'");
        }
    }

    /// <summary>
    /// Get the total number of hosted zones.
    /// </summary>
    public static async Task<GetHostedZoneCountResult> GetHostedZoneCountAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetHostedZoneCountAsync(new GetHostedZoneCountRequest());
            return new GetHostedZoneCountResult(HostedZoneCount: resp.HostedZoneCount ?? 0);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get hosted zone count");
        }
    }

    // ── Health Checks ───────────────────────────────────────────────────

    /// <summary>
    /// Create a health check.
    /// </summary>
    public static async Task<CreateHealthCheckResult> CreateHealthCheckAsync(
        CreateHealthCheckRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateHealthCheckAsync(request);
            return new CreateHealthCheckResult(
                HealthCheck: resp.HealthCheck,
                Location: resp.Location);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create health check");
        }
    }

    /// <summary>
    /// Delete a health check.
    /// </summary>
    public static async Task DeleteHealthCheckAsync(
        string healthCheckId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteHealthCheckAsync(new DeleteHealthCheckRequest
            {
                HealthCheckId = healthCheckId
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete health check '{healthCheckId}'");
        }
    }

    /// <summary>
    /// Get a health check.
    /// </summary>
    public static async Task<GetHealthCheckResult> GetHealthCheckAsync(
        string healthCheckId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetHealthCheckAsync(new GetHealthCheckRequest
            {
                HealthCheckId = healthCheckId
            });
            return new GetHealthCheckResult(HealthCheck: resp.HealthCheck);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get health check '{healthCheckId}'");
        }
    }

    /// <summary>
    /// List health checks.
    /// </summary>
    public static async Task<ListHealthChecksResult> ListHealthChecksAsync(
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListHealthChecksRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListHealthChecksAsync(request);
            return new ListHealthChecksResult(
                HealthChecks: resp.HealthChecks,
                NextMarker: resp.NextMarker,
                IsTruncated: resp.IsTruncated ?? false);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list health checks");
        }
    }

    /// <summary>
    /// Update a health check.
    /// </summary>
    public static async Task UpdateHealthCheckAsync(
        UpdateHealthCheckRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateHealthCheckAsync(request);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update health check");
        }
    }

    /// <summary>
    /// Get health check status.
    /// </summary>
    public static async Task<GetHealthCheckStatusResult> GetHealthCheckStatusAsync(
        string healthCheckId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetHealthCheckStatusAsync(new GetHealthCheckStatusRequest
            {
                HealthCheckId = healthCheckId
            });
            return new GetHealthCheckStatusResult(
                HealthCheckObservations: resp.HealthCheckObservations);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get health check status for '{healthCheckId}'");
        }
    }

    // ── DNS Answers ─────────────────────────────────────────────────────

    /// <summary>
    /// Test a DNS answer.
    /// </summary>
    public static async Task<TestDNSAnswerResult> TestDNSAnswerAsync(
        TestDNSAnswerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TestDNSAnswerAsync(request);
            return new TestDNSAnswerResult(
                Nameserver: resp.Nameserver,
                RecordName: resp.RecordName,
                RecordType: resp.RecordType?.Value,
                RecordData: resp.RecordData,
                ResponseCode: resp.ResponseCode,
                Protocol: resp.Protocol);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to test DNS answer");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────────

    /// <summary>
    /// Change tags for a Route 53 resource.
    /// </summary>
    public static async Task ChangeTagsForResourceAsync(
        ChangeTagsForResourceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ChangeTagsForResourceAsync(request);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to change tags for resource");
        }
    }

    /// <summary>
    /// List tags for a Route 53 resource.
    /// </summary>
    public static async Task<Route53TagsResult> ListTagsForResourceAsync(
        TagResourceType resourceType,
        string resourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(new ListTagsForResourceRequest
            {
                ResourceType = resourceType,
                ResourceId = resourceId
            });
            return new Route53TagsResult(ResourceTagSet: resp.ResourceTagSet);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceId}'");
        }
    }

    // ── VPC Association ─────────────────────────────────────────────────

    /// <summary>
    /// Associate a VPC with a private hosted zone.
    /// </summary>
    public static async Task AssociateVPCWithHostedZoneAsync(
        AssociateVPCWithHostedZoneRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateVPCWithHostedZoneAsync(request);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to associate VPC with hosted zone");
        }
    }

    /// <summary>
    /// Disassociate a VPC from a private hosted zone.
    /// </summary>
    public static async Task DisassociateVPCFromHostedZoneAsync(
        DisassociateVPCFromHostedZoneRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateVPCFromHostedZoneAsync(request);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to disassociate VPC from hosted zone");
        }
    }

    // ── Query Logging ───────────────────────────────────────────────────

    /// <summary>
    /// Create a query logging configuration.
    /// </summary>
    public static async Task<CreateQueryLoggingConfigResult> CreateQueryLoggingConfigAsync(
        string hostedZoneId,
        string cloudWatchLogsLogGroupArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateQueryLoggingConfigAsync(new CreateQueryLoggingConfigRequest
            {
                HostedZoneId = hostedZoneId,
                CloudWatchLogsLogGroupArn = cloudWatchLogsLogGroupArn
            });
            return new CreateQueryLoggingConfigResult(
                QueryLoggingConfig: resp.QueryLoggingConfig,
                Location: resp.Location);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create query logging config");
        }
    }

    /// <summary>
    /// Delete a query logging configuration.
    /// </summary>
    public static async Task DeleteQueryLoggingConfigAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteQueryLoggingConfigAsync(new DeleteQueryLoggingConfigRequest
            {
                Id = id
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete query logging config '{id}'");
        }
    }

    /// <summary>
    /// Get a query logging configuration.
    /// </summary>
    public static async Task<GetQueryLoggingConfigResult> GetQueryLoggingConfigAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueryLoggingConfigAsync(new GetQueryLoggingConfigRequest
            {
                Id = id
            });
            return new GetQueryLoggingConfigResult(QueryLoggingConfig: resp.QueryLoggingConfig);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get query logging config '{id}'");
        }
    }

    /// <summary>
    /// List query logging configurations.
    /// </summary>
    public static async Task<ListQueryLoggingConfigsResult> ListQueryLoggingConfigsAsync(
        string? hostedZoneId = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListQueryLoggingConfigsRequest();
        if (hostedZoneId != null) request.HostedZoneId = hostedZoneId;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value.ToString();

        try
        {
            var resp = await client.ListQueryLoggingConfigsAsync(request);
            return new ListQueryLoggingConfigsResult(
                QueryLoggingConfigs: resp.QueryLoggingConfigs,
                NextToken: resp.NextToken);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list query logging configs");
        }
    }

    // ── Reusable Delegation Sets ────────────────────────────────────────

    /// <summary>
    /// Create a reusable delegation set.
    /// </summary>
    public static async Task<CreateReusableDelegationSetResult> CreateReusableDelegationSetAsync(
        string callerReference,
        string? hostedZoneId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateReusableDelegationSetRequest
        {
            CallerReference = callerReference
        };
        if (hostedZoneId != null) request.HostedZoneId = hostedZoneId;

        try
        {
            var resp = await client.CreateReusableDelegationSetAsync(request);
            return new CreateReusableDelegationSetResult(
                DelegationSet: resp.DelegationSet,
                Location: resp.Location);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create reusable delegation set");
        }
    }

    /// <summary>
    /// Delete a reusable delegation set.
    /// </summary>
    public static async Task DeleteReusableDelegationSetAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteReusableDelegationSetAsync(new DeleteReusableDelegationSetRequest
            {
                Id = id
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete reusable delegation set '{id}'");
        }
    }

    /// <summary>
    /// Get a reusable delegation set.
    /// </summary>
    public static async Task<GetReusableDelegationSetResult> GetReusableDelegationSetAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetReusableDelegationSetAsync(new GetReusableDelegationSetRequest
            {
                Id = id
            });
            return new GetReusableDelegationSetResult(DelegationSet: resp.DelegationSet);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get reusable delegation set '{id}'");
        }
    }

    /// <summary>
    /// List reusable delegation sets.
    /// </summary>
    public static async Task<ListReusableDelegationSetsResult> ListReusableDelegationSetsAsync(
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListReusableDelegationSetsRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListReusableDelegationSetsAsync(request);
            return new ListReusableDelegationSetsResult(
                DelegationSets: resp.DelegationSets,
                NextMarker: resp.NextMarker,
                IsTruncated: resp.IsTruncated ?? false);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list reusable delegation sets");
        }
    }

    // ── Traffic Policies ────────────────────────────────────────────────

    /// <summary>
    /// Create a traffic policy.
    /// </summary>
    public static async Task<CreateTrafficPolicyResult> CreateTrafficPolicyAsync(
        CreateTrafficPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTrafficPolicyAsync(request);
            return new CreateTrafficPolicyResult(
                TrafficPolicy: resp.TrafficPolicy,
                Location: resp.Location);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create traffic policy");
        }
    }

    /// <summary>
    /// Delete a traffic policy.
    /// </summary>
    public static async Task DeleteTrafficPolicyAsync(
        string id,
        int version,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTrafficPolicyAsync(new DeleteTrafficPolicyRequest
            {
                Id = id,
                Version = version
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete traffic policy '{id}'");
        }
    }

    /// <summary>
    /// Get a traffic policy.
    /// </summary>
    public static async Task<GetTrafficPolicyResult> GetTrafficPolicyAsync(
        string id,
        int version,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTrafficPolicyAsync(new GetTrafficPolicyRequest
            {
                Id = id,
                Version = version
            });
            return new GetTrafficPolicyResult(TrafficPolicy: resp.TrafficPolicy);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get traffic policy '{id}'");
        }
    }

    /// <summary>
    /// List traffic policies.
    /// </summary>
    public static async Task<ListTrafficPoliciesResult> ListTrafficPoliciesAsync(
        string? trafficPolicyIdMarker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTrafficPoliciesRequest();
        if (trafficPolicyIdMarker != null) request.TrafficPolicyIdMarker = trafficPolicyIdMarker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListTrafficPoliciesAsync(request);
            return new ListTrafficPoliciesResult(
                TrafficPolicySummaries: resp.TrafficPolicySummaries,
                IsTruncated: resp.IsTruncated ?? false,
                TrafficPolicyIdMarker: resp.TrafficPolicyIdMarker);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list traffic policies");
        }
    }

    // ── DNSSEC ──────────────────────────────────────────────────────────

    /// <summary>
    /// Get DNSSEC information for a hosted zone.
    /// </summary>
    public static async Task<GetDNSSECResult> GetDNSSECAsync(
        string hostedZoneId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDNSSECAsync(new GetDNSSECRequest
            {
                HostedZoneId = hostedZoneId
            });
            return new GetDNSSECResult(
                Status: resp.Status,
                KeySigningKeys: resp.KeySigningKeys);
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get DNSSEC for hosted zone '{hostedZoneId}'");
        }
    }

    /// <summary>
    /// Enable DNSSEC signing for a hosted zone.
    /// </summary>
    public static async Task EnableHostedZoneDNSSECAsync(
        string hostedZoneId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableHostedZoneDNSSECAsync(new EnableHostedZoneDNSSECRequest
            {
                HostedZoneId = hostedZoneId
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable DNSSEC for hosted zone '{hostedZoneId}'");
        }
    }

    /// <summary>
    /// Disable DNSSEC signing for a hosted zone.
    /// </summary>
    public static async Task DisableHostedZoneDNSSECAsync(
        string hostedZoneId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableHostedZoneDNSSECAsync(new DisableHostedZoneDNSSECRequest
            {
                HostedZoneId = hostedZoneId
            });
        }
        catch (AmazonRoute53Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable DNSSEC for hosted zone '{hostedZoneId}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateHostedZoneAsync"/>.</summary>
    public static CreateHostedZoneResult CreateHostedZone(CreateHostedZoneRequest request, RegionEndpoint? region = null)
        => CreateHostedZoneAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteHostedZoneAsync"/>.</summary>
    public static void DeleteHostedZone(string hostedZoneId, RegionEndpoint? region = null)
        => DeleteHostedZoneAsync(hostedZoneId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetHostedZoneAsync"/>.</summary>
    public static GetHostedZoneResult GetHostedZone(string hostedZoneId, RegionEndpoint? region = null)
        => GetHostedZoneAsync(hostedZoneId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListHostedZonesAsync"/>.</summary>
    public static ListHostedZonesResult ListHostedZones(string? marker = null, int? maxItems = null, string? delegationSetId = null, RegionEndpoint? region = null)
        => ListHostedZonesAsync(marker, maxItems, delegationSetId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ChangeResourceRecordSetsAsync"/>.</summary>
    public static ChangeResourceRecordSetsResult ChangeResourceRecordSets(ChangeResourceRecordSetsRequest request, RegionEndpoint? region = null)
        => ChangeResourceRecordSetsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListResourceRecordSetsAsync"/>.</summary>
    public static ListResourceRecordSetsResult ListResourceRecordSets(string hostedZoneId, string? startRecordName = null, string? startRecordType = null, string? startRecordIdentifier = null, int? maxItems = null, RegionEndpoint? region = null)
        => ListResourceRecordSetsAsync(hostedZoneId, startRecordName, startRecordType, startRecordIdentifier, maxItems, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetHostedZoneCountAsync"/>.</summary>
    public static GetHostedZoneCountResult GetHostedZoneCount(RegionEndpoint? region = null)
        => GetHostedZoneCountAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateHealthCheckAsync"/>.</summary>
    public static CreateHealthCheckResult CreateHealthCheck(CreateHealthCheckRequest request, RegionEndpoint? region = null)
        => CreateHealthCheckAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteHealthCheckAsync"/>.</summary>
    public static void DeleteHealthCheck(string healthCheckId, RegionEndpoint? region = null)
        => DeleteHealthCheckAsync(healthCheckId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetHealthCheckAsync"/>.</summary>
    public static GetHealthCheckResult GetHealthCheck(string healthCheckId, RegionEndpoint? region = null)
        => GetHealthCheckAsync(healthCheckId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListHealthChecksAsync"/>.</summary>
    public static ListHealthChecksResult ListHealthChecks(string? marker = null, int? maxItems = null, RegionEndpoint? region = null)
        => ListHealthChecksAsync(marker, maxItems, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateHealthCheckAsync"/>.</summary>
    public static void UpdateHealthCheck(UpdateHealthCheckRequest request, RegionEndpoint? region = null)
        => UpdateHealthCheckAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetHealthCheckStatusAsync"/>.</summary>
    public static GetHealthCheckStatusResult GetHealthCheckStatus(string healthCheckId, RegionEndpoint? region = null)
        => GetHealthCheckStatusAsync(healthCheckId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TestDNSAnswerAsync"/>.</summary>
    public static TestDNSAnswerResult TestDNSAnswer(TestDNSAnswerRequest request, RegionEndpoint? region = null)
        => TestDNSAnswerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ChangeTagsForResourceAsync"/>.</summary>
    public static void ChangeTagsForResource(ChangeTagsForResourceRequest request, RegionEndpoint? region = null)
        => ChangeTagsForResourceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static Route53TagsResult ListTagsForResource(TagResourceType resourceType, string resourceId, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceType, resourceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateVPCWithHostedZoneAsync"/>.</summary>
    public static void AssociateVPCWithHostedZone(AssociateVPCWithHostedZoneRequest request, RegionEndpoint? region = null)
        => AssociateVPCWithHostedZoneAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateVPCFromHostedZoneAsync"/>.</summary>
    public static void DisassociateVPCFromHostedZone(DisassociateVPCFromHostedZoneRequest request, RegionEndpoint? region = null)
        => DisassociateVPCFromHostedZoneAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateQueryLoggingConfigAsync"/>.</summary>
    public static CreateQueryLoggingConfigResult CreateQueryLoggingConfig(string hostedZoneId, string cloudWatchLogsLogGroupArn, RegionEndpoint? region = null)
        => CreateQueryLoggingConfigAsync(hostedZoneId, cloudWatchLogsLogGroupArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteQueryLoggingConfigAsync"/>.</summary>
    public static void DeleteQueryLoggingConfig(string id, RegionEndpoint? region = null)
        => DeleteQueryLoggingConfigAsync(id, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueryLoggingConfigAsync"/>.</summary>
    public static GetQueryLoggingConfigResult GetQueryLoggingConfig(string id, RegionEndpoint? region = null)
        => GetQueryLoggingConfigAsync(id, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueryLoggingConfigsAsync"/>.</summary>
    public static ListQueryLoggingConfigsResult ListQueryLoggingConfigs(string? hostedZoneId = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListQueryLoggingConfigsAsync(hostedZoneId, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateReusableDelegationSetAsync"/>.</summary>
    public static CreateReusableDelegationSetResult CreateReusableDelegationSet(string callerReference, string? hostedZoneId = null, RegionEndpoint? region = null)
        => CreateReusableDelegationSetAsync(callerReference, hostedZoneId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteReusableDelegationSetAsync"/>.</summary>
    public static void DeleteReusableDelegationSet(string id, RegionEndpoint? region = null)
        => DeleteReusableDelegationSetAsync(id, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetReusableDelegationSetAsync"/>.</summary>
    public static GetReusableDelegationSetResult GetReusableDelegationSet(string id, RegionEndpoint? region = null)
        => GetReusableDelegationSetAsync(id, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListReusableDelegationSetsAsync"/>.</summary>
    public static ListReusableDelegationSetsResult ListReusableDelegationSets(string? marker = null, int? maxItems = null, RegionEndpoint? region = null)
        => ListReusableDelegationSetsAsync(marker, maxItems, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTrafficPolicyAsync"/>.</summary>
    public static CreateTrafficPolicyResult CreateTrafficPolicy(CreateTrafficPolicyRequest request, RegionEndpoint? region = null)
        => CreateTrafficPolicyAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTrafficPolicyAsync"/>.</summary>
    public static void DeleteTrafficPolicy(string id, int version, RegionEndpoint? region = null)
        => DeleteTrafficPolicyAsync(id, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTrafficPolicyAsync"/>.</summary>
    public static GetTrafficPolicyResult GetTrafficPolicy(string id, int version, RegionEndpoint? region = null)
        => GetTrafficPolicyAsync(id, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTrafficPoliciesAsync"/>.</summary>
    public static ListTrafficPoliciesResult ListTrafficPolicies(string? trafficPolicyIdMarker = null, int? maxItems = null, RegionEndpoint? region = null)
        => ListTrafficPoliciesAsync(trafficPolicyIdMarker, maxItems, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDNSSECAsync"/>.</summary>
    public static GetDNSSECResult GetDNSSEC(string hostedZoneId, RegionEndpoint? region = null)
        => GetDNSSECAsync(hostedZoneId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableHostedZoneDNSSECAsync"/>.</summary>
    public static void EnableHostedZoneDNSSEC(string hostedZoneId, RegionEndpoint? region = null)
        => EnableHostedZoneDNSSECAsync(hostedZoneId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableHostedZoneDNSSECAsync"/>.</summary>
    public static void DisableHostedZoneDNSSEC(string hostedZoneId, RegionEndpoint? region = null)
        => DisableHostedZoneDNSSECAsync(hostedZoneId, region).GetAwaiter().GetResult();

}
