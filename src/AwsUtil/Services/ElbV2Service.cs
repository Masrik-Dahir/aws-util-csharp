using Amazon;
using Amazon.ElasticLoadBalancingV2;
using Amazon.ElasticLoadBalancingV2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Elastic Load Balancing V2 operations.
/// </summary>
public sealed record ElbCreateLoadBalancerResult(
    List<LoadBalancer>? LoadBalancers = null);

public sealed record ElbDeleteLoadBalancerResult(bool Success = true);

public sealed record ElbDescribeLoadBalancersResult(
    List<LoadBalancer>? LoadBalancers = null, string? NextMarker = null);

public sealed record ElbModifyLoadBalancerAttributesResult(
    List<LoadBalancerAttribute>? Attributes = null);

public sealed record ElbCreateTargetGroupResult(
    List<TargetGroup>? TargetGroups = null);

public sealed record ElbDeleteTargetGroupResult(bool Success = true);

public sealed record ElbDescribeTargetGroupsResult(
    List<TargetGroup>? TargetGroups = null, string? NextMarker = null);

public sealed record ElbModifyTargetGroupResult(
    List<TargetGroup>? TargetGroups = null);

public sealed record ElbModifyTargetGroupAttributesResult(
    List<TargetGroupAttribute>? Attributes = null);

public sealed record ElbRegisterTargetsResult(bool Success = true);
public sealed record ElbDeregisterTargetsResult(bool Success = true);

public sealed record ElbDescribeTargetHealthResult(
    List<TargetHealthDescription>? TargetHealthDescriptions = null);

public sealed record ElbCreateListenerResult(List<Listener>? Listeners = null);
public sealed record ElbDeleteListenerResult(bool Success = true);

public sealed record ElbDescribeListenersResult(
    List<Listener>? Listeners = null, string? NextMarker = null);

public sealed record ElbModifyListenerResult(List<Listener>? Listeners = null);

public sealed record ElbCreateRuleResult(List<Rule>? Rules = null);
public sealed record ElbDeleteRuleResult(bool Success = true);

public sealed record ElbDescribeRulesResult(
    List<Rule>? Rules = null, string? NextMarker = null);

public sealed record ElbModifyRuleResult(List<Rule>? Rules = null);

public sealed record ElbSetRulePrioritiesResult(List<Rule>? Rules = null);

public sealed record ElbAddTagsResult(bool Success = true);
public sealed record ElbRemoveTagsResult(bool Success = true);

public sealed record ElbDescribeTagsResult(
    List<TagDescription>? TagDescriptions = null);

public sealed record ElbSetSecurityGroupsResult(
    List<string>? SecurityGroupIds = null);

public sealed record ElbSetSubnetsResult(
    List<AvailabilityZone>? AvailabilityZones = null);

public sealed record ElbDescribeAccountLimitsResult(
    List<Limit>? Limits = null, string? NextMarker = null);

public sealed record ElbDescribeLoadBalancerAttributesResult(
    List<LoadBalancerAttribute>? Attributes = null);

public sealed record ElbDescribeTargetGroupAttributesResult(
    List<TargetGroupAttribute>? Attributes = null);

public sealed record ElbDescribeSSLPoliciesResult(
    List<SslPolicy>? SslPolicies = null, string? NextMarker = null);

public sealed record ElbAddListenerCertificatesResult(
    List<Certificate>? Certificates = null);

public sealed record ElbRemoveListenerCertificatesResult(bool Success = true);

public sealed record ElbDescribeListenerCertificatesResult(
    List<Certificate>? Certificates = null, string? NextMarker = null);

/// <summary>
/// Utility helpers for Elastic Load Balancing V2 (ALB/NLB/GWLB).
/// </summary>
public static class ElbV2Service
{
    private static AmazonElasticLoadBalancingV2Client GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonElasticLoadBalancingV2Client>(region);

    /// <summary>
    /// Create a load balancer.
    /// </summary>
    public static async Task<ElbCreateLoadBalancerResult>
        CreateLoadBalancerAsync(
            CreateLoadBalancerRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateLoadBalancerAsync(request);
            return new ElbCreateLoadBalancerResult(
                LoadBalancers: resp.LoadBalancers);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create load balancer");
        }
    }

    /// <summary>
    /// Delete a load balancer.
    /// </summary>
    public static async Task<ElbDeleteLoadBalancerResult>
        DeleteLoadBalancerAsync(
            string loadBalancerArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteLoadBalancerAsync(
                new DeleteLoadBalancerRequest
                {
                    LoadBalancerArn = loadBalancerArn
                });
            return new ElbDeleteLoadBalancerResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete load balancer");
        }
    }

    /// <summary>
    /// Describe load balancers.
    /// </summary>
    public static async Task<ElbDescribeLoadBalancersResult>
        DescribeLoadBalancersAsync(
            DescribeLoadBalancersRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLoadBalancersAsync(request);
            return new ElbDescribeLoadBalancersResult(
                LoadBalancers: resp.LoadBalancers,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe load balancers");
        }
    }

    /// <summary>
    /// Modify load balancer attributes.
    /// </summary>
    public static async Task<ElbModifyLoadBalancerAttributesResult>
        ModifyLoadBalancerAttributesAsync(
            ModifyLoadBalancerAttributesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyLoadBalancerAttributesAsync(request);
            return new ElbModifyLoadBalancerAttributesResult(
                Attributes: resp.Attributes);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify load balancer attributes");
        }
    }

    /// <summary>
    /// Create a target group.
    /// </summary>
    public static async Task<ElbCreateTargetGroupResult>
        CreateTargetGroupAsync(
            CreateTargetGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTargetGroupAsync(request);
            return new ElbCreateTargetGroupResult(
                TargetGroups: resp.TargetGroups);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create target group");
        }
    }

    /// <summary>
    /// Delete a target group.
    /// </summary>
    public static async Task<ElbDeleteTargetGroupResult>
        DeleteTargetGroupAsync(
            string targetGroupArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTargetGroupAsync(
                new DeleteTargetGroupRequest
                {
                    TargetGroupArn = targetGroupArn
                });
            return new ElbDeleteTargetGroupResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete target group");
        }
    }

    /// <summary>
    /// Describe target groups.
    /// </summary>
    public static async Task<ElbDescribeTargetGroupsResult>
        DescribeTargetGroupsAsync(
            DescribeTargetGroupsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTargetGroupsAsync(request);
            return new ElbDescribeTargetGroupsResult(
                TargetGroups: resp.TargetGroups,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe target groups");
        }
    }

    /// <summary>
    /// Modify a target group.
    /// </summary>
    public static async Task<ElbModifyTargetGroupResult>
        ModifyTargetGroupAsync(
            ModifyTargetGroupRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyTargetGroupAsync(request);
            return new ElbModifyTargetGroupResult(
                TargetGroups: resp.TargetGroups);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify target group");
        }
    }

    /// <summary>
    /// Modify target group attributes.
    /// </summary>
    public static async Task<ElbModifyTargetGroupAttributesResult>
        ModifyTargetGroupAttributesAsync(
            ModifyTargetGroupAttributesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyTargetGroupAttributesAsync(request);
            return new ElbModifyTargetGroupAttributesResult(
                Attributes: resp.Attributes);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to modify target group attributes");
        }
    }

    /// <summary>
    /// Register targets with a target group.
    /// </summary>
    public static async Task<ElbRegisterTargetsResult> RegisterTargetsAsync(
        string targetGroupArn, List<TargetDescription> targets,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RegisterTargetsAsync(new RegisterTargetsRequest
            {
                TargetGroupArn = targetGroupArn,
                Targets = targets
            });
            return new ElbRegisterTargetsResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to register targets");
        }
    }

    /// <summary>
    /// Deregister targets from a target group.
    /// </summary>
    public static async Task<ElbDeregisterTargetsResult>
        DeregisterTargetsAsync(
            string targetGroupArn, List<TargetDescription> targets,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeregisterTargetsAsync(
                new DeregisterTargetsRequest
                {
                    TargetGroupArn = targetGroupArn,
                    Targets = targets
                });
            return new ElbDeregisterTargetsResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to deregister targets");
        }
    }

    /// <summary>
    /// Describe the health of targets in a target group.
    /// </summary>
    public static async Task<ElbDescribeTargetHealthResult>
        DescribeTargetHealthAsync(
            DescribeTargetHealthRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTargetHealthAsync(request);
            return new ElbDescribeTargetHealthResult(
                TargetHealthDescriptions: resp.TargetHealthDescriptions);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe target health");
        }
    }

    /// <summary>
    /// Create a listener for a load balancer.
    /// </summary>
    public static async Task<ElbCreateListenerResult> CreateListenerAsync(
        CreateListenerRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateListenerAsync(request);
            return new ElbCreateListenerResult(Listeners: resp.Listeners);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create listener");
        }
    }

    /// <summary>
    /// Delete a listener.
    /// </summary>
    public static async Task<ElbDeleteListenerResult> DeleteListenerAsync(
        string listenerArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteListenerAsync(
                new DeleteListenerRequest { ListenerArn = listenerArn });
            return new ElbDeleteListenerResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete listener");
        }
    }

    /// <summary>
    /// Describe listeners for a load balancer.
    /// </summary>
    public static async Task<ElbDescribeListenersResult>
        DescribeListenersAsync(
            DescribeListenersRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeListenersAsync(request);
            return new ElbDescribeListenersResult(
                Listeners: resp.Listeners,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe listeners");
        }
    }

    /// <summary>
    /// Modify a listener.
    /// </summary>
    public static async Task<ElbModifyListenerResult> ModifyListenerAsync(
        ModifyListenerRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyListenerAsync(request);
            return new ElbModifyListenerResult(Listeners: resp.Listeners);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to modify listener");
        }
    }

    /// <summary>
    /// Create a rule for a listener.
    /// </summary>
    public static async Task<ElbCreateRuleResult> CreateRuleAsync(
        CreateRuleRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRuleAsync(request);
            return new ElbCreateRuleResult(Rules: resp.Rules);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create rule");
        }
    }

    /// <summary>
    /// Delete a rule.
    /// </summary>
    public static async Task<ElbDeleteRuleResult> DeleteRuleAsync(
        string ruleArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRuleAsync(
                new DeleteRuleRequest { RuleArn = ruleArn });
            return new ElbDeleteRuleResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete rule");
        }
    }

    /// <summary>
    /// Describe rules for a listener.
    /// </summary>
    public static async Task<ElbDescribeRulesResult> DescribeRulesAsync(
        DescribeRulesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRulesAsync(request);
            return new ElbDescribeRulesResult(
                Rules: resp.Rules,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe rules");
        }
    }

    /// <summary>
    /// Modify a rule.
    /// </summary>
    public static async Task<ElbModifyRuleResult> ModifyRuleAsync(
        ModifyRuleRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ModifyRuleAsync(request);
            return new ElbModifyRuleResult(Rules: resp.Rules);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to modify rule");
        }
    }

    /// <summary>
    /// Set rule priorities.
    /// </summary>
    public static async Task<ElbSetRulePrioritiesResult>
        SetRulePrioritiesAsync(
            SetRulePrioritiesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SetRulePrioritiesAsync(request);
            return new ElbSetRulePrioritiesResult(Rules: resp.Rules);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to set rule priorities");
        }
    }

    /// <summary>
    /// Add tags to load balancer resources.
    /// </summary>
    public static async Task<ElbAddTagsResult> AddTagsAsync(
        AddTagsRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsAsync(request);
            return new ElbAddTagsResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add tags");
        }
    }

    /// <summary>
    /// Remove tags from load balancer resources.
    /// </summary>
    public static async Task<ElbRemoveTagsResult> RemoveTagsAsync(
        RemoveTagsRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsAsync(request);
            return new ElbRemoveTagsResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove tags");
        }
    }

    /// <summary>
    /// Describe tags for load balancer resources.
    /// </summary>
    public static async Task<ElbDescribeTagsResult> DescribeTagsAsync(
        DescribeTagsRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTagsAsync(request);
            return new ElbDescribeTagsResult(
                TagDescriptions: resp.TagDescriptions);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe tags");
        }
    }

    /// <summary>
    /// Set the security groups for a load balancer.
    /// </summary>
    public static async Task<ElbSetSecurityGroupsResult>
        SetSecurityGroupsAsync(
            string loadBalancerArn, List<string> securityGroups,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SetSecurityGroupsAsync(
                new SetSecurityGroupsRequest
                {
                    LoadBalancerArn = loadBalancerArn,
                    SecurityGroups = securityGroups
                });
            return new ElbSetSecurityGroupsResult(
                SecurityGroupIds: resp.SecurityGroupIds);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to set security groups");
        }
    }

    /// <summary>
    /// Set the subnets for a load balancer.
    /// </summary>
    public static async Task<ElbSetSubnetsResult> SetSubnetsAsync(
        SetSubnetsRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SetSubnetsAsync(request);
            return new ElbSetSubnetsResult(
                AvailabilityZones: resp.AvailabilityZones);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to set subnets");
        }
    }

    /// <summary>
    /// Describe account limits for Elastic Load Balancing.
    /// </summary>
    public static async Task<ElbDescribeAccountLimitsResult>
        DescribeAccountLimitsAsync(
            string? marker = null, int? pageSize = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAccountLimitsRequest();
        if (marker != null) request.Marker = marker;
        if (pageSize.HasValue) request.PageSize = pageSize.Value;

        try
        {
            var resp = await client.DescribeAccountLimitsAsync(request);
            return new ElbDescribeAccountLimitsResult(
                Limits: resp.Limits,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe account limits");
        }
    }

    /// <summary>
    /// Describe load balancer attributes.
    /// </summary>
    public static async Task<ElbDescribeLoadBalancerAttributesResult>
        DescribeLoadBalancerAttributesAsync(
            string loadBalancerArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLoadBalancerAttributesAsync(
                new DescribeLoadBalancerAttributesRequest
                {
                    LoadBalancerArn = loadBalancerArn
                });
            return new ElbDescribeLoadBalancerAttributesResult(
                Attributes: resp.Attributes);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe load balancer attributes");
        }
    }

    /// <summary>
    /// Describe target group attributes.
    /// </summary>
    public static async Task<ElbDescribeTargetGroupAttributesResult>
        DescribeTargetGroupAttributesAsync(
            string targetGroupArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTargetGroupAttributesAsync(
                new DescribeTargetGroupAttributesRequest
                {
                    TargetGroupArn = targetGroupArn
                });
            return new ElbDescribeTargetGroupAttributesResult(
                Attributes: resp.Attributes);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe target group attributes");
        }
    }

    /// <summary>
    /// Describe SSL policies.
    /// </summary>
    public static async Task<ElbDescribeSSLPoliciesResult>
        DescribeSSLPoliciesAsync(
            DescribeSSLPoliciesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSSLPoliciesAsync(request);
            return new ElbDescribeSSLPoliciesResult(
                SslPolicies: resp.SslPolicies,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe SSL policies");
        }
    }

    /// <summary>
    /// Add certificates to a listener.
    /// </summary>
    public static async Task<ElbAddListenerCertificatesResult>
        AddListenerCertificatesAsync(
            AddListenerCertificatesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.AddListenerCertificatesAsync(request);
            return new ElbAddListenerCertificatesResult(
                Certificates: resp.Certificates);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to add listener certificates");
        }
    }

    /// <summary>
    /// Remove certificates from a listener.
    /// </summary>
    public static async Task<ElbRemoveListenerCertificatesResult>
        RemoveListenerCertificatesAsync(
            RemoveListenerCertificatesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveListenerCertificatesAsync(request);
            return new ElbRemoveListenerCertificatesResult();
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to remove listener certificates");
        }
    }

    /// <summary>
    /// Describe certificates for a listener.
    /// </summary>
    public static async Task<ElbDescribeListenerCertificatesResult>
        DescribeListenerCertificatesAsync(
            DescribeListenerCertificatesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeListenerCertificatesAsync(request);
            return new ElbDescribeListenerCertificatesResult(
                Certificates: resp.Certificates,
                NextMarker: resp.NextMarker);
        }
        catch (AmazonElasticLoadBalancingV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe listener certificates");
        }
    }
}
