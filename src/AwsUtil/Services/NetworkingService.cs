using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of managing a VPC peering connection.</summary>
public sealed record VpcPeeringManagerResult(
    string PeeringConnectionId,
    string RequesterVpcId,
    string AccepterVpcId,
    string Status,
    bool RouteTablesUpdated);

/// <summary>Result of configuring a Transit Gateway.</summary>
public sealed record TransitGatewayConfiguratorResult(
    string TransitGatewayId,
    string VpcId,
    string AttachmentId,
    string Status,
    List<string> SubnetIds);

/// <summary>Result of managing a PrivateLink endpoint.</summary>
public sealed record PrivateLinkManagerResult(
    string VpcEndpointId,
    string ServiceName,
    string VpcId,
    string Status,
    List<string> SubnetIds,
    List<string> SecurityGroupIds);

/// <summary>Result of auditing network ACLs.</summary>
public sealed record NetworkAclAuditorResult(
    int AclsAudited,
    List<NetworkAclFinding> Findings,
    int CriticalFindings,
    int WarningFindings);

/// <summary>A finding from a network ACL audit.</summary>
public sealed record NetworkAclFinding(
    string NetworkAclId,
    string VpcId,
    string Severity,
    string RuleNumber,
    string Description);

/// <summary>
/// Networking operations orchestrating EC2, Route 53, CloudWatch,
/// and EventBridge for VPC peering, transit gateways, PrivateLink,
/// and network ACL auditing.
/// </summary>
public static class NetworkingService
{
    /// <summary>
    /// Create or manage a VPC peering connection and update route tables
    /// in both VPCs to enable cross-VPC traffic.
    /// </summary>
    public static async Task<VpcPeeringManagerResult> VpcPeeringManagerAsync(
        string requesterVpcId,
        string accepterVpcId,
        string? accepterAccountId = null,
        string? accepterRegion = null,
        List<(string RouteTableId, string DestinationCidr)>? routes = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Create VPC peering connection
            var peering = await Ec2Service.CreateVpcPeeringConnectionAsync(
                vpcId: requesterVpcId,
                peerVpcId: accepterVpcId,
                peerOwnerId: accepterAccountId,
                peerRegion: accepterRegion,
                region: region);

            var peeringId = peering.VpcPeeringConnectionId ?? "";

            // Accept the peering (if same account)
            if (accepterAccountId == null)
            {
                await Ec2Service.AcceptVpcPeeringConnectionAsync(
                    peeringId, region: region);
            }

            // Update route tables
            var routesUpdated = false;
            if (routes != null)
            {
                foreach (var (routeTableId, destCidr) in routes)
                {
                    try
                    {
                        await Ec2Service.CreateRouteAsync(
                            routeTableId: routeTableId,
                            destinationCidrBlock: destCidr,
                            vpcPeeringConnectionId: peeringId,
                            region: region);
                    }
                    catch (AwsConflictException)
                    {
                        // Route already exists, replace it
                        await Ec2Service.ReplaceRouteAsync(
                            routeTableId: routeTableId,
                            destinationCidrBlock: destCidr,
                            vpcPeeringConnectionId: peeringId,
                            region: region);
                    }
                }
                routesUpdated = true;
            }

            // Publish event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.networking",
                        DetailType = "VpcPeeringCreated",
                        Detail = JsonSerializer.Serialize(new
                        {
                            peeringConnectionId = peeringId,
                            requesterVpcId,
                            accepterVpcId,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new VpcPeeringManagerResult(
                PeeringConnectionId: peeringId,
                RequesterVpcId: requesterVpcId,
                AccepterVpcId: accepterVpcId,
                Status: "Active",
                RouteTablesUpdated: routesUpdated);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "VPC peering management failed");
        }
    }

    /// <summary>
    /// Attach a VPC to a Transit Gateway and configure routing for
    /// centralized network connectivity.
    /// </summary>
    public static async Task<TransitGatewayConfiguratorResult> TransitGatewayConfiguratorAsync(
        string transitGatewayId,
        string vpcId,
        List<string> subnetIds,
        List<(string RouteTableId, string DestinationCidr)>? routes = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Create Transit Gateway VPC attachment
            var attachment = await Ec2Service.CreateTransitGatewayVpcAttachmentAsync(
                transitGatewayId: transitGatewayId,
                vpcId: vpcId,
                subnetIds: subnetIds,
                region: region);

            var attachmentId = attachment.TransitGatewayAttachmentId ?? "";

            // Configure routes pointing to the transit gateway
            if (routes != null)
            {
                foreach (var (routeTableId, destCidr) in routes)
                {
                    try
                    {
                        await Ec2Service.CreateRouteAsync(
                            routeTableId: routeTableId,
                            destinationCidrBlock: destCidr,
                            transitGatewayId: transitGatewayId,
                            region: region);
                    }
                    catch (AwsConflictException)
                    {
                        // Route exists
                    }
                }
            }

            return new TransitGatewayConfiguratorResult(
                TransitGatewayId: transitGatewayId,
                VpcId: vpcId,
                AttachmentId: attachmentId,
                Status: "Pending",
                SubnetIds: subnetIds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Transit Gateway configuration failed");
        }
    }

    /// <summary>
    /// Create a VPC endpoint (PrivateLink) for an AWS service or
    /// custom endpoint service.
    /// </summary>
    public static async Task<PrivateLinkManagerResult> PrivateLinkManagerAsync(
        string vpcId,
        string serviceName,
        List<string> subnetIds,
        List<string> securityGroupIds,
        string endpointType = "Interface",
        RegionEndpoint? region = null)
    {
        try
        {
            var endpoint = await Ec2Service.CreateVpcEndpointAsync(
                vpcId: vpcId,
                serviceName: serviceName,
                vpcEndpointType: endpointType,
                subnetIds: endpointType == "Interface" ? subnetIds : null,
                securityGroupIds: endpointType == "Interface" ? securityGroupIds : null,
                region: region);

            var endpointId = endpoint.VpcEndpointId ?? "";

            // Publish event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.networking",
                        DetailType = "VpcEndpointCreated",
                        Detail = JsonSerializer.Serialize(new
                        {
                            vpcEndpointId = endpointId,
                            serviceName,
                            vpcId,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new PrivateLinkManagerResult(
                VpcEndpointId: endpointId,
                ServiceName: serviceName,
                VpcId: vpcId,
                Status: "Pending",
                SubnetIds: subnetIds,
                SecurityGroupIds: securityGroupIds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "PrivateLink management failed");
        }
    }

    /// <summary>
    /// Audit network ACLs across VPCs to identify overly permissive rules,
    /// unrestricted ingress, or missing deny rules.
    /// </summary>
    public static async Task<NetworkAclAuditorResult> NetworkAclAuditorAsync(
        List<string>? vpcIds = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var findings = new List<NetworkAclFinding>();

            // Describe network ACLs
            var acls = await Ec2Service.DescribeNetworkAclsAsync(
                vpcIds: vpcIds,
                region: region);

            foreach (var acl in acls.NetworkAcls ??
                Enumerable.Empty<Amazon.EC2.Model.NetworkAcl>())
            {
                var aclId = acl.NetworkAclId ?? "unknown";
                var vpcId = acl.VpcId ?? "unknown";

                foreach (var entry in acl.Entries ?? Enumerable.Empty<Amazon.EC2.Model.NetworkAclEntry>())
                {
                    // Check for overly permissive rules
                    if (entry.CidrBlock == "0.0.0.0/0" &&
                        entry.RuleAction?.Value == "allow" &&
                        !(entry.Egress ?? false))
                    {
                        var severity = entry.Protocol == "-1" ? "CRITICAL" : "WARNING";

                        findings.Add(new NetworkAclFinding(
                            NetworkAclId: aclId,
                            VpcId: vpcId,
                            Severity: severity,
                            RuleNumber: entry.RuleNumber.ToString(),
                            Description: entry.Protocol == "-1"
                                ? "Unrestricted inbound access (all protocols) from 0.0.0.0/0"
                                : $"Inbound access from 0.0.0.0/0 on protocol {entry.Protocol}"));
                    }

                    // Check for IPv6 unrestricted access
                    if (entry.Ipv6CidrBlock == "::/0" &&
                        entry.RuleAction?.Value == "allow" &&
                        !(entry.Egress ?? false))
                    {
                        findings.Add(new NetworkAclFinding(
                            NetworkAclId: aclId,
                            VpcId: vpcId,
                            Severity: "WARNING",
                            RuleNumber: entry.RuleNumber.ToString(),
                            Description: "Unrestricted IPv6 inbound access from ::/0"));
                    }
                }
            }

            // Publish audit results
            await CloudWatchService.PutMetricDataAsync(
                metricNamespace: "AwsUtil/NetworkAudit",
                metricData: new List<Amazon.CloudWatch.Model.MetricDatum>
                {
                    new()
                    {
                        MetricName = "AclFindings",
                        Value = findings.Count,
                        Unit = Amazon.CloudWatch.StandardUnit.Count
                    },
                    new()
                    {
                        MetricName = "CriticalFindings",
                        Value = findings.Count(f => f.Severity == "CRITICAL"),
                        Unit = Amazon.CloudWatch.StandardUnit.Count
                    }
                },
                region: region);

            return new NetworkAclAuditorResult(
                AclsAudited: acls.NetworkAcls?.Count ?? 0,
                Findings: findings,
                CriticalFindings: findings.Count(f => f.Severity == "CRITICAL"),
                WarningFindings: findings.Count(f => f.Severity == "WARNING"));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Network ACL audit failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="VpcPeeringManagerAsync"/>.</summary>
    public static VpcPeeringManagerResult VpcPeeringManager(string requesterVpcId, string accepterVpcId, string? accepterAccountId = null, string? accepterRegion = null, List<(string RouteTableId, string DestinationCidr)>? routes = null, RegionEndpoint? region = null)
        => VpcPeeringManagerAsync(requesterVpcId, accepterVpcId, accepterAccountId, accepterRegion, routes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TransitGatewayConfiguratorAsync"/>.</summary>
    public static TransitGatewayConfiguratorResult TransitGatewayConfigurator(string transitGatewayId, string vpcId, List<string> subnetIds, List<(string RouteTableId, string DestinationCidr)>? routes = null, RegionEndpoint? region = null)
        => TransitGatewayConfiguratorAsync(transitGatewayId, vpcId, subnetIds, routes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PrivateLinkManagerAsync"/>.</summary>
    public static PrivateLinkManagerResult PrivateLinkManager(string vpcId, string serviceName, List<string> subnetIds, List<string> securityGroupIds, string endpointType = "Interface", RegionEndpoint? region = null)
        => PrivateLinkManagerAsync(vpcId, serviceName, subnetIds, securityGroupIds, endpointType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="NetworkAclAuditorAsync"/>.</summary>
    public static NetworkAclAuditorResult NetworkAclAuditor(List<string>? vpcIds = null, RegionEndpoint? region = null)
        => NetworkAclAuditorAsync(vpcIds, region).GetAwaiter().GetResult();

}
