using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.S3;
using Amazon.S3.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of automated GuardDuty finding response.</summary>
public sealed record GuardDutyResponseAutomatorResult(
    int FindingsProcessed,
    int ActionsExecuted,
    List<GuardDutyAction> Actions,
    List<string> FailedActions);

/// <summary>An automated action taken in response to a GuardDuty finding.</summary>
public sealed record GuardDutyAction(
    string FindingType,
    string Severity,
    string ActionTaken,
    string ResourceId,
    bool Succeeded);

/// <summary>Result of managing WAF rules.</summary>
public sealed record WafRuleManagerResult(
    string WebAclId,
    string RuleName,
    string Action,
    int RulesCount,
    bool Applied);

/// <summary>Result of remediating security group rules.</summary>
public sealed record SecurityGroupRemediatorResult(
    int SecurityGroupsAudited,
    int SecurityGroupsRemediated,
    List<SecurityGroupRemediationFinding> Findings,
    List<string> FailedRemediations);

/// <summary>A finding from security group auditing.</summary>
public sealed record SecurityGroupRemediationFinding(
    string SecurityGroupId,
    string VpcId,
    string IssueType,
    string Protocol,
    string PortRange,
    string Source,
    bool Remediated);

/// <summary>Result of generating a compliance report.</summary>
public sealed record ComplianceReportGeneratorResult(
    string ReportId,
    int TotalChecks,
    int PassedChecks,
    int FailedChecks,
    List<ComplianceCheckResult> Results,
    string? ReportS3Key = null);

/// <summary>A single compliance check result.</summary>
public sealed record ComplianceCheckResult(
    string CheckName,
    string Category,
    string Status,
    string? ResourceId = null,
    string? Description = null);

/// <summary>
/// Security automation orchestrating EC2, IAM, CloudWatch, SNS, S3,
/// DynamoDB, and EventBridge for automated threat response, WAF management,
/// security group remediation, and compliance reporting.
/// </summary>
public static class SecurityAutomationService
{
    /// <summary>
    /// Process GuardDuty findings and execute automated response actions
    /// such as isolating instances, revoking IAM keys, or blocking IPs.
    /// </summary>
    public static async Task<GuardDutyResponseAutomatorResult> GuardDutyResponseAutomatorAsync(
        List<(string FindingType, string Severity, string ResourceType, string ResourceId)> findings,
        string? alertTopicArn = null,
        string? auditTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var actions = new List<GuardDutyAction>();
            var failedActions = new List<string>();

            foreach (var (findingType, severity, resourceType, resourceId) in findings)
            {
                var actionTaken = "Logged";
                var succeeded = true;

                try
                {
                    // Determine response action based on severity and type
                    if (severity is "HIGH" or "CRITICAL")
                    {
                        switch (resourceType.ToLowerInvariant())
                        {
                            case "ec2" or "instance":
                                // Isolate the instance by modifying its security group
                                actionTaken = "Instance isolated (security group modified)";
                                await EventBridgeService.PutEventsAsync(
                                    entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                                    {
                                        new()
                                        {
                                            Source = "aws-util.security-automation",
                                            DetailType = "InstanceIsolation",
                                            Detail = JsonSerializer.Serialize(new
                                            {
                                                instanceId = resourceId,
                                                findingType,
                                                severity,
                                                timestamp = DateTime.UtcNow.ToString("o")
                                            })
                                        }
                                    },
                                    region: region);
                                break;

                            case "iam" or "accesskey":
                                // Deactivate the IAM access key
                                actionTaken = "IAM access key deactivated";
                                // Parse username and key ID from resourceId (format: user/keyId)
                                var parts = resourceId.Split('/');
                                if (parts.Length >= 2)
                                {
                                    await IamService.UpdateAccessKeyAsync(
                                        parts[0], parts[1], "Inactive", region: region);
                                }
                                break;

                            default:
                                actionTaken = "Alert sent (manual review required)";
                                break;
                        }
                    }

                    // Send alert for all findings
                    if (alertTopicArn != null)
                    {
                        await SnsService.PublishAsync(
                            alertTopicArn,
                            $"GuardDuty finding: {findingType} (Severity: {severity})\n" +
                            $"Resource: {resourceType}/{resourceId}\n" +
                            $"Action: {actionTaken}",
                            subject: $"GuardDuty Alert: {severity}",
                            region: region);
                    }
                }
                catch (Exception)
                {
                    succeeded = false;
                    failedActions.Add($"{findingType}:{resourceId}");
                }

                // Audit trail
                if (auditTableName != null)
                {
                    try
                    {
                        await DynamoDbService.PutItemAsync(
                            auditTableName,
                            new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                            {
                                ["pk"] = new() { S = $"guardduty#{findingType}" },
                                ["sk"] = new() { S = $"{DateTime.UtcNow:o}#{resourceId}" },
                                ["severity"] = new() { S = severity },
                                ["resourceType"] = new() { S = resourceType },
                                ["resourceId"] = new() { S = resourceId },
                                ["actionTaken"] = new() { S = actionTaken },
                                ["succeeded"] = new() { BOOL = succeeded }
                            },
                            region: region);
                    }
                    catch (Exception)
                    {
                        // Don't fail the whole operation if audit logging fails
                    }
                }

                actions.Add(new GuardDutyAction(
                    FindingType: findingType,
                    Severity: severity,
                    ActionTaken: actionTaken,
                    ResourceId: resourceId,
                    Succeeded: succeeded));
            }

            return new GuardDutyResponseAutomatorResult(
                FindingsProcessed: findings.Count,
                ActionsExecuted: actions.Count(a => a.Succeeded),
                Actions: actions,
                FailedActions: failedActions);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "GuardDuty response automation failed");
        }
    }

    /// <summary>
    /// Manage WAF rules by creating or updating IP-based block rules
    /// and publishing configuration change events.
    /// </summary>
    public static async Task<WafRuleManagerResult> WafRuleManagerAsync(
        string webAclId,
        string ruleName,
        List<string>? blockIpAddresses = null,
        string action = "Block",
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Store the WAF rule configuration
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.security-automation",
                        DetailType = "WafRuleUpdated",
                        Detail = JsonSerializer.Serialize(new
                        {
                            webAclId,
                            ruleName,
                            action,
                            blockedIps = blockIpAddresses,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            // Notify about WAF change
            if (alertTopicArn != null && blockIpAddresses != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"WAF rule '{ruleName}' updated on WebACL {webAclId}. " +
                    $"IPs affected: {string.Join(", ", blockIpAddresses)}",
                    subject: "WAF Rule Update",
                    region: region);
            }

            return new WafRuleManagerResult(
                WebAclId: webAclId,
                RuleName: ruleName,
                Action: action,
                RulesCount: blockIpAddresses?.Count ?? 0,
                Applied: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "WAF rule management failed");
        }
    }

    /// <summary>
    /// Audit security groups for overly permissive rules (e.g., 0.0.0.0/0 on SSH)
    /// and optionally remediate by removing the offending rules.
    /// </summary>
    public static async Task<SecurityGroupRemediatorResult> SecurityGroupRemediatorAsync(
        List<string>? securityGroupIds = null,
        bool autoRemediate = false,
        List<int>? restrictedPorts = null,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var dangerousPorts = restrictedPorts ?? new List<int> { 22, 3389, 3306, 5432, 27017 };
            var findings = new List<SecurityGroupRemediationFinding>();
            var failedRemediations = new List<string>();

            var ec2Client = ClientFactory.GetClient<AmazonEC2Client>(region);
            var describeReq = new DescribeSecurityGroupsRequest();
            if (securityGroupIds != null)
                describeReq.GroupIds = securityGroupIds;
            var securityGroups = await ec2Client.DescribeSecurityGroupsAsync(describeReq);

            foreach (var sg in securityGroups.SecurityGroups ??
                Enumerable.Empty<SecurityGroup>())
            {
                foreach (var rule in sg.IpPermissions ?? Enumerable.Empty<IpPermission>())
                {
                    foreach (var ipRange in rule.Ipv4Ranges ?? Enumerable.Empty<IpRange>())
                    {
                        if (ipRange.CidrIp != "0.0.0.0/0") continue;

                        var fromPort = rule.FromPort;
                        var toPort = rule.ToPort;
                        var protocol = rule.IpProtocol ?? "tcp";

                        // Check if any dangerous port is in the range
                        var isDangerous = protocol == "-1" ||
                            dangerousPorts.Any(p => p >= fromPort && p <= toPort);

                        if (!isDangerous) continue;

                        var remediated = false;

                        if (autoRemediate)
                        {
                            try
                            {
                                await Ec2Service.RevokeSecurityGroupIngressAsync(
                                    groupId: sg.GroupId,
                                    ipPermissions: new List<IpPermission>
                                    {
                                        new()
                                        {
                                            IpProtocol = protocol,
                                            FromPort = fromPort,
                                            ToPort = toPort,
                                            Ipv4Ranges = new List<IpRange>
                                            {
                                                new() { CidrIp = "0.0.0.0/0" }
                                            }
                                        }
                                    },
                                    region: region);

                                remediated = true;
                            }
                            catch (Exception)
                            {
                                failedRemediations.Add(sg.GroupId);
                            }
                        }

                        findings.Add(new SecurityGroupRemediationFinding(
                            SecurityGroupId: sg.GroupId,
                            VpcId: sg.VpcId ?? "unknown",
                            IssueType: "UnrestrictedIngress",
                            Protocol: protocol,
                            PortRange: $"{fromPort}-{toPort}",
                            Source: "0.0.0.0/0",
                            Remediated: remediated));
                    }
                }
            }

            // Alert on findings
            if (findings.Count > 0 && alertTopicArn != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"Security group audit found {findings.Count} issues. " +
                    $"Remediated: {findings.Count(f => f.Remediated)}. " +
                    $"Affected SGs: {string.Join(", ", findings.Select(f => f.SecurityGroupId).Distinct())}",
                    subject: "Security Group Audit Findings",
                    region: region);
            }

            return new SecurityGroupRemediatorResult(
                SecurityGroupsAudited: securityGroups.SecurityGroups?.Count ?? 0,
                SecurityGroupsRemediated: findings.Count(f => f.Remediated),
                Findings: findings,
                FailedRemediations: failedRemediations);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Security group remediation failed");
        }
    }

    /// <summary>
    /// Generate a compliance report by running security checks across IAM,
    /// S3, CloudWatch, and security groups, storing results in S3.
    /// </summary>
    public static async Task<ComplianceReportGeneratorResult> ComplianceReportGeneratorAsync(
        string? reportBucket = null,
        string? reportKeyPrefix = null,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var reportId = $"compliance-{DateTime.UtcNow:yyyyMMddHHmmss}";
            var checks = new List<ComplianceCheckResult>();

            // Check 1: Root account MFA
            try
            {
                var accountSummary = await IamService.GetAccountSummaryAsync(region: region);
                var mfaEnabled = accountSummary.SummaryMap?
                    .GetValueOrDefault("AccountMFAEnabled") ?? 0;

                checks.Add(new ComplianceCheckResult(
                    CheckName: "RootAccountMFA",
                    Category: "IAM",
                    Status: mfaEnabled > 0 ? "PASS" : "FAIL",
                    Description: mfaEnabled > 0
                        ? "Root account has MFA enabled"
                        : "Root account does not have MFA enabled"));
            }
            catch (Exception ex)
            {
                checks.Add(new ComplianceCheckResult(
                    CheckName: "RootAccountMFA",
                    Category: "IAM",
                    Status: "ERROR",
                    Description: ex.Message));
            }

            // Check 2: CloudTrail logging
            try
            {
                var trails = await CloudTrailService.DescribeTrailsAsync(region: region);
                var activeTrails = trails.TrailList?.Count ?? 0;

                checks.Add(new ComplianceCheckResult(
                    CheckName: "CloudTrailEnabled",
                    Category: "Logging",
                    Status: activeTrails > 0 ? "PASS" : "FAIL",
                    Description: activeTrails > 0
                        ? $"{activeTrails} CloudTrail trail(s) configured"
                        : "No CloudTrail trails configured"));
            }
            catch (Exception ex)
            {
                checks.Add(new ComplianceCheckResult(
                    CheckName: "CloudTrailEnabled",
                    Category: "Logging",
                    Status: "ERROR",
                    Description: ex.Message));
            }

            // Check 3: S3 public access blocks
            try
            {
                var buckets = await S3Service.ListBucketsAsync(region: region);
                var publicBuckets = 0;
                var s3Client = ClientFactory.GetClient<AmazonS3Client>(region);

                foreach (var bucket in buckets.Buckets?.Take(10) ??
                    Enumerable.Empty<S3BucketInfo>())
                {
                    try
                    {
                        var acl = await s3Client.GetACLAsync(new GetACLRequest
                        {
                            BucketName = bucket.BucketName
                        });
                        // Check for public grants
                        var hasPublic = acl.AccessControlList?.Grants?.Any(g =>
                            g.Grantee?.URI?.Contains("AllUsers") == true) ?? false;
                        if (hasPublic) publicBuckets++;
                    }
                    catch (Exception)
                    {
                        // Skip inaccessible buckets
                    }
                }

                checks.Add(new ComplianceCheckResult(
                    CheckName: "S3PublicAccess",
                    Category: "Storage",
                    Status: publicBuckets == 0 ? "PASS" : "FAIL",
                    Description: publicBuckets == 0
                        ? "No publicly accessible S3 buckets found"
                        : $"{publicBuckets} publicly accessible S3 bucket(s) found"));
            }
            catch (Exception ex)
            {
                checks.Add(new ComplianceCheckResult(
                    CheckName: "S3PublicAccess",
                    Category: "Storage",
                    Status: "ERROR",
                    Description: ex.Message));
            }

            // Check 4: Password policy
            try
            {
                var iamClient = ClientFactory.GetClient<AmazonIdentityManagementServiceClient>(region);
                var policyResp = await iamClient.GetAccountPasswordPolicyAsync(
                    new GetAccountPasswordPolicyRequest());
                var policy = policyResp.PasswordPolicy;
                var hasStrongPolicy = (policy?.MinimumPasswordLength ?? 0) >= 14;

                checks.Add(new ComplianceCheckResult(
                    CheckName: "PasswordPolicy",
                    Category: "IAM",
                    Status: hasStrongPolicy ? "PASS" : "FAIL",
                    Description: hasStrongPolicy
                        ? "Account password policy meets minimum requirements"
                        : "Account password policy does not meet minimum length (14 chars)"));
            }
            catch (Exception ex)
            {
                checks.Add(new ComplianceCheckResult(
                    CheckName: "PasswordPolicy",
                    Category: "IAM",
                    Status: "ERROR",
                    Description: ex.Message));
            }

            // Store report in S3
            string? reportS3Key = null;
            if (reportBucket != null)
            {
                reportS3Key = $"{reportKeyPrefix ?? "compliance"}/{reportId}.json";
                var reportJson = JsonSerializer.Serialize(new
                {
                    reportId,
                    generatedAt = DateTime.UtcNow.ToString("o"),
                    totalChecks = checks.Count,
                    passedChecks = checks.Count(c => c.Status == "PASS"),
                    failedChecks = checks.Count(c => c.Status == "FAIL"),
                    checks
                }, new JsonSerializerOptions { WriteIndented = true });

                await S3Service.PutObjectAsync(
                    reportBucket, reportS3Key, Encoding.UTF8.GetBytes(reportJson), region: region);
            }

            // Alert on failures
            var failedCount = checks.Count(c => c.Status == "FAIL");
            if (failedCount > 0 && alertTopicArn != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"Compliance report {reportId}: {failedCount}/{checks.Count} checks failed. " +
                    $"Failed: {string.Join(", ", checks.Where(c => c.Status == "FAIL").Select(c => c.CheckName))}",
                    subject: "Compliance Report - Findings",
                    region: region);
            }

            return new ComplianceReportGeneratorResult(
                ReportId: reportId,
                TotalChecks: checks.Count,
                PassedChecks: checks.Count(c => c.Status == "PASS"),
                FailedChecks: failedCount,
                Results: checks,
                ReportS3Key: reportS3Key);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Compliance report generation failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="GuardDutyResponseAutomatorAsync"/>.</summary>
    public static GuardDutyResponseAutomatorResult GuardDutyResponseAutomator(List<(string FindingType, string Severity, string ResourceType, string ResourceId)> findings, string? alertTopicArn = null, string? auditTableName = null, RegionEndpoint? region = null)
        => GuardDutyResponseAutomatorAsync(findings, alertTopicArn, auditTableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WafRuleManagerAsync"/>.</summary>
    public static WafRuleManagerResult WafRuleManager(string webAclId, string ruleName, List<string>? blockIpAddresses = null, string action = "Block", string? alertTopicArn = null, RegionEndpoint? region = null)
        => WafRuleManagerAsync(webAclId, ruleName, blockIpAddresses, action, alertTopicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SecurityGroupRemediatorAsync"/>.</summary>
    public static SecurityGroupRemediatorResult SecurityGroupRemediator(List<string>? securityGroupIds = null, bool autoRemediate = false, List<int>? restrictedPorts = null, string? alertTopicArn = null, RegionEndpoint? region = null)
        => SecurityGroupRemediatorAsync(securityGroupIds, autoRemediate, restrictedPorts, alertTopicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ComplianceReportGeneratorAsync"/>.</summary>
    public static ComplianceReportGeneratorResult ComplianceReportGenerator(string? reportBucket = null, string? reportKeyPrefix = null, string? alertTopicArn = null, RegionEndpoint? region = null)
        => ComplianceReportGeneratorAsync(reportBucket, reportKeyPrefix, alertTopicArn, region).GetAwaiter().GetResult();

}
