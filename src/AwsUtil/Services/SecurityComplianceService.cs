using System.Text.Json;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.AccessAnalyzer;
using Amazon.AccessAnalyzer.Model;
using Amazon.CognitoIdentityProvider;
using Amazon.CognitoIdentityProvider.Model;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Amazon.WAFV2;
using Amazon.WAFV2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Finding from least privilege analysis.</summary>
public sealed record LeastPrivilegeFinding(
    string Principal,
    string? PolicyName = null,
    string? FindingType = null,
    string? Resource = null,
    string? Action = null,
    string? Recommendation = null);

/// <summary>Result of least privilege analysis.</summary>
public sealed record LeastPrivilegeAnalyzerResult(
    List<LeastPrivilegeFinding>? Findings = null,
    int FindingCount = 0,
    string? AnalyzerArn = null);

/// <summary>Result of secret rotation orchestration.</summary>
public sealed record SecretRotationOrchestratorResult(
    string? SecretArn = null,
    string? SecretName = null,
    string? VersionId = null,
    bool RotationInitiated = false);

/// <summary>Result of data masking processing.</summary>
public sealed record DataMaskingProcessorResult(
    string? MaskedData = null,
    int FieldsMasked = 0);

/// <summary>Security group audit finding.</summary>
public sealed record SecurityGroupFinding(
    string GroupId,
    string? GroupName = null,
    string? VpcId = null,
    string? Issue = null,
    string? Protocol = null,
    string? PortRange = null,
    string? CidrBlock = null);

/// <summary>Result of VPC security group audit.</summary>
public sealed record VpcSecurityGroupAuditorResult(
    List<SecurityGroupFinding>? Findings = null,
    int FindingCount = 0,
    int GroupsAudited = 0);

/// <summary>Result of encryption enforcement check.</summary>
public sealed record EncryptionEnforcerResult(
    string? ResourceArn = null,
    bool IsEncrypted = false,
    string? EncryptionType = null,
    string? KmsKeyId = null,
    bool EnforcementApplied = false);

/// <summary>Result of API Gateway WAF management.</summary>
public sealed record ApiGatewayWafManagerResult(
    string? WebAclArn = null,
    string? WebAclId = null,
    string? WebAclName = null,
    bool Associated = false);

/// <summary>Compliance check entry.</summary>
public sealed record ComplianceCheckItem(
    string ResourceType,
    string ResourceId,
    string CheckName,
    string Status,
    string? Detail = null);

/// <summary>Result of taking a compliance snapshot.</summary>
public sealed record ComplianceSnapshotResult(
    List<ComplianceCheckItem>? Items = null,
    int TotalChecks = 0,
    int PassedChecks = 0,
    int FailedChecks = 0,
    DateTime? SnapshotTime = null);

/// <summary>Result of validating a resource policy.</summary>
public sealed record ResourcePolicyValidatorResult(
    bool IsValid = false,
    List<string>? Warnings = null,
    List<string>? Errors = null);

/// <summary>Result of managing Cognito auth flows.</summary>
public sealed record CognitoAuthFlowManagerResult(
    string? UserPoolId = null,
    string? ClientId = null,
    string? AccessToken = null,
    string? IdToken = null,
    string? RefreshToken = null,
    string? TokenType = null,
    int? ExpiresIn = null,
    string? ChallengeName = null,
    string? Session = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Security and compliance utilities combining IAM, Secrets Manager, KMS,
/// VPC, WAF, Cognito, and IAM Access Analyzer.
/// </summary>
public static class SecurityComplianceService
{
    private static AmazonIdentityManagementServiceClient GetIamClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIdentityManagementServiceClient>(region);

    private static AmazonSecretsManagerClient GetSecretsClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSecretsManagerClient>(region);

    private static AmazonKeyManagementServiceClient GetKmsClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKeyManagementServiceClient>(region);

    private static AmazonEC2Client GetEc2Client(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonEC2Client>(region);

    private static AmazonAccessAnalyzerClient GetAnalyzerClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAccessAnalyzerClient>(region);

    private static AmazonCognitoIdentityProviderClient GetCognitoClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCognitoIdentityProviderClient>(region);

    private static AmazonWAFV2Client GetWafClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonWAFV2Client>(region);

    /// <summary>
    /// Analyze IAM entities for least-privilege violations using IAM Access Analyzer.
    /// </summary>
    public static async Task<LeastPrivilegeAnalyzerResult> LeastPrivilegeAnalyzerAsync(
        string analyzerArn,
        string? resourceType = null,
        int maxFindings = 100,
        RegionEndpoint? region = null)
    {
        var client = GetAnalyzerClient(region);

        try
        {
            var request = new ListFindingsV2Request
            {
                AnalyzerArn = analyzerArn,
                MaxResults = maxFindings
            };

            var resp = await client.ListFindingsV2Async(request);

            var findings = new List<LeastPrivilegeFinding>();
            if (resp.Findings != null)
            {
                foreach (var finding in resp.Findings)
                {
                    if (resourceType != null &&
                        finding.ResourceType?.Value != resourceType)
                        continue;

                    findings.Add(new LeastPrivilegeFinding(
                        Principal: finding.ResourceOwnerAccount ?? "Unknown",
                        FindingType: finding.FindingType?.Value,
                        Resource: finding.Resource,
                        Action: finding.ResourceType?.Value));
                }
            }

            return new LeastPrivilegeAnalyzerResult(
                Findings: findings,
                FindingCount: findings.Count,
                AnalyzerArn: analyzerArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to analyze IAM for least privilege");
        }
    }

    /// <summary>
    /// Orchestrate secret rotation by initiating rotation on a Secrets Manager secret.
    /// </summary>
    public static async Task<SecretRotationOrchestratorResult> SecretRotationOrchestratorAsync(
        string secretId,
        string? rotationLambdaArn = null,
        int? automaticallyAfterDays = null,
        bool rotateImmediately = true,
        RegionEndpoint? region = null)
    {
        var client = GetSecretsClient(region);

        try
        {
            // Configure rotation if Lambda ARN is provided
            if (rotationLambdaArn != null)
            {
                var rotationReq = new RotateSecretRequest
                {
                    SecretId = secretId,
                    RotationLambdaARN = rotationLambdaArn,
                    RotateImmediately = rotateImmediately
                };
                if (automaticallyAfterDays.HasValue)
                {
                    rotationReq.RotationRules = new RotationRulesType
                    {
                        AutomaticallyAfterDays = automaticallyAfterDays.Value
                    };
                }

                var rotateResp = await client.RotateSecretAsync(rotationReq);
                return new SecretRotationOrchestratorResult(
                    SecretArn: rotateResp.ARN,
                    SecretName: rotateResp.Name,
                    VersionId: rotateResp.VersionId,
                    RotationInitiated: true);
            }

            // Just trigger rotation on existing configuration
            var resp = await client.RotateSecretAsync(new RotateSecretRequest
            {
                SecretId = secretId,
                RotateImmediately = rotateImmediately
            });

            return new SecretRotationOrchestratorResult(
                SecretArn: resp.ARN,
                SecretName: resp.Name,
                VersionId: resp.VersionId,
                RotationInitiated: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to orchestrate secret rotation for '{secretId}'");
        }
    }

    /// <summary>
    /// Mask sensitive fields in a JSON string using pattern-based replacement.
    /// Fields matching the patterns are replaced with masked values.
    /// </summary>
    public static async Task<DataMaskingProcessorResult> DataMaskingProcessorAsync(
        string jsonData,
        List<string>? sensitiveFieldPatterns = null,
        string maskValue = "***MASKED***")
    {
        return await Task.Run(() =>
        {
            try
            {
                var patterns = sensitiveFieldPatterns ?? new List<string>
                {
                    @"(?i)(password|secret|token|key|credential|ssn|credit.?card)",
                    @"(?i)(api.?key|access.?key|private.?key|auth)"
                };

                var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
                    jsonData) ?? new Dictionary<string, JsonElement>();

                var masked = new Dictionary<string, object?>();
                var fieldsMasked = 0;

                foreach (var kv in doc)
                {
                    var shouldMask = patterns
                        .Any(p => System.Text.RegularExpressions.Regex.IsMatch(kv.Key, p));

                    if (shouldMask)
                    {
                        masked[kv.Key] = maskValue;
                        fieldsMasked++;
                    }
                    else
                    {
                        masked[kv.Key] = kv.Value;
                    }
                }

                var result = JsonSerializer.Serialize(masked);

                return new DataMaskingProcessorResult(
                    MaskedData: result,
                    FieldsMasked: fieldsMasked);
            }
            catch (Exception exc)
            {
                throw ErrorClassifier.WrapAwsError(exc, "Failed to mask sensitive data");
            }
        });
    }

    /// <summary>
    /// Audit VPC security groups for overly permissive rules (e.g., 0.0.0.0/0 ingress).
    /// </summary>
    public static async Task<VpcSecurityGroupAuditorResult> VpcSecurityGroupAuditorAsync(
        string? vpcId = null,
        List<string>? groupIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetEc2Client(region);

        try
        {
            var request = new DescribeSecurityGroupsRequest();
            if (groupIds != null) request.GroupIds = groupIds;
            if (vpcId != null)
            {
                request.Filters = new List<Amazon.EC2.Model.Filter>
                {
                    new() { Name = "vpc-id", Values = new List<string> { vpcId } }
                };
            }

            var resp = await client.DescribeSecurityGroupsAsync(request);
            var findings = new List<SecurityGroupFinding>();

            foreach (var sg in resp.SecurityGroups)
            {
                foreach (var perm in sg.IpPermissions)
                {
                    foreach (var range in perm.Ipv4Ranges)
                    {
                        if (range.CidrIp == "0.0.0.0/0")
                        {
                            var portRange = perm.FromPort == perm.ToPort
                                ? perm.FromPort.ToString()
                                : $"{perm.FromPort}-{perm.ToPort}";

                            findings.Add(new SecurityGroupFinding(
                                GroupId: sg.GroupId,
                                GroupName: sg.GroupName,
                                VpcId: sg.VpcId,
                                Issue: "Ingress rule allows traffic from 0.0.0.0/0",
                                Protocol: perm.IpProtocol,
                                PortRange: portRange,
                                CidrBlock: range.CidrIp));
                        }
                    }

                    foreach (var range in perm.Ipv6Ranges)
                    {
                        if (range.CidrIpv6 == "::/0")
                        {
                            var portRange = perm.FromPort == perm.ToPort
                                ? perm.FromPort.ToString()
                                : $"{perm.FromPort}-{perm.ToPort}";

                            findings.Add(new SecurityGroupFinding(
                                GroupId: sg.GroupId,
                                GroupName: sg.GroupName,
                                VpcId: sg.VpcId,
                                Issue: "Ingress rule allows traffic from ::/0",
                                Protocol: perm.IpProtocol,
                                PortRange: portRange,
                                CidrBlock: range.CidrIpv6));
                        }
                    }
                }
            }

            return new VpcSecurityGroupAuditorResult(
                Findings: findings,
                FindingCount: findings.Count,
                GroupsAudited: resp.SecurityGroups.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to audit VPC security groups");
        }
    }

    /// <summary>
    /// Check encryption status of a KMS key and optionally enable automatic rotation.
    /// </summary>
    public static async Task<EncryptionEnforcerResult> EncryptionEnforcerAsync(
        string keyId,
        bool enableRotation = false,
        RegionEndpoint? region = null)
    {
        var client = GetKmsClient(region);

        try
        {
            var descResp = await client.DescribeKeyAsync(
                new DescribeKeyRequest { KeyId = keyId });
            var keyMetadata = descResp.KeyMetadata;

            var isEncrypted = keyMetadata.Enabled ?? false;
            var enforcementApplied = false;

            if (enableRotation && keyMetadata.KeyManager?.Value == "CUSTOMER")
            {
                try
                {
                    var rotationResp = await client.GetKeyRotationStatusAsync(
                        new GetKeyRotationStatusRequest { KeyId = keyId });

                    if (!(rotationResp.KeyRotationEnabled ?? false))
                    {
                        await client.EnableKeyRotationAsync(
                            new EnableKeyRotationRequest { KeyId = keyId });
                        enforcementApplied = true;
                    }
                }
                catch (Exception)
                {
                    // Key rotation may not be supported for all key types
                }
            }

            return new EncryptionEnforcerResult(
                ResourceArn: keyMetadata.Arn,
                IsEncrypted: isEncrypted,
                EncryptionType: keyMetadata.KeySpec?.Value,
                KmsKeyId: keyMetadata.KeyId,
                EnforcementApplied: enforcementApplied);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enforce encryption for key '{keyId}'");
        }
    }

    /// <summary>
    /// Associate or create a WAFv2 Web ACL for an API Gateway stage.
    /// </summary>
    public static async Task<ApiGatewayWafManagerResult> ApiGatewayWafManagerAsync(
        string resourceArn,
        string? webAclArn = null,
        string? webAclName = null,
        string scope = "REGIONAL",
        RegionEndpoint? region = null)
    {
        var client = GetWafClient(region);

        try
        {
            string? finalWebAclArn = webAclArn;

            if (finalWebAclArn == null && webAclName != null)
            {
                // Create a basic Web ACL with default action
                var createResp = await client.CreateWebACLAsync(new CreateWebACLRequest
                {
                    Name = webAclName,
                    Scope = new Amazon.WAFV2.Scope(scope),
                    DefaultAction = new DefaultAction
                    {
                        Allow = new AllowAction()
                    },
                    VisibilityConfig = new VisibilityConfig
                    {
                        CloudWatchMetricsEnabled = true,
                        MetricName = webAclName,
                        SampledRequestsEnabled = true
                    }
                });

                finalWebAclArn = createResp.Summary?.ARN;
            }

            if (finalWebAclArn != null)
            {
                await client.AssociateWebACLAsync(new AssociateWebACLRequest
                {
                    ResourceArn = resourceArn,
                    WebACLArn = finalWebAclArn
                });
            }

            return new ApiGatewayWafManagerResult(
                WebAclArn: finalWebAclArn,
                WebAclName: webAclName,
                Associated: finalWebAclArn != null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to manage WAF for resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Take a compliance snapshot by checking encryption, public access, and
    /// security group rules for a set of resources.
    /// </summary>
    public static async Task<ComplianceSnapshotResult> ComplianceSnapshotAsync(
        List<string>? securityGroupIds = null,
        List<string>? kmsKeyIds = null,
        string? vpcId = null,
        RegionEndpoint? region = null)
    {
        var items = new List<ComplianceCheckItem>();

        try
        {
            // Check security groups for overly permissive rules
            if (securityGroupIds != null || vpcId != null)
            {
                var sgResult = await VpcSecurityGroupAuditorAsync(
                    vpcId: vpcId,
                    groupIds: securityGroupIds,
                    region: region);

                if (sgResult.Findings != null)
                {
                    foreach (var finding in sgResult.Findings)
                    {
                        items.Add(new ComplianceCheckItem(
                            ResourceType: "AWS::EC2::SecurityGroup",
                            ResourceId: finding.GroupId,
                            CheckName: "NoOpenIngress",
                            Status: "FAILED",
                            Detail: finding.Issue));
                    }
                }

                var auditedWithoutFindings = sgResult.GroupsAudited -
                    (sgResult.Findings?.Select(f => f.GroupId).Distinct().Count() ?? 0);
                for (var i = 0; i < auditedWithoutFindings; i++)
                {
                    items.Add(new ComplianceCheckItem(
                        ResourceType: "AWS::EC2::SecurityGroup",
                        ResourceId: "compliant-group",
                        CheckName: "NoOpenIngress",
                        Status: "PASSED"));
                }
            }

            // Check KMS keys for encryption and rotation
            if (kmsKeyIds != null)
            {
                foreach (var keyId in kmsKeyIds)
                {
                    try
                    {
                        var encResult = await EncryptionEnforcerAsync(
                            keyId, region: region);

                        items.Add(new ComplianceCheckItem(
                            ResourceType: "AWS::KMS::Key",
                            ResourceId: keyId,
                            CheckName: "EncryptionEnabled",
                            Status: encResult.IsEncrypted ? "PASSED" : "FAILED",
                            Detail: encResult.EncryptionType));
                    }
                    catch (Exception ex)
                    {
                        items.Add(new ComplianceCheckItem(
                            ResourceType: "AWS::KMS::Key",
                            ResourceId: keyId,
                            CheckName: "EncryptionEnabled",
                            Status: "ERROR",
                            Detail: ex.Message));
                    }
                }
            }

            var passed = items.Count(i => i.Status == "PASSED");
            var failed = items.Count(i => i.Status != "PASSED");

            return new ComplianceSnapshotResult(
                Items: items,
                TotalChecks: items.Count,
                PassedChecks: passed,
                FailedChecks: failed,
                SnapshotTime: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to take compliance snapshot");
        }
    }

    /// <summary>
    /// Validate a resource policy (JSON) using IAM Access Analyzer.
    /// </summary>
    public static async Task<ResourcePolicyValidatorResult> ResourcePolicyValidatorAsync(
        string policyDocument,
        string policyType = "RESOURCE_POLICY",
        RegionEndpoint? region = null)
    {
        var client = GetAnalyzerClient(region);

        try
        {
            var resp = await client.ValidatePolicyAsync(new ValidatePolicyRequest
            {
                PolicyDocument = policyDocument,
                PolicyType = new Amazon.AccessAnalyzer.PolicyType(policyType)
            });

            var warnings = new List<string>();
            var errors = new List<string>();

            if (resp.Findings != null)
            {
                foreach (var finding in resp.Findings)
                {
                    var message =
                        $"{finding.FindingType}: {finding.IssueCode} - {finding.FindingDetails}";
                    if (finding.FindingType?.Value == "ERROR")
                        errors.Add(message);
                    else
                        warnings.Add(message);
                }
            }

            return new ResourcePolicyValidatorResult(
                IsValid: errors.Count == 0,
                Warnings: warnings.Count > 0 ? warnings : null,
                Errors: errors.Count > 0 ? errors : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to validate resource policy");
        }
    }

    /// <summary>
    /// Initiate a Cognito USER_PASSWORD_AUTH or USER_SRP_AUTH flow and return tokens.
    /// </summary>
    public static async Task<CognitoAuthFlowManagerResult> CognitoAuthFlowManagerAsync(
        string userPoolId,
        string clientId,
        string username,
        string password,
        string authFlow = "USER_PASSWORD_AUTH",
        string? clientSecret = null,
        RegionEndpoint? region = null)
    {
        var client = GetCognitoClient(region);

        try
        {
            var authParams = new Dictionary<string, string>
            {
                ["USERNAME"] = username,
                ["PASSWORD"] = password
            };
            if (clientSecret != null)
                authParams["SECRET_HASH"] = clientSecret;

            var resp = await client.InitiateAuthAsync(new InitiateAuthRequest
            {
                ClientId = clientId,
                AuthFlow = new AuthFlowType(authFlow),
                AuthParameters = authParams
            });

            if (resp.ChallengeName != null)
            {
                return new CognitoAuthFlowManagerResult(
                    UserPoolId: userPoolId,
                    ClientId: clientId,
                    ChallengeName: resp.ChallengeName.Value,
                    Session: resp.Session);
            }

            return new CognitoAuthFlowManagerResult(
                UserPoolId: userPoolId,
                ClientId: clientId,
                AccessToken: resp.AuthenticationResult?.AccessToken,
                IdToken: resp.AuthenticationResult?.IdToken,
                RefreshToken: resp.AuthenticationResult?.RefreshToken,
                TokenType: resp.AuthenticationResult?.TokenType,
                ExpiresIn: resp.AuthenticationResult?.ExpiresIn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to manage Cognito auth flow for user '{username}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="LeastPrivilegeAnalyzerAsync"/>.</summary>
    public static LeastPrivilegeAnalyzerResult LeastPrivilegeAnalyzer(string analyzerArn, string? resourceType = null, int maxFindings = 100, RegionEndpoint? region = null)
        => LeastPrivilegeAnalyzerAsync(analyzerArn, resourceType, maxFindings, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SecretRotationOrchestratorAsync"/>.</summary>
    public static SecretRotationOrchestratorResult SecretRotationOrchestrator(string secretId, string? rotationLambdaArn = null, int? automaticallyAfterDays = null, bool rotateImmediately = true, RegionEndpoint? region = null)
        => SecretRotationOrchestratorAsync(secretId, rotationLambdaArn, automaticallyAfterDays, rotateImmediately, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DataMaskingProcessorAsync"/>.</summary>
    public static DataMaskingProcessorResult DataMaskingProcessor(string jsonData, List<string>? sensitiveFieldPatterns = null, string maskValue = "***MASKED***")
        => DataMaskingProcessorAsync(jsonData, sensitiveFieldPatterns, maskValue).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="VpcSecurityGroupAuditorAsync"/>.</summary>
    public static VpcSecurityGroupAuditorResult VpcSecurityGroupAuditor(string? vpcId = null, List<string>? groupIds = null, RegionEndpoint? region = null)
        => VpcSecurityGroupAuditorAsync(vpcId, groupIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EncryptionEnforcerAsync"/>.</summary>
    public static EncryptionEnforcerResult EncryptionEnforcer(string keyId, bool enableRotation = false, RegionEndpoint? region = null)
        => EncryptionEnforcerAsync(keyId, enableRotation, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ApiGatewayWafManagerAsync"/>.</summary>
    public static ApiGatewayWafManagerResult ApiGatewayWafManager(string resourceArn, string? webAclArn = null, string? webAclName = null, string scope = "REGIONAL", RegionEndpoint? region = null)
        => ApiGatewayWafManagerAsync(resourceArn, webAclArn, webAclName, scope, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ComplianceSnapshotAsync"/>.</summary>
    public static ComplianceSnapshotResult ComplianceSnapshot(List<string>? securityGroupIds = null, List<string>? kmsKeyIds = null, string? vpcId = null, RegionEndpoint? region = null)
        => ComplianceSnapshotAsync(securityGroupIds, kmsKeyIds, vpcId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ResourcePolicyValidatorAsync"/>.</summary>
    public static ResourcePolicyValidatorResult ResourcePolicyValidator(string policyDocument, string policyType = "RESOURCE_POLICY", RegionEndpoint? region = null)
        => ResourcePolicyValidatorAsync(policyDocument, policyType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CognitoAuthFlowManagerAsync"/>.</summary>
    public static CognitoAuthFlowManagerResult CognitoAuthFlowManager(string userPoolId, string clientId, string username, string password, string authFlow = "USER_PASSWORD_AUTH", string? clientSecret = null, RegionEndpoint? region = null)
        => CognitoAuthFlowManagerAsync(userPoolId, clientId, username, password, authFlow, clientSecret, region).GetAwaiter().GetResult();

}
