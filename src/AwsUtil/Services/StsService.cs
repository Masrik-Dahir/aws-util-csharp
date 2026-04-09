using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record AssumeRoleResult(
    string? AccessKeyId = null, string? SecretAccessKey = null,
    string? SessionToken = null, string? Expiration = null,
    string? AssumedRoleArn = null, string? AssumedRoleId = null);

public sealed record CallerIdentityResult(
    string? Account = null, string? Arn = null,
    string? UserId = null);

public sealed record SessionTokenResult(
    string? AccessKeyId = null, string? SecretAccessKey = null,
    string? SessionToken = null, string? Expiration = null);

public sealed record FederationTokenResult(
    string? AccessKeyId = null, string? SecretAccessKey = null,
    string? SessionToken = null, string? Expiration = null,
    string? FederatedUserArn = null, string? FederatedUserId = null);

public sealed record DecodeAuthorizationMessageResult(
    string? DecodedMessage = null);

public sealed record AccessKeyInfoResult(string? Account = null);

/// <summary>
/// Utility helpers for AWS Security Token Service (STS).
/// </summary>
public static class StsService
{
    private static AmazonSecurityTokenServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSecurityTokenServiceClient>(region);

    /// <summary>
    /// Assume an IAM role and return temporary credentials.
    /// </summary>
    public static async Task<AssumeRoleResult> AssumeRoleAsync(
        string roleArn,
        string roleSessionName,
        int? durationSeconds = null,
        string? externalId = null,
        string? policy = null,
        List<PolicyDescriptorType>? policyArns = null,
        string? serialNumber = null,
        string? tokenCode = null,
        List<Tag>? tags = null,
        List<string>? transitiveTagKeys = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AssumeRoleRequest
        {
            RoleArn = roleArn,
            RoleSessionName = roleSessionName
        };
        if (durationSeconds.HasValue) request.DurationSeconds = durationSeconds.Value;
        if (externalId != null) request.ExternalId = externalId;
        if (policy != null) request.Policy = policy;
        if (policyArns != null) request.PolicyArns = policyArns;
        if (serialNumber != null) request.SerialNumber = serialNumber;
        if (tokenCode != null) request.TokenCode = tokenCode;
        if (tags != null) request.Tags = tags;
        if (transitiveTagKeys != null) request.TransitiveTagKeys = transitiveTagKeys;

        try
        {
            var resp = await client.AssumeRoleAsync(request);
            return new AssumeRoleResult(
                AccessKeyId: resp.Credentials.AccessKeyId,
                SecretAccessKey: resp.Credentials.SecretAccessKey,
                SessionToken: resp.Credentials.SessionToken,
                Expiration: resp.Credentials.Expiration.ToString(),
                AssumedRoleArn: resp.AssumedRoleUser?.Arn,
                AssumedRoleId: resp.AssumedRoleUser?.AssumedRoleId);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to assume role '{roleArn}'");
        }
    }

    /// <summary>
    /// Assume a role using a web identity token (e.g., OIDC).
    /// </summary>
    public static async Task<AssumeRoleResult> AssumeRoleWithWebIdentityAsync(
        string roleArn,
        string roleSessionName,
        string webIdentityToken,
        int? durationSeconds = null,
        string? providerId = null,
        string? policy = null,
        List<PolicyDescriptorType>? policyArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AssumeRoleWithWebIdentityRequest
        {
            RoleArn = roleArn,
            RoleSessionName = roleSessionName,
            WebIdentityToken = webIdentityToken
        };
        if (durationSeconds.HasValue) request.DurationSeconds = durationSeconds.Value;
        if (providerId != null) request.ProviderId = providerId;
        if (policy != null) request.Policy = policy;
        if (policyArns != null) request.PolicyArns = policyArns;

        try
        {
            var resp = await client.AssumeRoleWithWebIdentityAsync(request);
            return new AssumeRoleResult(
                AccessKeyId: resp.Credentials.AccessKeyId,
                SecretAccessKey: resp.Credentials.SecretAccessKey,
                SessionToken: resp.Credentials.SessionToken,
                Expiration: resp.Credentials.Expiration.ToString(),
                AssumedRoleArn: resp.AssumedRoleUser?.Arn,
                AssumedRoleId: resp.AssumedRoleUser?.AssumedRoleId);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to assume role with web identity '{roleArn}'");
        }
    }

    /// <summary>
    /// Assume a role using a SAML assertion.
    /// </summary>
    public static async Task<AssumeRoleResult> AssumeRoleWithSAMLAsync(
        string roleArn,
        string principalArn,
        string samlAssertion,
        int? durationSeconds = null,
        string? policy = null,
        List<PolicyDescriptorType>? policyArns = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AssumeRoleWithSAMLRequest
        {
            RoleArn = roleArn,
            PrincipalArn = principalArn,
            SAMLAssertion = samlAssertion
        };
        if (durationSeconds.HasValue) request.DurationSeconds = durationSeconds.Value;
        if (policy != null) request.Policy = policy;
        if (policyArns != null) request.PolicyArns = policyArns;

        try
        {
            var resp = await client.AssumeRoleWithSAMLAsync(request);
            return new AssumeRoleResult(
                AccessKeyId: resp.Credentials.AccessKeyId,
                SecretAccessKey: resp.Credentials.SecretAccessKey,
                SessionToken: resp.Credentials.SessionToken,
                Expiration: resp.Credentials.Expiration.ToString(),
                AssumedRoleArn: resp.AssumedRoleUser?.Arn,
                AssumedRoleId: resp.AssumedRoleUser?.AssumedRoleId);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to assume role with SAML '{roleArn}'");
        }
    }

    /// <summary>
    /// Get the identity of the calling IAM entity.
    /// </summary>
    public static async Task<CallerIdentityResult> GetCallerIdentityAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCallerIdentityAsync(
                new GetCallerIdentityRequest());
            return new CallerIdentityResult(
                Account: resp.Account,
                Arn: resp.Arn,
                UserId: resp.UserId);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get caller identity");
        }
    }

    /// <summary>
    /// Get a session token for the current IAM user.
    /// </summary>
    public static async Task<SessionTokenResult> GetSessionTokenAsync(
        int? durationSeconds = null,
        string? serialNumber = null,
        string? tokenCode = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSessionTokenRequest();
        if (durationSeconds.HasValue) request.DurationSeconds = durationSeconds.Value;
        if (serialNumber != null) request.SerialNumber = serialNumber;
        if (tokenCode != null) request.TokenCode = tokenCode;

        try
        {
            var resp = await client.GetSessionTokenAsync(request);
            return new SessionTokenResult(
                AccessKeyId: resp.Credentials.AccessKeyId,
                SecretAccessKey: resp.Credentials.SecretAccessKey,
                SessionToken: resp.Credentials.SessionToken,
                Expiration: resp.Credentials.Expiration.ToString());
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get session token");
        }
    }

    /// <summary>
    /// Get a federation token for temporary access.
    /// </summary>
    public static async Task<FederationTokenResult> GetFederationTokenAsync(
        string name,
        int? durationSeconds = null,
        string? policy = null,
        List<PolicyDescriptorType>? policyArns = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFederationTokenRequest { Name = name };
        if (durationSeconds.HasValue) request.DurationSeconds = durationSeconds.Value;
        if (policy != null) request.Policy = policy;
        if (policyArns != null) request.PolicyArns = policyArns;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.GetFederationTokenAsync(request);
            return new FederationTokenResult(
                AccessKeyId: resp.Credentials.AccessKeyId,
                SecretAccessKey: resp.Credentials.SecretAccessKey,
                SessionToken: resp.Credentials.SessionToken,
                Expiration: resp.Credentials.Expiration.ToString(),
                FederatedUserArn: resp.FederatedUser?.Arn,
                FederatedUserId: resp.FederatedUser?.FederatedUserId);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get federation token for '{name}'");
        }
    }

    /// <summary>
    /// Decode an encoded authorization failure message.
    /// </summary>
    public static async Task<DecodeAuthorizationMessageResult>
        DecodeAuthorizationMessageAsync(
            string encodedMessage, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DecodeAuthorizationMessageAsync(
                new DecodeAuthorizationMessageRequest
                {
                    EncodedMessage = encodedMessage
                });
            return new DecodeAuthorizationMessageResult(
                DecodedMessage: resp.DecodedMessage);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to decode authorization message");
        }
    }

    /// <summary>
    /// Get the account ID associated with an access key.
    /// </summary>
    public static async Task<AccessKeyInfoResult> GetAccessKeyInfoAsync(
        string accessKeyId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccessKeyInfoAsync(
                new GetAccessKeyInfoRequest { AccessKeyId = accessKeyId });
            return new AccessKeyInfoResult(Account: resp.Account);
        }
        catch (AmazonSecurityTokenServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get access key info for '{accessKeyId}'");
        }
    }

    /// <summary>
    /// Assume a role with session tags.
    /// This is a convenience wrapper around AssumeRoleAsync that accepts
    /// a dictionary of tag key/value pairs.
    /// </summary>
    public static async Task<AssumeRoleResult> TagSessionAsync(
        string roleArn,
        string roleSessionName,
        Dictionary<string, string> sessionTags,
        List<string>? transitiveTagKeys = null,
        int? durationSeconds = null,
        string? externalId = null,
        RegionEndpoint? region = null)
    {
        var tags = sessionTags.Select(kv => new Tag
        {
            Key = kv.Key,
            Value = kv.Value
        }).ToList();

        return await AssumeRoleAsync(
            roleArn: roleArn,
            roleSessionName: roleSessionName,
            durationSeconds: durationSeconds,
            externalId: externalId,
            tags: tags,
            transitiveTagKeys: transitiveTagKeys,
            region: region);
    }
}
