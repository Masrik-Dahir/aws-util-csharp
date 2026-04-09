using System.Text.Json;
using Amazon;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Secrets Manager operations.
/// </summary>
public sealed record BatchGetSecretValueResult(
    List<Dictionary<string, object?>>? SecretValues = null,
    string? NextToken = null,
    List<Dictionary<string, object?>>? Errors = null);

public sealed record CancelRotateSecretResult(string? Arn = null, string? Name = null, string? VersionId = null);
public sealed record DeleteResourcePolicyResult(string? Arn = null, string? Name = null);

public sealed record DescribeSecretResult(
    string? Arn = null, string? Name = null, string? Description = null,
    string? KmsKeyId = null, bool? RotationEnabled = null,
    string? RotationLambdaArn = null, Dictionary<string, object?>? RotationRules = null,
    string? LastRotatedDate = null, string? LastChangedDate = null,
    string? LastAccessedDate = null, string? DeletedDate = null,
    string? NextRotationDate = null, List<Dictionary<string, string>>? Tags = null,
    Dictionary<string, object?>? VersionIdsToStages = null,
    string? OwningService = null, string? CreatedDate = null,
    string? PrimaryRegion = null, List<Dictionary<string, object?>>? ReplicationStatus = null);

public sealed record GetRandomPasswordResult(string? RandomPassword = null);
public sealed record GetResourcePolicyResult(string? Arn = null, string? Name = null, string? ResourcePolicy = null);

public sealed record GetSecretValueResult(
    string? Arn = null, string? Name = null, string? VersionId = null,
    byte[]? SecretBinary = null, string? SecretString = null,
    List<string>? VersionStages = null, string? CreatedDate = null);

public sealed record ListSecretVersionIdsResult(
    List<Dictionary<string, object?>>? Versions = null, string? NextToken = null,
    string? Arn = null, string? Name = null);

public sealed record PutResourcePolicyResult(string? Arn = null, string? Name = null);

public sealed record PutSecretValueResult(
    string? Arn = null, string? Name = null, string? VersionId = null,
    List<string>? VersionStages = null);

public sealed record RemoveRegionsFromReplicationResult(
    string? Arn = null, List<Dictionary<string, object?>>? ReplicationStatus = null);

public sealed record ReplicateSecretToRegionsResult(
    string? Arn = null, List<Dictionary<string, object?>>? ReplicationStatus = null);

public sealed record RestoreSecretResult(string? Arn = null, string? Name = null);
public sealed record StopReplicationToReplicaResult(string? Arn = null);

public sealed record UpdateSecretVersionStageResult(string? Arn = null, string? Name = null);

public sealed record ValidateResourcePolicyResult(
    bool? PolicyValidationPassed = null,
    List<Dictionary<string, object?>>? ValidationErrors = null);

/// <summary>
/// Utility helpers for AWS Secrets Manager.
/// </summary>
public static class SecretsManagerService
{
    private static AmazonSecretsManagerClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSecretsManagerClient>(region);

    /// <summary>
    /// Create a new secret in AWS Secrets Manager.
    /// </summary>
    public static async Task<string> CreateSecretAsync(
        string name,
        object value,
        string description = "",
        string? kmsKeyId = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var raw = value is string s ? s : JsonSerializer.Serialize(value);
        var request = new CreateSecretRequest
        {
            Name = name,
            SecretString = raw
        };
        if (!string.IsNullOrEmpty(description))
            request.Description = description;
        if (kmsKeyId != null)
            request.KmsKeyId = kmsKeyId;
        if (tags != null)
            request.Tags = tags.Select(kv => new Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            var resp = await client.CreateSecretAsync(request);
            return resp.ARN;
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create secret '{name}'");
        }
    }

    /// <summary>
    /// Update the value of an existing Secrets Manager secret.
    /// </summary>
    public static async Task UpdateSecretAsync(
        string name,
        object value,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var raw = value is string s ? s : JsonSerializer.Serialize(value);
        try
        {
            await client.UpdateSecretAsync(new UpdateSecretRequest
            {
                SecretId = name,
                SecretString = raw
            });
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update secret '{name}'");
        }
    }

    /// <summary>
    /// Delete a Secrets Manager secret.
    /// </summary>
    public static async Task DeleteSecretAsync(
        string name,
        int recoveryWindowInDays = 30,
        bool forceDelete = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteSecretRequest { SecretId = name };
        if (forceDelete)
            request.ForceDeleteWithoutRecovery = true;
        else
            request.RecoveryWindowInDays = recoveryWindowInDays;

        try
        {
            await client.DeleteSecretAsync(request);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete secret '{name}'");
        }
    }

    /// <summary>
    /// List secrets in Secrets Manager, optionally filtered by name prefix.
    /// </summary>
    public static async Task<List<Dictionary<string, object?>>> ListSecretsAsync(
        string? namePrefix = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var secrets = new List<Dictionary<string, object?>>();
        var request = new ListSecretsRequest();
        if (namePrefix != null)
            request.Filters = [new Filter { Key = FilterNameStringType.Name, Values = [namePrefix] }];

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListSecretsAsync(request);
                foreach (var s in resp.SecretList)
                {
                    secrets.Add(new Dictionary<string, object?>
                    {
                        ["name"] = s.Name,
                        ["arn"] = s.ARN,
                        ["description"] = s.Description ?? "",
                        ["last_changed_date"] = s.LastChangedDate,
                        ["last_accessed_date"] = s.LastAccessedDate,
                        ["rotation_enabled"] = s.RotationEnabled
                    });
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "list_secrets failed");
        }
        return secrets;
    }

    /// <summary>
    /// Trigger an immediate rotation of a Secrets Manager secret.
    /// </summary>
    public static async Task RotateSecretAsync(
        string name,
        string? lambdaArn = null,
        int? rotationDays = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RotateSecretRequest
        {
            SecretId = name,
            RotateImmediately = true
        };
        if (lambdaArn != null)
        {
            request.RotationLambdaARN = lambdaArn;
            if (rotationDays.HasValue)
                request.RotationRules = new RotationRulesType
                {
                    AutomaticallyAfterDays = rotationDays.Value
                };
        }

        try
        {
            await client.RotateSecretAsync(request);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to rotate secret '{name}'");
        }
    }

    /// <summary>
    /// Fetch a secret value from AWS Secrets Manager.
    /// Supports "name", "name:jsonKey", and ARN forms.
    /// </summary>
    public static async Task<string> GetSecretAsync(
        string inner,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);

        string? jsonKey = null;
        string secretId;
        if (inner.Contains(':'))
        {
            var lastColon = inner.LastIndexOf(':');
            secretId = inner[..lastColon];
            jsonKey = inner[(lastColon + 1)..];
        }
        else
        {
            secretId = inner;
        }

        GetSecretValueResponse resp;
        try
        {
            resp = await client.GetSecretValueAsync(new GetSecretValueRequest { SecretId = secretId });
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Error resolving secret '{secretId}'");
        }

        var secretStr = resp.SecretString ?? System.Text.Encoding.UTF8.GetString(resp.SecretBinary.ToArray());

        if (jsonKey == null)
            return secretStr;

        Dictionary<string, JsonElement> data;
        try
        {
            data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(secretStr)!;
        }
        catch (JsonException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Secret '{secretId}' is not valid JSON; cannot extract key '{jsonKey}'");
        }

        if (!data.ContainsKey(jsonKey))
            throw new KeyNotFoundException($"Key '{jsonKey}' not found in secret '{secretId}'");

        return data[jsonKey].ToString();
    }

    /// <summary>
    /// Describe secret.
    /// </summary>
    public static async Task<DescribeSecretResult> DescribeSecretAsync(
        string secretId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSecretAsync(new DescribeSecretRequest { SecretId = secretId });
            return new DescribeSecretResult(
                Arn: resp.ARN,
                Name: resp.Name,
                Description: resp.Description,
                KmsKeyId: resp.KmsKeyId,
                RotationEnabled: resp.RotationEnabled,
                RotationLambdaArn: resp.RotationLambdaARN,
                LastChangedDate: resp.LastChangedDate.ToString(),
                LastAccessedDate: resp.LastAccessedDate.ToString(),
                CreatedDate: resp.CreatedDate.ToString(),
                PrimaryRegion: resp.PrimaryRegion);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe secret");
        }
    }

    /// <summary>
    /// Get secret value with full details.
    /// </summary>
    public static async Task<GetSecretValueResult> GetSecretValueAsync(
        string secretId,
        string? versionId = null,
        string? versionStage = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSecretValueRequest { SecretId = secretId };
        if (versionId != null) request.VersionId = versionId;
        if (versionStage != null) request.VersionStage = versionStage;

        try
        {
            var resp = await client.GetSecretValueAsync(request);
            return new GetSecretValueResult(
                Arn: resp.ARN,
                Name: resp.Name,
                VersionId: resp.VersionId,
                SecretBinary: resp.SecretBinary?.ToArray(),
                SecretString: resp.SecretString,
                VersionStages: resp.VersionStages,
                CreatedDate: resp.CreatedDate.ToString());
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get secret value");
        }
    }

    /// <summary>
    /// Get random password.
    /// </summary>
    public static async Task<GetRandomPasswordResult> GetRandomPasswordAsync(
        int? passwordLength = null,
        string? excludeCharacters = null,
        bool? excludeNumbers = null,
        bool? excludePunctuation = null,
        bool? excludeUppercase = null,
        bool? excludeLowercase = null,
        bool? includeSpace = null,
        bool? requireEachIncludedType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRandomPasswordRequest();
        if (passwordLength.HasValue) request.PasswordLength = passwordLength.Value;
        if (excludeCharacters != null) request.ExcludeCharacters = excludeCharacters;
        if (excludeNumbers.HasValue) request.ExcludeNumbers = excludeNumbers.Value;
        if (excludePunctuation.HasValue) request.ExcludePunctuation = excludePunctuation.Value;
        if (excludeUppercase.HasValue) request.ExcludeUppercase = excludeUppercase.Value;
        if (excludeLowercase.HasValue) request.ExcludeLowercase = excludeLowercase.Value;
        if (includeSpace.HasValue) request.IncludeSpace = includeSpace.Value;
        if (requireEachIncludedType.HasValue) request.RequireEachIncludedType = requireEachIncludedType.Value;

        try
        {
            var resp = await client.GetRandomPasswordAsync(request);
            return new GetRandomPasswordResult(RandomPassword: resp.RandomPassword);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get random password");
        }
    }

    /// <summary>
    /// Tag resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string secretId, List<Tag> tags, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest { SecretId = secretId, Tags = tags });
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resource");
        }
    }

    /// <summary>
    /// Untag resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string secretId, List<string> tagKeys, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest { SecretId = secretId, TagKeys = tagKeys });
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag resource");
        }
    }

    /// <summary>
    /// Restore secret.
    /// </summary>
    public static async Task<RestoreSecretResult> RestoreSecretAsync(
        string secretId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreSecretAsync(new RestoreSecretRequest { SecretId = secretId });
            return new RestoreSecretResult(Arn: resp.ARN, Name: resp.Name);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to restore secret");
        }
    }

    /// <summary>
    /// Get resource policy.
    /// </summary>
    public static async Task<GetResourcePolicyResult> GetResourcePolicyAsync(
        string secretId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetResourcePolicyAsync(new GetResourcePolicyRequest { SecretId = secretId });
            return new GetResourcePolicyResult(Arn: resp.ARN, Name: resp.Name, ResourcePolicy: resp.ResourcePolicy);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get resource policy");
        }
    }

    /// <summary>
    /// Put resource policy.
    /// </summary>
    public static async Task<PutResourcePolicyResult> PutResourcePolicyAsync(
        string secretId,
        string resourcePolicy,
        bool? blockPublicPolicy = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutResourcePolicyRequest
        {
            SecretId = secretId,
            ResourcePolicy = resourcePolicy
        };
        if (blockPublicPolicy.HasValue)
            request.BlockPublicPolicy = blockPublicPolicy.Value;

        try
        {
            var resp = await client.PutResourcePolicyAsync(request);
            return new PutResourcePolicyResult(Arn: resp.ARN, Name: resp.Name);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put resource policy");
        }
    }

    /// <summary>
    /// Delete resource policy.
    /// </summary>
    public static async Task<DeleteResourcePolicyResult> DeleteResourcePolicyAsync(
        string secretId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteResourcePolicyAsync(new DeleteResourcePolicyRequest { SecretId = secretId });
            return new DeleteResourcePolicyResult(Arn: resp.ARN, Name: resp.Name);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete resource policy");
        }
    }

    /// <summary>
    /// Cancel rotate secret.
    /// </summary>
    public static async Task<CancelRotateSecretResult> CancelRotateSecretAsync(
        string secretId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelRotateSecretAsync(new CancelRotateSecretRequest { SecretId = secretId });
            return new CancelRotateSecretResult(Arn: resp.ARN, Name: resp.Name, VersionId: resp.VersionId);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to cancel rotate secret");
        }
    }

    /// <summary>
    /// Put secret value.
    /// </summary>
    public static async Task<PutSecretValueResult> PutSecretValueAsync(
        string secretId,
        string? clientRequestToken = null,
        byte[]? secretBinary = null,
        string? secretString = null,
        List<string>? versionStages = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutSecretValueRequest { SecretId = secretId };
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (secretBinary != null) request.SecretBinary = new MemoryStream(secretBinary);
        if (secretString != null) request.SecretString = secretString;
        if (versionStages != null) request.VersionStages = versionStages;

        try
        {
            var resp = await client.PutSecretValueAsync(request);
            return new PutSecretValueResult(
                Arn: resp.ARN, Name: resp.Name,
                VersionId: resp.VersionId, VersionStages: resp.VersionStages);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put secret value");
        }
    }

    /// <summary>
    /// Update secret version stage.
    /// </summary>
    public static async Task<UpdateSecretVersionStageResult> UpdateSecretVersionStageAsync(
        string secretId,
        string versionStage,
        string? removeFromVersionId = null,
        string? moveToVersionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateSecretVersionStageRequest
        {
            SecretId = secretId,
            VersionStage = versionStage
        };
        if (removeFromVersionId != null) request.RemoveFromVersionId = removeFromVersionId;
        if (moveToVersionId != null) request.MoveToVersionId = moveToVersionId;

        try
        {
            var resp = await client.UpdateSecretVersionStageAsync(request);
            return new UpdateSecretVersionStageResult(Arn: resp.ARN, Name: resp.Name);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update secret version stage");
        }
    }

    /// <summary>
    /// Validate resource policy.
    /// </summary>
    public static async Task<ValidateResourcePolicyResult> ValidateResourcePolicyAsync(
        string resourcePolicy,
        string? secretId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ValidateResourcePolicyRequest { ResourcePolicy = resourcePolicy };
        if (secretId != null) request.SecretId = secretId;

        try
        {
            var resp = await client.ValidateResourcePolicyAsync(request);
            return new ValidateResourcePolicyResult(PolicyValidationPassed: resp.PolicyValidationPassed);
        }
        catch (AmazonSecretsManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to validate resource policy");
        }
    }
}
