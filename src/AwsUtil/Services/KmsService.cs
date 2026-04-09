using Amazon;
using Amazon.KeyManagementService;
using Amazon.KeyManagementService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateKeyResult(
    string? KeyId = null,
    string? Arn = null,
    string? Description = null,
    string? KeyState = null,
    string? KeyUsage = null,
    DateTime? CreationDate = null);

public sealed record DescribeKeyResult(
    string? KeyId = null,
    string? Arn = null,
    string? Description = null,
    string? KeyState = null,
    string? KeyUsage = null,
    string? KeySpec = null,
    string? Origin = null,
    string? KeyManager = null,
    bool? Enabled = null,
    DateTime? CreationDate = null,
    DateTime? DeletionDate = null);

public sealed record ListKeysResult(
    List<KeyListEntry>? Keys = null,
    string? NextMarker = null,
    bool? Truncated = null);

public sealed record ScheduleKeyDeletionResult(
    string? KeyId = null,
    DateTime? DeletionDate = null,
    string? KeyState = null,
    int? PendingWindowInDays = null);

public sealed record EncryptResult(
    byte[]? CiphertextBlob = null,
    string? KeyId = null,
    string? EncryptionAlgorithm = null);

public sealed record DecryptResult(
    byte[]? Plaintext = null,
    string? KeyId = null,
    string? EncryptionAlgorithm = null);

public sealed record ReEncryptResult(
    byte[]? CiphertextBlob = null,
    string? SourceKeyId = null,
    string? KeyId = null,
    string? SourceEncryptionAlgorithm = null,
    string? DestinationEncryptionAlgorithm = null);

public sealed record GenerateDataKeyResult(
    byte[]? CiphertextBlob = null,
    byte[]? Plaintext = null,
    string? KeyId = null);

public sealed record GenerateDataKeyWithoutPlaintextResult(
    byte[]? CiphertextBlob = null,
    string? KeyId = null);

public sealed record KmsListAliasesResult(
    List<AliasListEntry>? Aliases = null,
    string? NextMarker = null,
    bool? Truncated = null);

public sealed record CreateGrantResult(
    string? GrantToken = null,
    string? GrantId = null);

public sealed record ListGrantsResult(
    List<GrantListEntry>? Grants = null,
    string? NextMarker = null,
    bool? Truncated = null);

public sealed record GetKeyRotationStatusResult(
    bool? KeyRotationEnabled = null);

public sealed record GetKeyPolicyResult(
    string? Policy = null);

public sealed record ListKeyPoliciesResult(
    List<string>? PolicyNames = null,
    string? NextMarker = null,
    bool? Truncated = null);

public sealed record ListResourceTagsResult(
    List<Amazon.KeyManagementService.Model.Tag>? Tags = null,
    string? NextMarker = null,
    bool? Truncated = null);

public sealed record GetPublicKeyResult(
    string? KeyId = null,
    byte[]? PublicKey = null,
    string? KeySpec = null,
    string? KeyUsage = null,
    List<string>? EncryptionAlgorithms = null,
    List<string>? SigningAlgorithms = null);

public sealed record SignResult(
    string? KeyId = null,
    byte[]? Signature = null,
    string? SigningAlgorithm = null);

public sealed record VerifyResult(
    string? KeyId = null,
    bool? SignatureValid = null,
    string? SigningAlgorithm = null);

public sealed record GenerateRandomResult(
    byte[]? Plaintext = null);

public sealed record GetParametersForImportResult(
    string? KeyId = null,
    byte[]? ImportToken = null,
    byte[]? PublicKey = null,
    DateTime? ParametersValidTo = null);

public sealed record ReplicateKeyResult(
    string? ReplicaKeyArn = null,
    string? ReplicaKeyId = null,
    string? ReplicaKeyState = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Key Management Service (KMS).
/// </summary>
public static class KmsService
{
    private static AmazonKeyManagementServiceClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKeyManagementServiceClient>(region);

    /// <summary>
    /// Create a new KMS key.
    /// </summary>
    public static async Task<CreateKeyResult> CreateKeyAsync(
        CreateKeyRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new CreateKeyRequest();

        try
        {
            var resp = await client.CreateKeyAsync(request);
            var m = resp.KeyMetadata;
            return new CreateKeyResult(
                KeyId: m.KeyId,
                Arn: m.Arn,
                Description: m.Description,
                KeyState: m.KeyState?.Value,
                KeyUsage: m.KeyUsage?.Value,
                CreationDate: m.CreationDate);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create KMS key");
        }
    }

    /// <summary>
    /// Describe a KMS key.
    /// </summary>
    public static async Task<DescribeKeyResult> DescribeKeyAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeKeyAsync(new DescribeKeyRequest { KeyId = keyId });
            var m = resp.KeyMetadata;
            return new DescribeKeyResult(
                KeyId: m.KeyId,
                Arn: m.Arn,
                Description: m.Description,
                KeyState: m.KeyState?.Value,
                KeyUsage: m.KeyUsage?.Value,
                KeySpec: m.KeySpec?.Value,
                Origin: m.Origin?.Value,
                KeyManager: m.KeyManager?.Value,
                Enabled: m.Enabled,
                CreationDate: m.CreationDate,
                DeletionDate: m.DeletionDate);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// List KMS keys.
    /// </summary>
    public static async Task<ListKeysResult> ListKeysAsync(
        int? limit = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListKeysRequest();
        if (limit.HasValue) request.Limit = limit.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListKeysAsync(request);
            return new ListKeysResult(
                Keys: resp.Keys,
                NextMarker: resp.NextMarker,
                Truncated: resp.Truncated);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list KMS keys");
        }
    }

    /// <summary>
    /// Enable a KMS key.
    /// </summary>
    public static async Task EnableKeyAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableKeyAsync(new EnableKeyRequest { KeyId = keyId });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to enable KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Disable a KMS key.
    /// </summary>
    public static async Task DisableKeyAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableKeyAsync(new DisableKeyRequest { KeyId = keyId });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to disable KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Schedule a KMS key for deletion.
    /// </summary>
    public static async Task<ScheduleKeyDeletionResult> ScheduleKeyDeletionAsync(
        string keyId,
        int? pendingWindowInDays = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ScheduleKeyDeletionRequest { KeyId = keyId };
        if (pendingWindowInDays.HasValue)
            request.PendingWindowInDays = pendingWindowInDays.Value;

        try
        {
            var resp = await client.ScheduleKeyDeletionAsync(request);
            return new ScheduleKeyDeletionResult(
                KeyId: resp.KeyId,
                DeletionDate: resp.DeletionDate,
                KeyState: resp.KeyState?.Value,
                PendingWindowInDays: resp.PendingWindowInDays);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to schedule KMS key deletion for '{keyId}'");
        }
    }

    /// <summary>
    /// Cancel a scheduled KMS key deletion.
    /// </summary>
    public static async Task CancelKeyDeletionAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CancelKeyDeletionAsync(new CancelKeyDeletionRequest { KeyId = keyId });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to cancel KMS key deletion for '{keyId}'");
        }
    }

    /// <summary>
    /// Encrypt data using a KMS key.
    /// </summary>
    public static async Task<EncryptResult> EncryptAsync(
        string keyId,
        MemoryStream plaintext,
        string? encryptionAlgorithm = null,
        Dictionary<string, string>? encryptionContext = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = plaintext
        };
        if (encryptionAlgorithm != null)
            request.EncryptionAlgorithm = new EncryptionAlgorithmSpec(encryptionAlgorithm);
        if (encryptionContext != null) request.EncryptionContext = encryptionContext;
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.EncryptAsync(request);
            return new EncryptResult(
                CiphertextBlob: resp.CiphertextBlob?.ToArray(),
                KeyId: resp.KeyId,
                EncryptionAlgorithm: resp.EncryptionAlgorithm?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to encrypt with KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Decrypt data using a KMS key.
    /// </summary>
    public static async Task<DecryptResult> DecryptAsync(
        MemoryStream ciphertextBlob,
        string? keyId = null,
        string? encryptionAlgorithm = null,
        Dictionary<string, string>? encryptionContext = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DecryptRequest
        {
            CiphertextBlob = ciphertextBlob
        };
        if (keyId != null) request.KeyId = keyId;
        if (encryptionAlgorithm != null)
            request.EncryptionAlgorithm = new EncryptionAlgorithmSpec(encryptionAlgorithm);
        if (encryptionContext != null) request.EncryptionContext = encryptionContext;
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.DecryptAsync(request);
            return new DecryptResult(
                Plaintext: resp.Plaintext?.ToArray(),
                KeyId: resp.KeyId,
                EncryptionAlgorithm: resp.EncryptionAlgorithm?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to decrypt with KMS");
        }
    }

    /// <summary>
    /// Re-encrypt data from one KMS key to another.
    /// </summary>
    public static async Task<ReEncryptResult> ReEncryptAsync(
        MemoryStream ciphertextBlob,
        string destinationKeyId,
        string? sourceKeyId = null,
        string? sourceEncryptionAlgorithm = null,
        string? destinationEncryptionAlgorithm = null,
        Dictionary<string, string>? sourceEncryptionContext = null,
        Dictionary<string, string>? destinationEncryptionContext = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ReEncryptRequest
        {
            CiphertextBlob = ciphertextBlob,
            DestinationKeyId = destinationKeyId
        };
        if (sourceKeyId != null) request.SourceKeyId = sourceKeyId;
        if (sourceEncryptionAlgorithm != null)
            request.SourceEncryptionAlgorithm = new EncryptionAlgorithmSpec(sourceEncryptionAlgorithm);
        if (destinationEncryptionAlgorithm != null)
            request.DestinationEncryptionAlgorithm = new EncryptionAlgorithmSpec(destinationEncryptionAlgorithm);
        if (sourceEncryptionContext != null) request.SourceEncryptionContext = sourceEncryptionContext;
        if (destinationEncryptionContext != null)
            request.DestinationEncryptionContext = destinationEncryptionContext;
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.ReEncryptAsync(request);
            return new ReEncryptResult(
                CiphertextBlob: resp.CiphertextBlob?.ToArray(),
                SourceKeyId: resp.SourceKeyId,
                KeyId: resp.KeyId,
                SourceEncryptionAlgorithm: resp.SourceEncryptionAlgorithm?.Value,
                DestinationEncryptionAlgorithm: resp.DestinationEncryptionAlgorithm?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to re-encrypt with KMS");
        }
    }

    /// <summary>
    /// Generate a data key.
    /// </summary>
    public static async Task<GenerateDataKeyResult> GenerateDataKeyAsync(
        string keyId,
        string? keySpec = null,
        int? numberOfBytes = null,
        Dictionary<string, string>? encryptionContext = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GenerateDataKeyRequest { KeyId = keyId };
        if (keySpec != null) request.KeySpec = new DataKeySpec(keySpec);
        if (numberOfBytes.HasValue) request.NumberOfBytes = numberOfBytes.Value;
        if (encryptionContext != null) request.EncryptionContext = encryptionContext;
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.GenerateDataKeyAsync(request);
            return new GenerateDataKeyResult(
                CiphertextBlob: resp.CiphertextBlob?.ToArray(),
                Plaintext: resp.Plaintext?.ToArray(),
                KeyId: resp.KeyId);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to generate data key for '{keyId}'");
        }
    }

    /// <summary>
    /// Generate a data key without plaintext.
    /// </summary>
    public static async Task<GenerateDataKeyWithoutPlaintextResult> GenerateDataKeyWithoutPlaintextAsync(
        string keyId,
        string? keySpec = null,
        int? numberOfBytes = null,
        Dictionary<string, string>? encryptionContext = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GenerateDataKeyWithoutPlaintextRequest { KeyId = keyId };
        if (keySpec != null) request.KeySpec = new DataKeySpec(keySpec);
        if (numberOfBytes.HasValue) request.NumberOfBytes = numberOfBytes.Value;
        if (encryptionContext != null) request.EncryptionContext = encryptionContext;
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.GenerateDataKeyWithoutPlaintextAsync(request);
            return new GenerateDataKeyWithoutPlaintextResult(
                CiphertextBlob: resp.CiphertextBlob?.ToArray(),
                KeyId: resp.KeyId);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to generate data key without plaintext for '{keyId}'");
        }
    }

    /// <summary>
    /// Create a KMS alias.
    /// </summary>
    public static async Task CreateAliasAsync(
        string aliasName,
        string targetKeyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateAliasAsync(new CreateAliasRequest
            {
                AliasName = aliasName,
                TargetKeyId = targetKeyId
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create alias '{aliasName}'");
        }
    }

    /// <summary>
    /// Delete a KMS alias.
    /// </summary>
    public static async Task DeleteAliasAsync(
        string aliasName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAliasAsync(new DeleteAliasRequest { AliasName = aliasName });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete alias '{aliasName}'");
        }
    }

    /// <summary>
    /// List KMS aliases.
    /// </summary>
    public static async Task<KmsListAliasesResult> ListAliasesAsync(
        string? keyId = null,
        int? limit = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAliasesRequest();
        if (keyId != null) request.KeyId = keyId;
        if (limit.HasValue) request.Limit = limit.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListAliasesAsync(request);
            return new KmsListAliasesResult(
                Aliases: resp.Aliases,
                NextMarker: resp.NextMarker,
                Truncated: resp.Truncated);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list aliases");
        }
    }

    /// <summary>
    /// Update a KMS alias to point to a different key.
    /// </summary>
    public static async Task UpdateAliasAsync(
        string aliasName,
        string targetKeyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateAliasAsync(new UpdateAliasRequest
            {
                AliasName = aliasName,
                TargetKeyId = targetKeyId
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update alias '{aliasName}'");
        }
    }

    /// <summary>
    /// Create a KMS grant.
    /// </summary>
    public static async Task<CreateGrantResult> CreateGrantAsync(
        CreateGrantRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGrantAsync(request);
            return new CreateGrantResult(
                GrantToken: resp.GrantToken,
                GrantId: resp.GrantId);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create grant");
        }
    }

    /// <summary>
    /// Retire a KMS grant.
    /// </summary>
    public static async Task RetireGrantAsync(
        string? grantToken = null,
        string? keyId = null,
        string? grantId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RetireGrantRequest();
        if (grantToken != null) request.GrantToken = grantToken;
        if (keyId != null) request.KeyId = keyId;
        if (grantId != null) request.GrantId = grantId;

        try
        {
            await client.RetireGrantAsync(request);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to retire grant");
        }
    }

    /// <summary>
    /// Revoke a KMS grant.
    /// </summary>
    public static async Task RevokeGrantAsync(
        string keyId,
        string grantId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RevokeGrantAsync(new RevokeGrantRequest
            {
                KeyId = keyId,
                GrantId = grantId
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to revoke grant '{grantId}'");
        }
    }

    /// <summary>
    /// List KMS grants.
    /// </summary>
    public static async Task<ListGrantsResult> ListGrantsAsync(
        string keyId,
        int? limit = null,
        string? marker = null,
        string? granteePrincipal = null,
        string? grantId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGrantsRequest { KeyId = keyId };
        if (limit.HasValue) request.Limit = limit.Value;
        if (marker != null) request.Marker = marker;
        if (granteePrincipal != null) request.GranteePrincipal = granteePrincipal;
        if (grantId != null) request.GrantId = grantId;

        try
        {
            var resp = await client.ListGrantsAsync(request);
            return new ListGrantsResult(
                Grants: resp.Grants,
                NextMarker: resp.NextMarker,
                Truncated: resp.Truncated);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list grants for key '{keyId}'");
        }
    }

    /// <summary>
    /// Enable automatic key rotation for a KMS key.
    /// </summary>
    public static async Task EnableKeyRotationAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableKeyRotationAsync(new EnableKeyRotationRequest { KeyId = keyId });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to enable key rotation for '{keyId}'");
        }
    }

    /// <summary>
    /// Disable automatic key rotation for a KMS key.
    /// </summary>
    public static async Task DisableKeyRotationAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableKeyRotationAsync(new DisableKeyRotationRequest { KeyId = keyId });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to disable key rotation for '{keyId}'");
        }
    }

    /// <summary>
    /// Get the key rotation status for a KMS key.
    /// </summary>
    public static async Task<GetKeyRotationStatusResult> GetKeyRotationStatusAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetKeyRotationStatusAsync(new GetKeyRotationStatusRequest
            {
                KeyId = keyId
            });
            return new GetKeyRotationStatusResult(
                KeyRotationEnabled: resp.KeyRotationEnabled);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get key rotation status for '{keyId}'");
        }
    }

    /// <summary>
    /// Get the key policy for a KMS key.
    /// </summary>
    public static async Task<GetKeyPolicyResult> GetKeyPolicyAsync(
        string keyId,
        string policyName = "default",
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetKeyPolicyAsync(new GetKeyPolicyRequest
            {
                KeyId = keyId,
                PolicyName = policyName
            });
            return new GetKeyPolicyResult(Policy: resp.Policy);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get key policy for '{keyId}'");
        }
    }

    /// <summary>
    /// Put a key policy for a KMS key.
    /// </summary>
    public static async Task PutKeyPolicyAsync(
        string keyId,
        string policy,
        string policyName = "default",
        bool? bypassPolicyLockoutSafetyCheck = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutKeyPolicyRequest
        {
            KeyId = keyId,
            Policy = policy,
            PolicyName = policyName
        };
        if (bypassPolicyLockoutSafetyCheck.HasValue)
            request.BypassPolicyLockoutSafetyCheck = bypassPolicyLockoutSafetyCheck.Value;

        try
        {
            await client.PutKeyPolicyAsync(request);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put key policy for '{keyId}'");
        }
    }

    /// <summary>
    /// List key policies for a KMS key.
    /// </summary>
    public static async Task<ListKeyPoliciesResult> ListKeyPoliciesAsync(
        string keyId,
        int? limit = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListKeyPoliciesRequest { KeyId = keyId };
        if (limit.HasValue) request.Limit = limit.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListKeyPoliciesAsync(request);
            return new ListKeyPoliciesResult(
                PolicyNames: resp.PolicyNames,
                NextMarker: resp.NextMarker,
                Truncated: resp.Truncated);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list key policies for '{keyId}'");
        }
    }

    /// <summary>
    /// Tag a KMS resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string keyId,
        List<Amazon.KeyManagementService.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                KeyId = keyId,
                Tags = tags
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Untag a KMS resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string keyId,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                KeyId = keyId,
                TagKeys = tagKeys
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// List resource tags for a KMS key.
    /// </summary>
    public static async Task<ListResourceTagsResult> ListResourceTagsAsync(
        string keyId,
        int? limit = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListResourceTagsRequest { KeyId = keyId };
        if (limit.HasValue) request.Limit = limit.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListResourceTagsAsync(request);
            return new ListResourceTagsResult(
                Tags: resp.Tags,
                NextMarker: resp.NextMarker,
                Truncated: resp.Truncated);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list resource tags for key '{keyId}'");
        }
    }

    /// <summary>
    /// Update the description of a KMS key.
    /// </summary>
    public static async Task UpdateKeyDescriptionAsync(
        string keyId,
        string description,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateKeyDescriptionAsync(new UpdateKeyDescriptionRequest
            {
                KeyId = keyId,
                Description = description
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update description for key '{keyId}'");
        }
    }

    /// <summary>
    /// Get the public key for an asymmetric KMS key.
    /// </summary>
    public static async Task<GetPublicKeyResult> GetPublicKeyAsync(
        string keyId,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPublicKeyRequest { KeyId = keyId };
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.GetPublicKeyAsync(request);
            return new GetPublicKeyResult(
                KeyId: resp.KeyId,
                PublicKey: resp.PublicKey?.ToArray(),
                KeySpec: resp.KeySpec?.Value,
                KeyUsage: resp.KeyUsage?.Value,
                EncryptionAlgorithms: resp.EncryptionAlgorithms?.ToList(),
                SigningAlgorithms: resp.SigningAlgorithms?.ToList());
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get public key for '{keyId}'");
        }
    }

    /// <summary>
    /// Sign data using a KMS key.
    /// </summary>
    public static async Task<SignResult> SignAsync(
        string keyId,
        MemoryStream message,
        string signingAlgorithm,
        string? messageType = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SignRequest
        {
            KeyId = keyId,
            Message = message,
            SigningAlgorithm = new SigningAlgorithmSpec(signingAlgorithm)
        };
        if (messageType != null) request.MessageType = new MessageType(messageType);
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.SignAsync(request);
            return new SignResult(
                KeyId: resp.KeyId,
                Signature: resp.Signature?.ToArray(),
                SigningAlgorithm: resp.SigningAlgorithm?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to sign with KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Verify a signature using a KMS key.
    /// </summary>
    public static async Task<VerifyResult> VerifyAsync(
        string keyId,
        MemoryStream message,
        MemoryStream signature,
        string signingAlgorithm,
        string? messageType = null,
        List<string>? grantTokens = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new VerifyRequest
        {
            KeyId = keyId,
            Message = message,
            Signature = signature,
            SigningAlgorithm = new SigningAlgorithmSpec(signingAlgorithm)
        };
        if (messageType != null) request.MessageType = new MessageType(messageType);
        if (grantTokens != null) request.GrantTokens = grantTokens;

        try
        {
            var resp = await client.VerifyAsync(request);
            return new VerifyResult(
                KeyId: resp.KeyId,
                SignatureValid: resp.SignatureValid,
                SigningAlgorithm: resp.SigningAlgorithm?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to verify with KMS key '{keyId}'");
        }
    }

    /// <summary>
    /// Generate random bytes using KMS.
    /// </summary>
    public static async Task<GenerateRandomResult> GenerateRandomAsync(
        int numberOfBytes,
        string? customKeyStoreId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GenerateRandomRequest { NumberOfBytes = numberOfBytes };
        if (customKeyStoreId != null) request.CustomKeyStoreId = customKeyStoreId;

        try
        {
            var resp = await client.GenerateRandomAsync(request);
            return new GenerateRandomResult(Plaintext: resp.Plaintext?.ToArray());
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to generate random bytes");
        }
    }

    /// <summary>
    /// Import key material into a KMS key.
    /// </summary>
    public static async Task ImportKeyMaterialAsync(
        string keyId,
        MemoryStream importToken,
        MemoryStream encryptedKeyMaterial,
        DateTime? validTo = null,
        string? expirationModel = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ImportKeyMaterialRequest
        {
            KeyId = keyId,
            ImportToken = importToken,
            EncryptedKeyMaterial = encryptedKeyMaterial
        };
        if (validTo.HasValue) request.ValidTo = validTo.Value;
        if (expirationModel != null)
            request.ExpirationModel = new ExpirationModelType(expirationModel);

        try
        {
            await client.ImportKeyMaterialAsync(request);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to import key material for '{keyId}'");
        }
    }

    /// <summary>
    /// Delete imported key material from a KMS key.
    /// </summary>
    public static async Task DeleteImportedKeyMaterialAsync(
        string keyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteImportedKeyMaterialAsync(new DeleteImportedKeyMaterialRequest
            {
                KeyId = keyId
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete imported key material for '{keyId}'");
        }
    }

    /// <summary>
    /// Get parameters for importing key material.
    /// </summary>
    public static async Task<GetParametersForImportResult> GetParametersForImportAsync(
        string keyId,
        string wrappingAlgorithm,
        string wrappingKeySpec,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetParametersForImportAsync(new GetParametersForImportRequest
            {
                KeyId = keyId,
                WrappingAlgorithm = new AlgorithmSpec(wrappingAlgorithm),
                WrappingKeySpec = new WrappingKeySpec(wrappingKeySpec)
            });
            return new GetParametersForImportResult(
                KeyId: resp.KeyId,
                ImportToken: resp.ImportToken?.ToArray(),
                PublicKey: resp.PublicKey?.ToArray(),
                ParametersValidTo: resp.ParametersValidTo);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get parameters for import for '{keyId}'");
        }
    }

    /// <summary>
    /// Replicate a KMS key to another region.
    /// </summary>
    public static async Task<ReplicateKeyResult> ReplicateKeyAsync(
        string keyId,
        string replicaRegion,
        string? policy = null,
        string? description = null,
        List<Amazon.KeyManagementService.Model.Tag>? tags = null,
        bool? bypassPolicyLockoutSafetyCheck = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ReplicateKeyRequest
        {
            KeyId = keyId,
            ReplicaRegion = replicaRegion
        };
        if (policy != null) request.Policy = policy;
        if (description != null) request.Description = description;
        if (tags != null) request.Tags = tags;
        if (bypassPolicyLockoutSafetyCheck.HasValue)
            request.BypassPolicyLockoutSafetyCheck = bypassPolicyLockoutSafetyCheck.Value;

        try
        {
            var resp = await client.ReplicateKeyAsync(request);
            var m = resp.ReplicaKeyMetadata;
            return new ReplicateKeyResult(
                ReplicaKeyArn: m.Arn,
                ReplicaKeyId: m.KeyId,
                ReplicaKeyState: m.KeyState?.Value);
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to replicate key '{keyId}'");
        }
    }

    /// <summary>
    /// Update the primary region for a multi-region KMS key.
    /// </summary>
    public static async Task UpdatePrimaryRegionAsync(
        string keyId,
        string primaryRegion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdatePrimaryRegionAsync(new UpdatePrimaryRegionRequest
            {
                KeyId = keyId,
                PrimaryRegion = primaryRegion
            });
        }
        catch (AmazonKeyManagementServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update primary region for key '{keyId}'");
        }
    }
}
