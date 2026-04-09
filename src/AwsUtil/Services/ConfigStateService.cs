using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Resolved configuration from multiple sources with provenance tracking.</summary>
public sealed record ConfigResolverResult(
    Dictionary<string, string> ResolvedConfig,
    Dictionary<string, string> Sources,
    List<string> UnresolvedKeys);

/// <summary>Result of acquiring or releasing a distributed lock.</summary>
public sealed record DistributedLockResult(
    string LockId,
    string ResourceName,
    bool Acquired,
    string? OwnerId = null,
    DateTime? ExpiresAt = null,
    string? Reason = null);

/// <summary>State machine checkpoint stored in DynamoDB.</summary>
public sealed record StateMachineCheckpointResult(
    string ExecutionId,
    string StateName,
    string Status,
    Dictionary<string, object?>? StateData = null,
    DateTime? CheckpointedAt = null);

/// <summary>Result of assuming a cross-account role.</summary>
public sealed record CrossAccountRoleAssumerResult(
    string RoleArn,
    string? AccessKeyId,
    string? SecretAccessKey,
    string? SessionToken,
    DateTime? Expiration,
    string? AssumedRoleArn);

/// <summary>Result of syncing environment variables to Lambda functions.</summary>
public sealed record EnvironmentVariableSyncResult(
    int FunctionsUpdated,
    int FunctionsSkipped,
    List<string> FailedFunctions,
    Dictionary<string, string> SyncedVariables);

/// <summary>Result of loading feature flags from AppConfig / Parameter Store.</summary>
public sealed record AppConfigFeatureLoaderResult(
    Dictionary<string, bool> FeatureFlags,
    Dictionary<string, string> RawValues,
    string Source,
    DateTime LoadedAt);

/// <summary>
/// Configuration resolution, distributed locking, state machine checkpoints,
/// and feature flag loading across Parameter Store, Secrets Manager, DynamoDB,
/// STS, and Lambda.
/// </summary>
public static class ConfigStateService
{
    /// <summary>
    /// Resolve configuration values from multiple sources (Parameter Store,
    /// Secrets Manager, environment variables) with priority ordering.
    /// </summary>
    public static async Task<ConfigResolverResult> ConfigResolverAsync(
        List<string> keys,
        string? parameterStorePrefix = null,
        string? secretsManagerPrefix = null,
        Dictionary<string, string>? environmentOverrides = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var resolved = new Dictionary<string, string>();
            var sources = new Dictionary<string, string>();
            var unresolved = new List<string>();

            // Priority 1: Environment overrides (highest)
            if (environmentOverrides != null)
            {
                foreach (var key in keys.Where(k => environmentOverrides.ContainsKey(k)))
                {
                    resolved[key] = environmentOverrides[key];
                    sources[key] = "environment-override";
                }
            }

            // Priority 2: Secrets Manager
            var remainingKeys = keys.Where(k => !resolved.ContainsKey(k)).ToList();
            if (secretsManagerPrefix != null)
            {
                foreach (var key in remainingKeys.ToList())
                {
                    try
                    {
                        var secretName = $"{secretsManagerPrefix}/{key}";
                        var secret = await SecretsManagerService.GetSecretValueAsync(
                            secretName, region: region);
                        if (secret.SecretString != null)
                        {
                            resolved[key] = secret.SecretString;
                            sources[key] = $"secrets-manager:{secretName}";
                            remainingKeys.Remove(key);
                        }
                    }
                    catch (AwsNotFoundException)
                    {
                        // Not found in Secrets Manager, try next source
                    }
                }
            }

            // Priority 3: Parameter Store (lowest)
            if (parameterStorePrefix != null)
            {
                foreach (var key in remainingKeys.ToList())
                {
                    try
                    {
                        var paramName = $"{parameterStorePrefix}/{key}";
                        var param = await ParameterStoreService.GetParameterAsync(
                            paramName, withDecryption: true, region: region);
                        if (param != null)
                        {
                            resolved[key] = param;
                            sources[key] = $"parameter-store:{paramName}";
                            remainingKeys.Remove(key);
                        }
                    }
                    catch (AwsNotFoundException)
                    {
                        // Not found in Parameter Store
                    }
                }
            }

            unresolved.AddRange(remainingKeys);

            return new ConfigResolverResult(
                ResolvedConfig: resolved,
                Sources: sources,
                UnresolvedKeys: unresolved);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to resolve configuration");
        }
    }

    /// <summary>
    /// Acquire a distributed lock using a DynamoDB table with conditional writes.
    /// </summary>
    public static async Task<DistributedLockResult> DistributedLockAsync(
        string tableName,
        string resourceName,
        string ownerId,
        int ttlSeconds = 30,
        bool release = false,
        RegionEndpoint? region = null)
    {
        try
        {
            var lockId = $"lock#{resourceName}";
            var expiresAt = DateTime.UtcNow.AddSeconds(ttlSeconds);
            var expiresEpoch = new DateTimeOffset(expiresAt).ToUnixTimeSeconds();

            if (release)
            {
                // Release: delete the lock item if we own it
                try
                {
                    await DynamoDbService.DeleteItemAsync(
                        tableName,
                        new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                        {
                            ["pk"] = new() { S = lockId }
                        },
                        conditionExpression: "ownerId = :owner",
                        expressionAttributeValues: new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                        {
                            [":owner"] = new() { S = ownerId }
                        },
                        region: region);

                    return new DistributedLockResult(
                        LockId: lockId,
                        ResourceName: resourceName,
                        Acquired: false,
                        OwnerId: ownerId,
                        Reason: "Lock released");
                }
                catch (AwsConflictException)
                {
                    return new DistributedLockResult(
                        LockId: lockId,
                        ResourceName: resourceName,
                        Acquired: false,
                        OwnerId: ownerId,
                        Reason: "Lock not owned by this owner");
                }
            }

            // Acquire: put item with condition that it doesn't exist or is expired
            try
            {
                await DynamoDbService.PutItemAsync(
                    tableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = lockId },
                        ["ownerId"] = new() { S = ownerId },
                        ["expiresAt"] = new() { N = expiresEpoch.ToString() },
                        ["resourceName"] = new() { S = resourceName }
                    },
                    conditionExpression: "attribute_not_exists(pk) OR expiresAt < :now",
                    expressionAttributeValues: new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        [":now"] = new() { N = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() }
                    },
                    region: region);

                return new DistributedLockResult(
                    LockId: lockId,
                    ResourceName: resourceName,
                    Acquired: true,
                    OwnerId: ownerId,
                    ExpiresAt: expiresAt);
            }
            catch (AwsConflictException)
            {
                return new DistributedLockResult(
                    LockId: lockId,
                    ResourceName: resourceName,
                    Acquired: false,
                    OwnerId: ownerId,
                    Reason: "Lock already held by another owner");
            }
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Distributed lock operation failed");
        }
    }

    /// <summary>
    /// Save or retrieve a state machine execution checkpoint in DynamoDB.
    /// </summary>
    public static async Task<StateMachineCheckpointResult> StateMachineCheckpointAsync(
        string tableName,
        string executionId,
        string? stateName = null,
        string? status = null,
        Dictionary<string, object?>? stateData = null,
        bool retrieve = false,
        RegionEndpoint? region = null)
    {
        try
        {
            if (retrieve)
            {
                var item = await DynamoDbService.GetItemAsync(
                    tableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"checkpoint#{executionId}" }
                    },
                    region: region);

                return new StateMachineCheckpointResult(
                    ExecutionId: executionId,
                    StateName: item?.GetValueOrDefault("stateName")?.S ?? "unknown",
                    Status: item?.GetValueOrDefault("status")?.S ?? "unknown",
                    CheckpointedAt: DateTime.UtcNow);
            }

            var dataJson = stateData != null
                ? JsonSerializer.Serialize(stateData)
                : "{}";

            await DynamoDbService.PutItemAsync(
                tableName,
                new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                {
                    ["pk"] = new() { S = $"checkpoint#{executionId}" },
                    ["stateName"] = new() { S = stateName ?? "unknown" },
                    ["status"] = new() { S = status ?? "in-progress" },
                    ["stateData"] = new() { S = dataJson },
                    ["checkpointedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                },
                region: region);

            return new StateMachineCheckpointResult(
                ExecutionId: executionId,
                StateName: stateName ?? "unknown",
                Status: status ?? "in-progress",
                StateData: stateData,
                CheckpointedAt: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "State machine checkpoint operation failed");
        }
    }

    /// <summary>
    /// Assume a cross-account IAM role and return temporary credentials.
    /// </summary>
    public static async Task<CrossAccountRoleAssumerResult> CrossAccountRoleAssumerAsync(
        string roleArn,
        string sessionName,
        int? durationSeconds = null,
        string? externalId = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var result = await StsService.AssumeRoleAsync(
                roleArn: roleArn,
                roleSessionName: sessionName,
                durationSeconds: durationSeconds,
                externalId: externalId,
                region: region);

            return new CrossAccountRoleAssumerResult(
                RoleArn: roleArn,
                AccessKeyId: result.AccessKeyId,
                SecretAccessKey: result.SecretAccessKey,
                SessionToken: result.SessionToken,
                Expiration: result.Expiration != null
                    ? DateTime.Parse(result.Expiration)
                    : null,
                AssumedRoleArn: result.AssumedRoleArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to assume cross-account role");
        }
    }

    /// <summary>
    /// Sync environment variables from Parameter Store to Lambda function configurations.
    /// </summary>
    public static async Task<EnvironmentVariableSyncResult> EnvironmentVariableSyncAsync(
        List<string> functionNames,
        string parameterStorePrefix,
        List<string> variableNames,
        RegionEndpoint? region = null)
    {
        try
        {
            // Fetch all parameters
            var variables = new Dictionary<string, string>();
            foreach (var name in variableNames)
            {
                try
                {
                    var param = await ParameterStoreService.GetParameterAsync(
                        $"{parameterStorePrefix}/{name}",
                        withDecryption: true,
                        region: region);
                    if (param != null)
                        variables[name] = param;
                }
                catch (AwsNotFoundException)
                {
                    // Skip missing parameters
                }
            }

            var updated = 0;
            var skipped = 0;
            var failed = new List<string>();

            foreach (var fn in functionNames)
            {
                try
                {
                    var config = await LambdaService.GetFunctionConfigurationAsync(
                        fn, region: region);

                    var existingVars = (config.Environment?.GetValueOrDefault("Variables") as Dictionary<string, string>)
                        ?? new Dictionary<string, string>();

                    // Check if update is needed
                    var needsUpdate = variables.Any(v =>
                        !existingVars.TryGetValue(v.Key, out var existing) ||
                        existing != v.Value);

                    if (!needsUpdate)
                    {
                        skipped++;
                        continue;
                    }

                    // Merge variables
                    foreach (var (key, value) in variables)
                        existingVars[key] = value;

                    await LambdaService.UpdateFunctionConfigurationAsync(
                        fn,
                        environment: new Amazon.Lambda.Model.Environment { Variables = existingVars },
                        region: region);
                    updated++;
                }
                catch (Exception)
                {
                    failed.Add(fn);
                }
            }

            return new EnvironmentVariableSyncResult(
                FunctionsUpdated: updated,
                FunctionsSkipped: skipped,
                FailedFunctions: failed,
                SyncedVariables: variables);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to sync environment variables");
        }
    }

    /// <summary>
    /// Load feature flags from Parameter Store, treating parameters as boolean flags
    /// or JSON feature-flag documents.
    /// </summary>
    public static async Task<AppConfigFeatureLoaderResult> AppConfigFeatureLoaderAsync(
        string parameterPrefix,
        List<string> featureNames,
        RegionEndpoint? region = null)
    {
        try
        {
            var flags = new Dictionary<string, bool>();
            var rawValues = new Dictionary<string, string>();

            foreach (var feature in featureNames)
            {
                try
                {
                    var param = await ParameterStoreService.GetParameterAsync(
                        $"{parameterPrefix}/{feature}",
                        withDecryption: true,
                        region: region);

                    var value = param ?? "false";
                    rawValues[feature] = value;

                    // Parse as boolean
                    flags[feature] = value.Trim().ToLowerInvariant() switch
                    {
                        "true" or "1" or "yes" or "enabled" or "on" => true,
                        _ => false
                    };
                }
                catch (AwsNotFoundException)
                {
                    flags[feature] = false;
                    rawValues[feature] = "not-found";
                }
            }

            return new AppConfigFeatureLoaderResult(
                FeatureFlags: flags,
                RawValues: rawValues,
                Source: $"parameter-store:{parameterPrefix}",
                LoadedAt: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to load feature flags");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="ConfigResolverAsync"/>.</summary>
    public static ConfigResolverResult ConfigResolver(List<string> keys, string? parameterStorePrefix = null, string? secretsManagerPrefix = null, Dictionary<string, string>? environmentOverrides = null, RegionEndpoint? region = null)
        => ConfigResolverAsync(keys, parameterStorePrefix, secretsManagerPrefix, environmentOverrides, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DistributedLockAsync"/>.</summary>
    public static DistributedLockResult DistributedLock(string tableName, string resourceName, string ownerId, int ttlSeconds = 30, bool release = false, RegionEndpoint? region = null)
        => DistributedLockAsync(tableName, resourceName, ownerId, ttlSeconds, release, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StateMachineCheckpointAsync"/>.</summary>
    public static StateMachineCheckpointResult StateMachineCheckpoint(string tableName, string executionId, string? stateName = null, string? status = null, Dictionary<string, object?>? stateData = null, bool retrieve = false, RegionEndpoint? region = null)
        => StateMachineCheckpointAsync(tableName, executionId, stateName, status, stateData, retrieve, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CrossAccountRoleAssumerAsync"/>.</summary>
    public static CrossAccountRoleAssumerResult CrossAccountRoleAssumer(string roleArn, string sessionName, int? durationSeconds = null, string? externalId = null, RegionEndpoint? region = null)
        => CrossAccountRoleAssumerAsync(roleArn, sessionName, durationSeconds, externalId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnvironmentVariableSyncAsync"/>.</summary>
    public static EnvironmentVariableSyncResult EnvironmentVariableSync(List<string> functionNames, string parameterStorePrefix, List<string> variableNames, RegionEndpoint? region = null)
        => EnvironmentVariableSyncAsync(functionNames, parameterStorePrefix, variableNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AppConfigFeatureLoaderAsync"/>.</summary>
    public static AppConfigFeatureLoaderResult AppConfigFeatureLoader(string parameterPrefix, List<string> featureNames, RegionEndpoint? region = null)
        => AppConfigFeatureLoaderAsync(parameterPrefix, featureNames, region).GetAwaiter().GetResult();

}
