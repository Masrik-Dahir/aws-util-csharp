using System.Security.Cryptography;
using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of rotating an IAM access key.</summary>
public sealed record IamKeyRotatorResult(
    string UserName,
    string OldAccessKeyId,
    string NewAccessKeyId,
    string? NewSecretAccessKey,
    bool OldKeyDeactivated,
    string? SecretStorageArn = null);

/// <summary>Result of rotating an RDS database password.</summary>
public sealed record RdsPasswordRotatorResult(
    string DbInstanceIdentifier,
    bool PasswordRotated,
    string? SecretArn = null,
    DateTime RotatedAt = default);

/// <summary>Result of configuring automatic secret rotation.</summary>
public sealed record SecretsManagerAutoRotatorResult(
    string SecretArn,
    string RotationLambdaArn,
    int RotationIntervalDays,
    bool Configured,
    DateTime? NextRotationDate = null);

/// <summary>
/// Credential and secret rotation orchestrating IAM, Secrets Manager,
/// RDS, DynamoDB, and Lambda for automated credential lifecycle management.
/// </summary>
public static class CredentialRotationService
{
    /// <summary>
    /// Rotate an IAM user's access key by creating a new key, storing it
    /// in Secrets Manager, and deactivating the old key.
    /// </summary>
    public static async Task<IamKeyRotatorResult> IamKeyRotatorAsync(
        string userName,
        string? secretName = null,
        bool deactivateOldKey = true,
        string? auditTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // List current access keys
            var keys = await IamService.ListAccessKeysAsync(userName, region: region);
            var activeKeys = keys
                .Where(k => k.Status == "Active")
                .ToList();

            if (activeKeys.Count == 0)
                throw new AwsValidationException($"No active access keys found for user {userName}");

            var oldKey = activeKeys.First();
            var oldKeyId = oldKey.AccessKeyId;

            // Create new access key
            var newKey = await IamService.CreateAccessKeyAsync(userName, region: region);
            var newKeyId = newKey.AccessKeyId ?? "";
            var newSecret = newKey.SecretAccessKey;

            // Store new key in Secrets Manager
            string? secretArn = null;
            if (secretName != null && newSecret != null)
            {
                var secretValue = JsonSerializer.Serialize(new
                {
                    accessKeyId = newKeyId,
                    secretAccessKey = newSecret,
                    userName,
                    rotatedAt = DateTime.UtcNow.ToString("o")
                });

                try
                {
                    var updateResult = await SecretsManagerService.PutSecretValueAsync(
                        secretName, secretString: secretValue, region: region);
                    secretArn = updateResult.Arn;
                }
                catch (AwsNotFoundException)
                {
                    secretArn = await SecretsManagerService.CreateSecretAsync(
                        secretName, secretValue, region: region);
                }
            }

            // Deactivate old key
            if (deactivateOldKey)
            {
                await IamService.UpdateAccessKeyAsync(
                    userName, oldKeyId, "Inactive", region: region);
            }

            // Audit trail
            if (auditTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    auditTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"key-rotation#{userName}" },
                        ["sk"] = new() { S = DateTime.UtcNow.ToString("o") },
                        ["oldKeyId"] = new() { S = oldKeyId },
                        ["newKeyId"] = new() { S = newKeyId },
                        ["deactivated"] = new() { BOOL = deactivateOldKey }
                    },
                    region: region);
            }

            return new IamKeyRotatorResult(
                UserName: userName,
                OldAccessKeyId: oldKeyId,
                NewAccessKeyId: newKeyId,
                NewSecretAccessKey: newSecret,
                OldKeyDeactivated: deactivateOldKey,
                SecretStorageArn: secretArn);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "IAM key rotation failed");
        }
    }

    /// <summary>
    /// Rotate an RDS instance's master password, store the new password
    /// in Secrets Manager, and update dependent Lambda function environment variables.
    /// </summary>
    public static async Task<RdsPasswordRotatorResult> RdsPasswordRotatorAsync(
        string dbInstanceIdentifier,
        string secretName,
        List<string>? dependentFunctions = null,
        string? environmentVariableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Generate a new random password
            var newPassword = GenerateSecurePassword(32);

            // Modify the RDS instance with the new password
            await RdsService.ModifyDBInstanceAsync(
                new Amazon.RDS.Model.ModifyDBInstanceRequest
                {
                    DBInstanceIdentifier = dbInstanceIdentifier,
                    MasterUserPassword = newPassword,
                    ApplyImmediately = true
                },
                region: region);

            // Store the new password in Secrets Manager
            var secretValue = JsonSerializer.Serialize(new
            {
                password = newPassword,
                dbInstanceIdentifier,
                rotatedAt = DateTime.UtcNow.ToString("o")
            });

            string? secretArn = null;
            try
            {
                var updateResult = await SecretsManagerService.PutSecretValueAsync(
                    secretName, secretString: secretValue, region: region);
                secretArn = updateResult.Arn;
            }
            catch (AwsNotFoundException)
            {
                secretArn = await SecretsManagerService.CreateSecretAsync(
                    secretName, secretValue, region: region);
            }

            // Update dependent Lambda functions
            if (dependentFunctions != null && environmentVariableName != null)
            {
                foreach (var fn in dependentFunctions)
                {
                    try
                    {
                        var config = await LambdaService.GetFunctionConfigurationAsync(
                            fn, region: region);

                        var existingVars = new Dictionary<string, string>();
                        if (config.Environment is Dictionary<string, object?> envDict
                            && envDict.TryGetValue("Variables", out var varsObj)
                            && varsObj is Dictionary<string, string> parsedVars)
                        {
                            existingVars = parsedVars;
                        }
                        existingVars[environmentVariableName] = secretName; // Reference secret, not password

                        await LambdaService.UpdateFunctionConfigurationAsync(
                            fn, environment: new Amazon.Lambda.Model.Environment
                            {
                                Variables = existingVars
                            }, region: region);
                    }
                    catch (Exception)
                    {
                        // Continue with other functions
                    }
                }
            }

            return new RdsPasswordRotatorResult(
                DbInstanceIdentifier: dbInstanceIdentifier,
                PasswordRotated: true,
                SecretArn: secretArn,
                RotatedAt: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "RDS password rotation failed");
        }
    }

    /// <summary>
    /// Configure automatic rotation for a Secrets Manager secret using a
    /// Lambda rotation function with the specified interval.
    /// </summary>
    public static async Task<SecretsManagerAutoRotatorResult> SecretsManagerAutoRotatorAsync(
        string secretName,
        string rotationLambdaArn,
        int rotationIntervalDays = 30,
        RegionEndpoint? region = null)
    {
        try
        {
            // Enable rotation on the secret
            await SecretsManagerService.RotateSecretAsync(
                secretName,
                lambdaArn: rotationLambdaArn,
                rotationDays: rotationIntervalDays,
                region: region);

            // Get updated secret metadata
            var metadata = await SecretsManagerService.DescribeSecretAsync(
                secretName, region: region);

            DateTime? nextRotation = metadata.NextRotationDate != null
                ? DateTime.Parse(metadata.NextRotationDate)
                : null;

            // Publish event about rotation configuration
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.credential-rotation",
                        DetailType = "AutoRotationConfigured",
                        Detail = JsonSerializer.Serialize(new
                        {
                            secretName,
                            rotationLambdaArn,
                            rotationIntervalDays,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new SecretsManagerAutoRotatorResult(
                SecretArn: metadata.Arn ?? secretName,
                RotationLambdaArn: rotationLambdaArn,
                RotationIntervalDays: rotationIntervalDays,
                Configured: true,
                NextRotationDate: nextRotation);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Secrets Manager auto-rotation configuration failed");
        }
    }

    private static string GenerateSecurePassword(int length)
    {
        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*";
        var result = new char[length];
        var bytes = RandomNumberGenerator.GetBytes(length);
        for (var i = 0; i < length; i++)
            result[i] = chars[bytes[i] % chars.Length];
        return new string(result);
    }
}
