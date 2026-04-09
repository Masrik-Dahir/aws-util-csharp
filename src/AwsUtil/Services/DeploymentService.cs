using System.IO.Compression;
using System.Text.Json;
using Amazon;
using Amazon.CloudFormation;
using Amazon.CloudFormation.Model;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Amazon.S3;
using Amazon.S3.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of a Lambda canary deployment.</summary>
public sealed record LambdaCanaryDeployResult(
    string? FunctionName = null,
    string? NewVersion = null,
    string? AliasName = null,
    double CanaryWeight = 0.0,
    bool Promoted = false);

/// <summary>Result of publishing a Lambda layer.</summary>
public sealed record LambdaLayerPublisherResult(
    string? LayerVersionArn = null,
    string? LayerName = null,
    long? Version = null);

/// <summary>Result of deploying a CloudFormation stack.</summary>
public sealed record StackDeployerResult(
    string? StackId = null,
    string? StackName = null,
    string? Status = null);

/// <summary>Result of promoting an environment.</summary>
public sealed record EnvironmentPromoterResult(
    string? SourceEnvironment = null,
    string? TargetEnvironment = null,
    string? FunctionName = null,
    string? PromotedVersion = null,
    bool Success = true);

/// <summary>Result of a Lambda warmer invocation.</summary>
public sealed record LambdaWarmerResult(
    string? FunctionName = null,
    int InvocationsSent = 0,
    int SuccessCount = 0,
    int FailureCount = 0);

/// <summary>Drift item detected in configuration.</summary>
public sealed record ConfigDriftItem(
    string ResourceType,
    string ResourceId,
    string? ExpectedValue = null,
    string? ActualValue = null,
    string? DriftStatus = null);

/// <summary>Result of config drift detection.</summary>
public sealed record ConfigDriftDetectorResult(
    string? StackName = null,
    List<ConfigDriftItem>? DriftedResources = null,
    int DriftedCount = 0,
    string? DetectionStatus = null);

/// <summary>Result of a rollback operation.</summary>
public sealed record RollbackManagerResult(
    string? FunctionName = null,
    string? RolledBackToVersion = null,
    string? AliasName = null,
    bool Success = true);

/// <summary>Result of building a Lambda deployment package.</summary>
public sealed record LambdaPackageBuilderResult(
    string? Bucket = null,
    string? Key = null,
    long PackageSizeBytes = 0,
    bool Success = true);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Deployment and release management utilities for Lambda, CloudFormation,
/// and related services.
/// </summary>
public static class DeploymentService
{
    private static AmazonLambdaClient GetLambdaClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLambdaClient>(region);

    private static AmazonCloudFormationClient GetCfnClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudFormationClient>(region);

    private static AmazonS3Client GetS3Client(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonS3Client>(region);

    /// <summary>
    /// Perform a canary deployment for a Lambda function by publishing a new version
    /// and shifting a percentage of traffic to it via an alias.
    /// </summary>
    public static async Task<LambdaCanaryDeployResult> LambdaCanaryDeployAsync(
        string functionName,
        string aliasName,
        double canaryWeight = 0.1,
        bool promoteImmediately = false,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);

        try
        {
            // Publish a new version
            var publishResp = await client.PublishVersionAsync(
                new PublishVersionRequest { FunctionName = functionName });
            var newVersion = publishResp.Version;

            // Get the current alias to find the existing stable version
            GetAliasResponse aliasResp;
            string currentVersion;
            try
            {
                aliasResp = await client.GetAliasAsync(new GetAliasRequest
                {
                    FunctionName = functionName,
                    Name = aliasName
                });
                currentVersion = aliasResp.FunctionVersion;
            }
            catch (ResourceNotFoundException)
            {
                // Alias doesn't exist, create it pointing to the new version
                await client.CreateAliasAsync(new CreateAliasRequest
                {
                    FunctionName = functionName,
                    Name = aliasName,
                    FunctionVersion = newVersion
                });

                return new LambdaCanaryDeployResult(
                    FunctionName: functionName,
                    NewVersion: newVersion,
                    AliasName: aliasName,
                    Promoted: true);
            }

            if (promoteImmediately)
            {
                await client.UpdateAliasAsync(new UpdateAliasRequest
                {
                    FunctionName = functionName,
                    Name = aliasName,
                    FunctionVersion = newVersion,
                    RoutingConfig = null
                });

                return new LambdaCanaryDeployResult(
                    FunctionName: functionName,
                    NewVersion: newVersion,
                    AliasName: aliasName,
                    Promoted: true);
            }

            // Set up canary: main traffic to current, canary weight to new
            await client.UpdateAliasAsync(new UpdateAliasRequest
            {
                FunctionName = functionName,
                Name = aliasName,
                FunctionVersion = currentVersion,
                RoutingConfig = new AliasRoutingConfiguration
                {
                    AdditionalVersionWeights = new Dictionary<string, double>
                    {
                        [newVersion] = canaryWeight
                    }
                }
            });

            return new LambdaCanaryDeployResult(
                FunctionName: functionName,
                NewVersion: newVersion,
                AliasName: aliasName,
                CanaryWeight: canaryWeight);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to perform canary deployment for '{functionName}'");
        }
    }

    /// <summary>
    /// Publish a Lambda layer from an S3 object or zip content.
    /// </summary>
    public static async Task<LambdaLayerPublisherResult> LambdaLayerPublisherAsync(
        string layerName,
        string? s3Bucket = null,
        string? s3Key = null,
        byte[]? zipContent = null,
        string? description = null,
        List<string>? compatibleRuntimes = null,
        List<string>? compatibleArchitectures = null,
        string? licenseInfo = null,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);

        try
        {
            var request = new PublishLayerVersionRequest
            {
                LayerName = layerName
            };

            if (s3Bucket != null && s3Key != null)
            {
                request.Content = new LayerVersionContentInput
                {
                    S3Bucket = s3Bucket,
                    S3Key = s3Key
                };
            }
            else if (zipContent != null)
            {
                request.Content = new LayerVersionContentInput
                {
                    ZipFile = new MemoryStream(zipContent)
                };
            }

            if (description != null) request.Description = description;
            if (compatibleRuntimes != null)
                request.CompatibleRuntimes = compatibleRuntimes
                    .Select(r => new Runtime(r).Value).ToList();
            if (compatibleArchitectures != null)
                request.CompatibleArchitectures = compatibleArchitectures
                    .Select(a => new Architecture(a).Value).ToList();
            if (licenseInfo != null) request.LicenseInfo = licenseInfo;

            var resp = await client.PublishLayerVersionAsync(request);

            return new LambdaLayerPublisherResult(
                LayerVersionArn: resp.LayerVersionArn,
                LayerName: layerName,
                Version: resp.Version);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to publish Lambda layer '{layerName}'");
        }
    }

    /// <summary>
    /// Deploy a CloudFormation stack (create or update) and wait for completion.
    /// </summary>
    public static async Task<StackDeployerResult> StackDeployerAsync(
        string stackName,
        string? templateBody = null,
        string? templateUrl = null,
        List<Parameter>? parameters = null,
        List<string>? capabilities = null,
        Dictionary<string, string>? tags = null,
        int timeoutMinutes = 30,
        int pollIntervalSeconds = 10,
        RegionEndpoint? region = null)
    {
        var client = GetCfnClient(region);

        try
        {
            string? stackId;

            // Check if stack exists
            bool stackExists;
            try
            {
                var descResp = await client.DescribeStacksAsync(
                    new DescribeStacksRequest { StackName = stackName });
                stackExists = descResp.Stacks.Count > 0 &&
                              descResp.Stacks[0].StackStatus != StackStatus.DELETE_COMPLETE;
            }
            catch (AmazonCloudFormationException ex) when (
                ex.Message.Contains("does not exist"))
            {
                stackExists = false;
            }

            if (stackExists)
            {
                var updateReq = new UpdateStackRequest { StackName = stackName };
                if (templateBody != null) updateReq.TemplateBody = templateBody;
                if (templateUrl != null) updateReq.TemplateURL = templateUrl;
                if (parameters != null) updateReq.Parameters = parameters;
                if (capabilities != null)
                    updateReq.Capabilities = capabilities
                        .Select(c => new Capability(c).Value).ToList();
                if (tags != null)
                    updateReq.Tags = tags.Select(kv =>
                        new Amazon.CloudFormation.Model.Tag
                        {
                            Key = kv.Key,
                            Value = kv.Value
                        }).ToList();

                var updateResp = await client.UpdateStackAsync(updateReq);
                stackId = updateResp.StackId;
            }
            else
            {
                var createReq = new CreateStackRequest { StackName = stackName };
                if (templateBody != null) createReq.TemplateBody = templateBody;
                if (templateUrl != null) createReq.TemplateURL = templateUrl;
                if (parameters != null) createReq.Parameters = parameters;
                if (capabilities != null)
                    createReq.Capabilities = capabilities
                        .Select(c => new Capability(c).Value).ToList();
                if (tags != null)
                    createReq.Tags = tags.Select(kv =>
                        new Amazon.CloudFormation.Model.Tag
                        {
                            Key = kv.Key,
                            Value = kv.Value
                        }).ToList();
                createReq.TimeoutInMinutes = timeoutMinutes;

                var createResp = await client.CreateStackAsync(createReq);
                stackId = createResp.StackId;
            }

            // Poll for completion
            var deadline = DateTime.UtcNow.AddMinutes(timeoutMinutes);
            while (DateTime.UtcNow < deadline)
            {
                var descResp = await client.DescribeStacksAsync(
                    new DescribeStacksRequest { StackName = stackId });
                if (descResp.Stacks.Count == 0)
                    break;

                var status = descResp.Stacks[0].StackStatus.Value;
                if (status.EndsWith("_COMPLETE") || status.EndsWith("_FAILED"))
                {
                    return new StackDeployerResult(
                        StackId: stackId,
                        StackName: stackName,
                        Status: status);
                }

                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }

            return new StackDeployerResult(
                StackId: stackId,
                StackName: stackName,
                Status: "TIMED_OUT_WAITING");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deploy stack '{stackName}'");
        }
    }

    /// <summary>
    /// Promote a Lambda function version from a source environment (alias) to a
    /// target environment (alias).
    /// </summary>
    public static async Task<EnvironmentPromoterResult> EnvironmentPromoterAsync(
        string functionName,
        string sourceAlias,
        string targetAlias,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);

        try
        {
            // Get the version from the source alias
            var sourceResp = await client.GetAliasAsync(new GetAliasRequest
            {
                FunctionName = functionName,
                Name = sourceAlias
            });
            var version = sourceResp.FunctionVersion;

            // Update or create the target alias
            try
            {
                await client.UpdateAliasAsync(new UpdateAliasRequest
                {
                    FunctionName = functionName,
                    Name = targetAlias,
                    FunctionVersion = version,
                    RoutingConfig = null
                });
            }
            catch (ResourceNotFoundException)
            {
                await client.CreateAliasAsync(new CreateAliasRequest
                {
                    FunctionName = functionName,
                    Name = targetAlias,
                    FunctionVersion = version
                });
            }

            return new EnvironmentPromoterResult(
                SourceEnvironment: sourceAlias,
                TargetEnvironment: targetAlias,
                FunctionName: functionName,
                PromotedVersion: version);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to promote '{functionName}' from '{sourceAlias}' to '{targetAlias}'");
        }
    }

    /// <summary>
    /// Invoke a Lambda function multiple times concurrently to keep it warm.
    /// </summary>
    public static async Task<LambdaWarmerResult> LambdaWarmerAsync(
        string functionName,
        int concurrency = 5,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);
        var successCount = 0;
        var failureCount = 0;

        try
        {
            var warmPayload = JsonSerializer.Serialize(
                new Dictionary<string, object> { ["source"] = "warmer" });

            var tasks = Enumerable.Range(0, concurrency).Select(async _ =>
            {
                try
                {
                    var req = new InvokeRequest
                    {
                        FunctionName = functionName,
                        InvocationType = InvocationType.RequestResponse,
                        Payload = warmPayload
                    };
                    if (qualifier != null) req.Qualifier = qualifier;

                    await client.InvokeAsync(req);
                    Interlocked.Increment(ref successCount);
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref failureCount);
                }
            });

            await Task.WhenAll(tasks);

            return new LambdaWarmerResult(
                FunctionName: functionName,
                InvocationsSent: concurrency,
                SuccessCount: successCount,
                FailureCount: failureCount);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to warm Lambda function '{functionName}'");
        }
    }

    /// <summary>
    /// Detect configuration drift in a CloudFormation stack.
    /// </summary>
    public static async Task<ConfigDriftDetectorResult> ConfigDriftDetectorAsync(
        string stackName,
        int pollIntervalSeconds = 10,
        int timeoutSeconds = 300,
        RegionEndpoint? region = null)
    {
        var client = GetCfnClient(region);

        try
        {
            var detectResp = await client.DetectStackDriftAsync(
                new DetectStackDriftRequest { StackName = stackName });
            var detectionId = detectResp.StackDriftDetectionId;

            // Poll for detection completion
            var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
            string? detectionStatus = null;

            while (DateTime.UtcNow < deadline)
            {
                var statusResp = await client.DescribeStackDriftDetectionStatusAsync(
                    new DescribeStackDriftDetectionStatusRequest
                    {
                        StackDriftDetectionId = detectionId
                    });

                detectionStatus = statusResp.DetectionStatus.Value;
                if (detectionStatus == "DETECTION_COMPLETE" ||
                    detectionStatus == "DETECTION_FAILED")
                    break;

                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }

            // Get drifted resources
            var driftResp = await client.DescribeStackResourceDriftsAsync(
                new DescribeStackResourceDriftsRequest
                {
                    StackName = stackName,
                    StackResourceDriftStatusFilters = new List<string>
                    {
                        "MODIFIED", "DELETED"
                    }
                });

            var driftedResources = driftResp.StackResourceDrifts
                .Select(d => new ConfigDriftItem(
                    ResourceType: d.ResourceType,
                    ResourceId: d.LogicalResourceId,
                    ExpectedValue: d.ExpectedProperties,
                    ActualValue: d.ActualProperties,
                    DriftStatus: d.StackResourceDriftStatus?.Value))
                .ToList();

            return new ConfigDriftDetectorResult(
                StackName: stackName,
                DriftedResources: driftedResources,
                DriftedCount: driftedResources.Count,
                DetectionStatus: detectionStatus);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detect config drift for stack '{stackName}'");
        }
    }

    /// <summary>
    /// Roll back a Lambda function alias to a previous version.
    /// </summary>
    public static async Task<RollbackManagerResult> RollbackManagerAsync(
        string functionName,
        string aliasName,
        string targetVersion,
        RegionEndpoint? region = null)
    {
        var client = GetLambdaClient(region);

        try
        {
            await client.UpdateAliasAsync(new UpdateAliasRequest
            {
                FunctionName = functionName,
                Name = aliasName,
                FunctionVersion = targetVersion,
                RoutingConfig = null
            });

            return new RollbackManagerResult(
                FunctionName: functionName,
                RolledBackToVersion: targetVersion,
                AliasName: aliasName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rollback '{functionName}' alias '{aliasName}' " +
                $"to version '{targetVersion}'");
        }
    }

    /// <summary>
    /// Build a Lambda deployment package by zipping source files and uploading to S3.
    /// </summary>
    public static async Task<LambdaPackageBuilderResult> LambdaPackageBuilderAsync(
        string sourcePath,
        string bucket,
        string key,
        List<string>? excludePatterns = null,
        RegionEndpoint? region = null)
    {
        var s3 = GetS3Client(region);

        try
        {
            using var memoryStream = new MemoryStream();
            using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
            {
                var sourceDir = new DirectoryInfo(sourcePath);
                if (!sourceDir.Exists)
                    throw new ArgumentException($"Source path '{sourcePath}' does not exist.");

                var files = sourceDir.GetFiles("*", SearchOption.AllDirectories);
                foreach (var file in files)
                {
                    var relativePath = Path.GetRelativePath(sourcePath, file.FullName)
                        .Replace('\\', '/');

                    if (excludePatterns != null &&
                        excludePatterns.Any(p => relativePath.Contains(p)))
                        continue;

                    var entry = archive.CreateEntry(relativePath, CompressionLevel.Optimal);
                    using var entryStream = entry.Open();
                    using var fileStream = file.OpenRead();
                    await fileStream.CopyToAsync(entryStream);
                }
            }

            memoryStream.Position = 0;
            var packageSize = memoryStream.Length;

            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = memoryStream,
                ContentType = "application/zip"
            });

            return new LambdaPackageBuilderResult(
                Bucket: bucket,
                Key: key,
                PackageSizeBytes: packageSize);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to build Lambda package from '{sourcePath}'");
        }
    }
}
