using System.Text.Json;
using Amazon;
using Amazon.ECS.Model;
using Amazon.Lambda.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of deploying a Lambda function with updated configuration.</summary>
public sealed record DeployLambdaWithConfigResult(
    string? FunctionName = null,
    string? FunctionArn = null,
    string? Version = null,
    string? CodeSha256 = null,
    long? ParameterVersion = null);

/// <summary>Result of deploying an ECS service from an ECR image.</summary>
public sealed record DeployEcsFromEcrResult(
    string? ServiceArn = null,
    string? ServiceName = null,
    string? TaskDefinitionArn = null,
    string? Status = null);

/// <summary>Result of deploying a Lambda function from an S3 ZIP.</summary>
public sealed record DeployLambdaFromS3Result(
    string? FunctionName = null,
    string? FunctionArn = null,
    string? Version = null,
    string? CodeSha256 = null,
    string? S3Bucket = null,
    string? S3Key = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Multi-service deployer combining Lambda, ECS, ECR, S3, and Parameter Store
/// into high-level deployment orchestration workflows.
/// </summary>
public static class DeployerService
{
    /// <summary>
    /// Deploy a Lambda function by updating its code from a local ZIP file and then
    /// writing (or overwriting) one or more SSM Parameter Store values that the
    /// function reads at runtime.
    /// </summary>
    /// <param name="functionName">Lambda function name or ARN.</param>
    /// <param name="zipFilePath">Local path to the deployment ZIP archive.</param>
    /// <param name="parameters">SSM parameter name/value pairs to set.</param>
    /// <param name="publish">Whether to publish a new function version.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<DeployLambdaWithConfigResult> DeployLambdaWithConfigAsync(
        string functionName,
        string zipFilePath,
        Dictionary<string, string> parameters,
        bool publish = true,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Read the ZIP bytes and update the Lambda function code
            var zipBytes = await File.ReadAllBytesAsync(zipFilePath);
            using var zipStream = new MemoryStream(zipBytes);

            var codeResult = await LambdaService.UpdateFunctionCodeAsync(
                functionName,
                zipFile: zipStream,
                publish: publish,
                region: region);

            // 2. Write each config parameter to SSM Parameter Store
            long? lastParameterVersion = null;
            foreach (var (name, value) in parameters)
            {
                lastParameterVersion = await ParameterStoreService.PutParameterAsync(
                    name,
                    value,
                    type: "String",
                    overwrite: true,
                    region: region);
            }

            return new DeployLambdaWithConfigResult(
                FunctionName: codeResult.FunctionName,
                FunctionArn: codeResult.FunctionArn,
                Version: codeResult.Version,
                CodeSha256: codeResult.CodeSha256,
                ParameterVersion: lastParameterVersion);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deploy Lambda '{functionName}' with config");
        }
    }

    /// <summary>
    /// Deploy an ECS service with a new container image from ECR.
    /// Registers a new task definition revision that references the given ECR image
    /// URI and then updates the target ECS service to use the new task definition.
    /// </summary>
    /// <param name="cluster">ECS cluster name or ARN.</param>
    /// <param name="serviceName">ECS service name.</param>
    /// <param name="taskFamily">Task definition family name.</param>
    /// <param name="containerName">Name of the container within the task definition.</param>
    /// <param name="imageUri">Full ECR image URI (e.g. <c>123456.dkr.ecr.us-east-1.amazonaws.com/repo:tag</c>).</param>
    /// <param name="cpu">Task-level CPU units (e.g. "256").</param>
    /// <param name="memory">Task-level memory in MiB (e.g. "512").</param>
    /// <param name="executionRoleArn">Task execution role ARN.</param>
    /// <param name="forceNewDeployment">Force a new deployment even if the task definition has not changed.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<DeployEcsFromEcrResult> DeployEcsFromEcrAsync(
        string cluster,
        string serviceName,
        string taskFamily,
        string containerName,
        string imageUri,
        string cpu = "256",
        string memory = "512",
        string? executionRoleArn = null,
        bool forceNewDeployment = true,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Register a new task definition revision with the updated image
            var registerRequest = new RegisterTaskDefinitionRequest
            {
                Family = taskFamily,
                NetworkMode = Amazon.ECS.NetworkMode.Awsvpc,
                RequiresCompatibilities = ["FARGATE"],
                Cpu = cpu,
                Memory = memory,
                ContainerDefinitions =
                [
                    new ContainerDefinition
                    {
                        Name = containerName,
                        Image = imageUri,
                        Essential = true
                    }
                ]
            };
            if (executionRoleArn != null)
                registerRequest.ExecutionRoleArn = executionRoleArn;

            var taskDefResult = await EcsService.RegisterTaskDefinitionAsync(
                registerRequest, region);

            // 2. Update the ECS service to use the new task definition revision
            var updateRequest = new UpdateServiceRequest
            {
                Cluster = cluster,
                Service = serviceName,
                TaskDefinition = taskDefResult.TaskDefinitionArn,
                ForceNewDeployment = forceNewDeployment
            };

            var serviceResult = await EcsService.UpdateServiceAsync(updateRequest, region);

            return new DeployEcsFromEcrResult(
                ServiceArn: serviceResult.ServiceArn,
                ServiceName: serviceResult.ServiceName,
                TaskDefinitionArn: taskDefResult.TaskDefinitionArn,
                Status: serviceResult.Status);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deploy ECS service '{serviceName}' from ECR image '{imageUri}'");
        }
    }

    /// <summary>
    /// Deploy a Lambda function from a ZIP archive stored in S3.
    /// Updates the function code to point at the specified S3 bucket/key and
    /// optionally publishes a new version.
    /// </summary>
    /// <param name="functionName">Lambda function name or ARN.</param>
    /// <param name="s3Bucket">S3 bucket containing the deployment ZIP.</param>
    /// <param name="s3Key">S3 key of the deployment ZIP.</param>
    /// <param name="s3ObjectVersion">Optional S3 object version ID.</param>
    /// <param name="publish">Whether to publish a new function version.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<DeployLambdaFromS3Result> DeployLambdaFromS3Async(
        string functionName,
        string s3Bucket,
        string s3Key,
        string? s3ObjectVersion = null,
        bool publish = true,
        RegionEndpoint? region = null)
    {
        try
        {
            // Verify the S3 object exists before attempting deployment
            var exists = await S3Service.ObjectExistsAsync(s3Bucket, s3Key, region);
            if (!exists)
            {
                throw new InvalidOperationException(
                    $"Deployment artifact not found at s3://{s3Bucket}/{s3Key}");
            }

            // Update the Lambda function code to reference the S3 object
            var codeResult = await LambdaService.UpdateFunctionCodeAsync(
                functionName,
                s3Bucket: s3Bucket,
                s3Key: s3Key,
                s3ObjectVersion: s3ObjectVersion,
                publish: publish,
                region: region);

            return new DeployLambdaFromS3Result(
                FunctionName: codeResult.FunctionName,
                FunctionArn: codeResult.FunctionArn,
                Version: codeResult.Version,
                CodeSha256: codeResult.CodeSha256,
                S3Bucket: s3Bucket,
                S3Key: s3Key);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to deploy Lambda '{functionName}' from s3://{s3Bucket}/{s3Key}");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="DeployLambdaWithConfigAsync"/>.</summary>
    public static DeployLambdaWithConfigResult DeployLambdaWithConfig(string functionName, string zipFilePath, Dictionary<string, string> parameters, bool publish = true, RegionEndpoint? region = null)
        => DeployLambdaWithConfigAsync(functionName, zipFilePath, parameters, publish, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeployEcsFromEcrAsync"/>.</summary>
    public static DeployEcsFromEcrResult DeployEcsFromEcr(string cluster, string serviceName, string taskFamily, string containerName, string imageUri, string cpu = "256", string memory = "512", string? executionRoleArn = null, bool forceNewDeployment = true, RegionEndpoint? region = null)
        => DeployEcsFromEcrAsync(cluster, serviceName, taskFamily, containerName, imageUri, cpu, memory, executionRoleArn, forceNewDeployment, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeployLambdaFromS3Async"/>.</summary>
    public static DeployLambdaFromS3Result DeployLambdaFromS3(string functionName, string s3Bucket, string s3Key, string? s3ObjectVersion = null, bool publish = true, RegionEndpoint? region = null)
        => DeployLambdaFromS3Async(functionName, s3Bucket, s3Key, s3ObjectVersion, publish, region).GetAwaiter().GetResult();

}
