using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of running a SageMaker pipeline.</summary>
public sealed record SageMakerPipelineRunnerResult(
    string PipelineName,
    string ExecutionArn,
    string Status,
    Dictionary<string, string>? Parameters = null,
    DateTime StartedAt = default);

/// <summary>Result of managing a model in the registry.</summary>
public sealed record ModelRegistryManagerResult(
    string ModelPackageGroupName,
    string ModelPackageArn,
    string ModelApprovalStatus,
    string Version,
    Dictionary<string, string>? Metadata = null);

/// <summary>Result of ingesting features into a feature store.</summary>
public sealed record FeatureStoreIngesterResult(
    string FeatureGroupName,
    int RecordsIngested,
    int RecordsFailed,
    double DurationMs);

/// <summary>Result of a batch transform job.</summary>
public sealed record BatchTransformOrchestratorResult(
    string TransformJobName,
    string InputS3Uri,
    string OutputS3Uri,
    string ModelName,
    string Status,
    DateTime StartedAt = default);

/// <summary>
/// ML pipeline orchestration combining S3, DynamoDB, Lambda, EventBridge,
/// and CloudWatch for SageMaker pipeline management, model registry,
/// feature store ingestion, and batch transform operations.
/// </summary>
public static class MlPipelineService
{
    /// <summary>
    /// Start a SageMaker pipeline execution by invoking a Lambda function
    /// that manages the pipeline, tracking execution state in DynamoDB.
    /// </summary>
    public static async Task<SageMakerPipelineRunnerResult> SageMakerPipelineRunnerAsync(
        string pipelineName,
        string executionLambdaArn,
        Dictionary<string, string>? parameters = null,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var executionId = $"{pipelineName}-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid():N}"[..63];

            // Invoke the pipeline execution Lambda
            var payload = JsonSerializer.Serialize(new
            {
                pipelineName,
                executionId,
                parameters = parameters ?? new Dictionary<string, string>()
            });

            var invokeResult = await LambdaService.InvokeAsync(
                executionLambdaArn,
                payload: payload,
                region: region);

            var executionArn = $"arn:aws:sagemaker:{region?.SystemName ?? "us-east-1"}:pipeline/{pipelineName}/execution/{executionId}";

            // Track execution in DynamoDB
            if (trackingTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    trackingTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"pipeline#{pipelineName}" },
                        ["sk"] = new() { S = $"execution#{executionId}" },
                        ["status"] = new() { S = "Executing" },
                        ["parameters"] = new() { S = JsonSerializer.Serialize(parameters ?? new Dictionary<string, string>()) },
                        ["startedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);
            }

            // Publish pipeline event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.ml-pipeline",
                        DetailType = "PipelineExecutionStarted",
                        Detail = JsonSerializer.Serialize(new
                        {
                            pipelineName,
                            executionId,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new SageMakerPipelineRunnerResult(
                PipelineName: pipelineName,
                ExecutionArn: executionArn,
                Status: invokeResult.Succeeded ? "Executing" : "Failed",
                Parameters: parameters,
                StartedAt: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "SageMaker pipeline execution failed");
        }
    }

    /// <summary>
    /// Register a model package in a model package group, storing metadata
    /// in DynamoDB and publishing a registration event.
    /// </summary>
    public static async Task<ModelRegistryManagerResult> ModelRegistryManagerAsync(
        string modelPackageGroupName,
        string modelArtifactS3Uri,
        string approvalStatus = "PendingManualApproval",
        Dictionary<string, string>? metadata = null,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
            var modelPackageArn = $"arn:aws:sagemaker:{region?.SystemName ?? "us-east-1"}:" +
                $"model-package/{modelPackageGroupName}/{version}";

            // Store model metadata in DynamoDB
            if (trackingTableName != null)
            {
                var item = new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                {
                    ["pk"] = new() { S = $"model-group#{modelPackageGroupName}" },
                    ["sk"] = new() { S = $"version#{version}" },
                    ["modelPackageArn"] = new() { S = modelPackageArn },
                    ["artifactS3Uri"] = new() { S = modelArtifactS3Uri },
                    ["approvalStatus"] = new() { S = approvalStatus },
                    ["registeredAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                };

                if (metadata != null)
                {
                    item["metadata"] = new() { S = JsonSerializer.Serialize(metadata) };
                }

                await DynamoDbService.PutItemAsync(trackingTableName, item, region: region);
            }

            // Store the model artifact reference
            await S3Service.PutObjectAsync(
                bucket: modelArtifactS3Uri.Split('/')[2], // Extract bucket from S3 URI
                key: $"model-registry/{modelPackageGroupName}/{version}/metadata.json",
                body: System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
                {
                    modelPackageGroupName,
                    version,
                    artifactUri = modelArtifactS3Uri,
                    approvalStatus,
                    metadata,
                    registeredAt = DateTime.UtcNow.ToString("o")
                })),
                region: region);

            // Publish event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.ml-pipeline",
                        DetailType = "ModelRegistered",
                        Detail = JsonSerializer.Serialize(new
                        {
                            modelPackageGroupName,
                            version,
                            approvalStatus,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new ModelRegistryManagerResult(
                ModelPackageGroupName: modelPackageGroupName,
                ModelPackageArn: modelPackageArn,
                ModelApprovalStatus: approvalStatus,
                Version: version,
                Metadata: metadata);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Model registry operation failed");
        }
    }

    /// <summary>
    /// Ingest feature records into a DynamoDB-backed feature store,
    /// tracking ingestion metrics in CloudWatch.
    /// </summary>
    public static async Task<FeatureStoreIngesterResult> FeatureStoreIngesterAsync(
        string featureGroupName,
        string featureTableName,
        List<Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>> records,
        RegionEndpoint? region = null)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var ingested = 0;
            var failed = 0;

            foreach (var record in records)
            {
                try
                {
                    // Add feature group metadata
                    record["featureGroupName"] = new() { S = featureGroupName };
                    record["ingestTime"] = new() { S = DateTime.UtcNow.ToString("o") };

                    await DynamoDbService.PutItemAsync(
                        featureTableName, record, region: region);
                    ingested++;
                }
                catch (Exception)
                {
                    failed++;
                }
            }

            sw.Stop();

            // Publish ingestion metrics
            await CloudWatchService.PutMetricDataAsync(
                metricNamespace: "AwsUtil/FeatureStore",
                metricData: new List<Amazon.CloudWatch.Model.MetricDatum>
                {
                    new()
                    {
                        MetricName = "RecordsIngested",
                        Value = ingested,
                        Unit = Amazon.CloudWatch.StandardUnit.Count,
                        Dimensions = new List<Amazon.CloudWatch.Model.Dimension>
                        {
                            new() { Name = "FeatureGroup", Value = featureGroupName }
                        }
                    },
                    new()
                    {
                        MetricName = "IngestionDurationMs",
                        Value = sw.Elapsed.TotalMilliseconds,
                        Unit = Amazon.CloudWatch.StandardUnit.Milliseconds,
                        Dimensions = new List<Amazon.CloudWatch.Model.Dimension>
                        {
                            new() { Name = "FeatureGroup", Value = featureGroupName }
                        }
                    }
                },
                region: region);

            return new FeatureStoreIngesterResult(
                FeatureGroupName: featureGroupName,
                RecordsIngested: ingested,
                RecordsFailed: failed,
                DurationMs: sw.Elapsed.TotalMilliseconds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Feature store ingestion failed");
        }
    }

    /// <summary>
    /// Orchestrate a batch transform job by preparing input data in S3,
    /// invoking a Lambda to start the transform, and tracking status.
    /// </summary>
    public static async Task<BatchTransformOrchestratorResult> BatchTransformOrchestratorAsync(
        string modelName,
        string inputS3Uri,
        string outputS3Uri,
        string transformLambdaArn,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var jobName = $"transform-{modelName}-{DateTime.UtcNow:yyyyMMddHHmmss}";

            // Invoke the transform orchestration Lambda
            var payload = JsonSerializer.Serialize(new
            {
                jobName,
                modelName,
                inputS3Uri,
                outputS3Uri
            });

            var invokeResult = await LambdaService.InvokeAsync(
                transformLambdaArn,
                payload: payload,
                region: region);

            // Track in DynamoDB
            if (trackingTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    trackingTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"transform#{jobName}" },
                        ["modelName"] = new() { S = modelName },
                        ["inputS3Uri"] = new() { S = inputS3Uri },
                        ["outputS3Uri"] = new() { S = outputS3Uri },
                        ["status"] = new() { S = "InProgress" },
                        ["startedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);
            }

            // Publish event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.ml-pipeline",
                        DetailType = "BatchTransformStarted",
                        Detail = JsonSerializer.Serialize(new
                        {
                            jobName,
                            modelName,
                            inputS3Uri,
                            outputS3Uri,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new BatchTransformOrchestratorResult(
                TransformJobName: jobName,
                InputS3Uri: inputS3Uri,
                OutputS3Uri: outputS3Uri,
                ModelName: modelName,
                Status: invokeResult.Succeeded ? "InProgress" : "Failed",
                StartedAt: DateTime.UtcNow);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Batch transform orchestration failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="SageMakerPipelineRunnerAsync"/>.</summary>
    public static SageMakerPipelineRunnerResult SageMakerPipelineRunner(string pipelineName, string executionLambdaArn, Dictionary<string, string>? parameters = null, string? trackingTableName = null, RegionEndpoint? region = null)
        => SageMakerPipelineRunnerAsync(pipelineName, executionLambdaArn, parameters, trackingTableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ModelRegistryManagerAsync"/>.</summary>
    public static ModelRegistryManagerResult ModelRegistryManager(string modelPackageGroupName, string modelArtifactS3Uri, string approvalStatus = "PendingManualApproval", Dictionary<string, string>? metadata = null, string? trackingTableName = null, RegionEndpoint? region = null)
        => ModelRegistryManagerAsync(modelPackageGroupName, modelArtifactS3Uri, approvalStatus, metadata, trackingTableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="FeatureStoreIngesterAsync"/>.</summary>
    public static FeatureStoreIngesterResult FeatureStoreIngester(string featureGroupName, string featureTableName, List<Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>> records, RegionEndpoint? region = null)
        => FeatureStoreIngesterAsync(featureGroupName, featureTableName, records, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchTransformOrchestratorAsync"/>.</summary>
    public static BatchTransformOrchestratorResult BatchTransformOrchestrator(string modelName, string inputS3Uri, string outputS3Uri, string transformLambdaArn, string? trackingTableName = null, RegionEndpoint? region = null)
        => BatchTransformOrchestratorAsync(modelName, inputS3Uri, outputS3Uri, transformLambdaArn, trackingTableName, region).GetAwaiter().GetResult();

}
