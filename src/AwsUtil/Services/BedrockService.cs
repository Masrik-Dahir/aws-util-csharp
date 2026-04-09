using System.Text.Json;
using Amazon;
using Amazon.Bedrock;
using Amazon.Bedrock.Model;
using Amazon.BedrockRuntime;
using Amazon.BedrockRuntime.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Bedrock operations.
/// </summary>
public sealed record ListFoundationModelsResult(
    List<FoundationModelSummary>? ModelSummaries = null);

public sealed record GetFoundationModelResult(
    FoundationModelDetails? ModelDetails = null);

public sealed record CreateModelCustomizationJobResult(string? JobArn = null);

public sealed record GetModelCustomizationJobResult(
    string? JobArn = null,
    string? JobName = null,
    string? Status = null,
    string? BaseModelIdentifier = null,
    string? CustomModelArn = null);

public sealed record ListModelCustomizationJobsResult(
    List<ModelCustomizationJobSummary>? ModelCustomizationJobSummaries = null);

public sealed record CreateProvisionedModelThroughputResult(
    string? ProvisionedModelArn = null);

public sealed record GetProvisionedModelThroughputResult(
    string? ProvisionedModelArn = null,
    string? ProvisionedModelName = null,
    string? ModelArn = null,
    string? Status = null,
    int? ModelUnits = null);

public sealed record ListProvisionedModelThroughputsResult(
    List<ProvisionedModelSummary>? ProvisionedModelSummaries = null);

public sealed record GetModelInvocationLoggingConfigurationResult(
    LoggingConfig? LoggingConfig = null);

public sealed record ListCustomModelsResult(
    List<CustomModelSummary>? ModelSummaries = null);

public sealed record GetCustomModelResult(
    string? ModelArn = null,
    string? ModelName = null,
    string? BaseModelArn = null);

public sealed record CreateGuardrailResult(
    string? GuardrailId = null,
    string? GuardrailArn = null);

public sealed record GetGuardrailResult(
    string? GuardrailId = null,
    string? GuardrailArn = null,
    string? Name = null,
    string? Status = null);

public sealed record ListGuardrailsResult(
    List<GuardrailSummary>? Guardrails = null);

public sealed record ListBedrockTagsResult(
    List<Amazon.Bedrock.Model.Tag>? Tags = null);

public sealed record InvokeModelResult(
    string? Body = null,
    string? ContentType = null);

public sealed record InvokeModelWithResponseStreamResult(
    string? ContentType = null);

public sealed record ConverseResult(
    ConverseOutput? Output = null,
    StopReason? StopReason = null,
    TokenUsage? Usage = null);

public sealed record ConverseStreamResult(
    string? StreamArn = null);

/// <summary>
/// Utility helpers for Amazon Bedrock and Bedrock Runtime.
/// </summary>
public static class BedrockService
{
    private static AmazonBedrockClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonBedrockClient>(region);

    private static AmazonBedrockRuntimeClient GetRuntimeClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonBedrockRuntimeClient>(region);

    // ══════════════════════════════════════════════════════════════════
    //  Bedrock (Control Plane)
    // ══════════════════════════════════════════════════════════════════

    // ──────────────────────────── Foundation Models ────────────────────────────

    /// <summary>
    /// List available foundation models.
    /// </summary>
    public static async Task<ListFoundationModelsResult> ListFoundationModelsAsync(
        string? byProvider = null,
        string? byOutputModality = null,
        string? byInferenceType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFoundationModelsRequest();
        if (byProvider != null) request.ByProvider = byProvider;
        if (byOutputModality != null) request.ByOutputModality = byOutputModality;
        if (byInferenceType != null) request.ByInferenceType = byInferenceType;

        try
        {
            var resp = await client.ListFoundationModelsAsync(request);
            return new ListFoundationModelsResult(ModelSummaries: resp.ModelSummaries);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Bedrock foundation models");
        }
    }

    /// <summary>
    /// Get details about a specific foundation model.
    /// </summary>
    public static async Task<GetFoundationModelResult> GetFoundationModelAsync(
        string modelIdentifier, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFoundationModelAsync(new GetFoundationModelRequest
            {
                ModelIdentifier = modelIdentifier
            });
            return new GetFoundationModelResult(ModelDetails: resp.ModelDetails);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock foundation model '{modelIdentifier}'");
        }
    }

    // ──────────────────────────── Model Customization Jobs ────────────────────────────

    /// <summary>
    /// Create a model customization job.
    /// </summary>
    public static async Task<CreateModelCustomizationJobResult> CreateModelCustomizationJobAsync(
        CreateModelCustomizationJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateModelCustomizationJobAsync(request);
            return new CreateModelCustomizationJobResult(JobArn: resp.JobArn);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock model customization job '{request.JobName}'");
        }
    }

    /// <summary>
    /// Get information about a model customization job.
    /// </summary>
    public static async Task<GetModelCustomizationJobResult> GetModelCustomizationJobAsync(
        string jobIdentifier, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetModelCustomizationJobAsync(
                new GetModelCustomizationJobRequest { JobIdentifier = jobIdentifier });
            return new GetModelCustomizationJobResult(
                JobArn: resp.JobArn,
                JobName: resp.JobName,
                Status: resp.Status?.Value,
                BaseModelIdentifier: resp.BaseModelArn,
                CustomModelArn: resp.OutputModelArn);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock model customization job '{jobIdentifier}'");
        }
    }

    /// <summary>
    /// List model customization jobs, automatically paginating.
    /// </summary>
    public static async Task<ListModelCustomizationJobsResult> ListModelCustomizationJobsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var jobs = new List<ModelCustomizationJobSummary>();
        var request = new ListModelCustomizationJobsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListModelCustomizationJobsAsync(request);
                jobs.AddRange(resp.ModelCustomizationJobSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Bedrock model customization jobs");
        }

        return new ListModelCustomizationJobsResult(ModelCustomizationJobSummaries: jobs);
    }

    /// <summary>
    /// Stop a model customization job.
    /// </summary>
    public static async Task StopModelCustomizationJobAsync(
        string jobIdentifier, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopModelCustomizationJobAsync(
                new StopModelCustomizationJobRequest { JobIdentifier = jobIdentifier });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Bedrock model customization job '{jobIdentifier}'");
        }
    }

    // ──────────────────────────── Provisioned Model Throughput ────────────────────────────

    /// <summary>
    /// Create a provisioned model throughput.
    /// </summary>
    public static async Task<CreateProvisionedModelThroughputResult>
        CreateProvisionedModelThroughputAsync(
            string provisionedModelName,
            string modelId,
            int modelUnits,
            Dictionary<string, string>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateProvisionedModelThroughputRequest
        {
            ProvisionedModelName = provisionedModelName,
            ModelId = modelId,
            ModelUnits = modelUnits
        };
        if (tags != null)
            request.Tags = tags.Select(kv =>
                new Amazon.Bedrock.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            var resp = await client.CreateProvisionedModelThroughputAsync(request);
            return new CreateProvisionedModelThroughputResult(
                ProvisionedModelArn: resp.ProvisionedModelArn);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock provisioned model throughput '{provisionedModelName}'");
        }
    }

    /// <summary>
    /// Delete a provisioned model throughput.
    /// </summary>
    public static async Task DeleteProvisionedModelThroughputAsync(
        string provisionedModelId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProvisionedModelThroughputAsync(
                new DeleteProvisionedModelThroughputRequest
                {
                    ProvisionedModelId = provisionedModelId
                });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock provisioned model throughput '{provisionedModelId}'");
        }
    }

    /// <summary>
    /// Get information about a provisioned model throughput.
    /// </summary>
    public static async Task<GetProvisionedModelThroughputResult>
        GetProvisionedModelThroughputAsync(
            string provisionedModelId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetProvisionedModelThroughputAsync(
                new GetProvisionedModelThroughputRequest
                {
                    ProvisionedModelId = provisionedModelId
                });
            return new GetProvisionedModelThroughputResult(
                ProvisionedModelArn: resp.ProvisionedModelArn,
                ProvisionedModelName: resp.ProvisionedModelName,
                ModelArn: resp.ModelArn,
                Status: resp.Status?.Value,
                ModelUnits: resp.DesiredModelUnits);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock provisioned model throughput '{provisionedModelId}'");
        }
    }

    /// <summary>
    /// List provisioned model throughputs, automatically paginating.
    /// </summary>
    public static async Task<ListProvisionedModelThroughputsResult>
        ListProvisionedModelThroughputsAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var models = new List<ProvisionedModelSummary>();
        var request = new ListProvisionedModelThroughputsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListProvisionedModelThroughputsAsync(request);
                models.AddRange(resp.ProvisionedModelSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Bedrock provisioned model throughputs");
        }

        return new ListProvisionedModelThroughputsResult(ProvisionedModelSummaries: models);
    }

    /// <summary>
    /// Update a provisioned model throughput.
    /// </summary>
    public static async Task UpdateProvisionedModelThroughputAsync(
        string provisionedModelId,
        string? desiredProvisionedModelName = null,
        string? desiredModelId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateProvisionedModelThroughputRequest
        {
            ProvisionedModelId = provisionedModelId
        };
        if (desiredProvisionedModelName != null)
            request.DesiredProvisionedModelName = desiredProvisionedModelName;
        if (desiredModelId != null)
            request.DesiredModelId = desiredModelId;

        try
        {
            await client.UpdateProvisionedModelThroughputAsync(request);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock provisioned model throughput '{provisionedModelId}'");
        }
    }

    // ──────────────────────────── Invocation Logging ────────────────────────────

    /// <summary>
    /// Get the model invocation logging configuration.
    /// </summary>
    public static async Task<GetModelInvocationLoggingConfigurationResult>
        GetModelInvocationLoggingConfigurationAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetModelInvocationLoggingConfigurationAsync(
                new GetModelInvocationLoggingConfigurationRequest());
            return new GetModelInvocationLoggingConfigurationResult(
                LoggingConfig: resp.LoggingConfig);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get Bedrock model invocation logging configuration");
        }
    }

    /// <summary>
    /// Put the model invocation logging configuration.
    /// </summary>
    public static async Task PutModelInvocationLoggingConfigurationAsync(
        LoggingConfig loggingConfig, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutModelInvocationLoggingConfigurationAsync(
                new PutModelInvocationLoggingConfigurationRequest
                {
                    LoggingConfig = loggingConfig
                });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put Bedrock model invocation logging configuration");
        }
    }

    // ──────────────────────────── Custom Models ────────────────────────────

    /// <summary>
    /// List custom models, automatically paginating.
    /// </summary>
    public static async Task<ListCustomModelsResult> ListCustomModelsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var models = new List<CustomModelSummary>();
        var request = new ListCustomModelsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListCustomModelsAsync(request);
                models.AddRange(resp.ModelSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Bedrock custom models");
        }

        return new ListCustomModelsResult(ModelSummaries: models);
    }

    /// <summary>
    /// Delete a custom model.
    /// </summary>
    public static async Task DeleteCustomModelAsync(
        string modelIdentifier, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCustomModelAsync(
                new DeleteCustomModelRequest { ModelIdentifier = modelIdentifier });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock custom model '{modelIdentifier}'");
        }
    }

    /// <summary>
    /// Get information about a custom model.
    /// </summary>
    public static async Task<GetCustomModelResult> GetCustomModelAsync(
        string modelIdentifier, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCustomModelAsync(
                new GetCustomModelRequest { ModelIdentifier = modelIdentifier });
            return new GetCustomModelResult(
                ModelArn: resp.ModelArn,
                ModelName: resp.ModelName,
                BaseModelArn: resp.BaseModelArn);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock custom model '{modelIdentifier}'");
        }
    }

    // ──────────────────────────── Guardrails ────────────────────────────

    /// <summary>
    /// Create a Bedrock guardrail.
    /// </summary>
    public static async Task<CreateGuardrailResult> CreateGuardrailAsync(
        CreateGuardrailRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGuardrailAsync(request);
            return new CreateGuardrailResult(
                GuardrailId: resp.GuardrailId,
                GuardrailArn: resp.GuardrailArn);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock guardrail '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Bedrock guardrail.
    /// </summary>
    public static async Task DeleteGuardrailAsync(
        string guardrailIdentifier,
        string? guardrailVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteGuardrailRequest
        {
            GuardrailIdentifier = guardrailIdentifier
        };
        if (guardrailVersion != null) request.GuardrailVersion = guardrailVersion;

        try
        {
            await client.DeleteGuardrailAsync(request);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock guardrail '{guardrailIdentifier}'");
        }
    }

    /// <summary>
    /// Get information about a Bedrock guardrail.
    /// </summary>
    public static async Task<GetGuardrailResult> GetGuardrailAsync(
        string guardrailIdentifier,
        string? guardrailVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetGuardrailRequest
        {
            GuardrailIdentifier = guardrailIdentifier
        };
        if (guardrailVersion != null) request.GuardrailVersion = guardrailVersion;

        try
        {
            var resp = await client.GetGuardrailAsync(request);
            return new GetGuardrailResult(
                GuardrailId: resp.GuardrailId,
                GuardrailArn: resp.GuardrailArn,
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock guardrail '{guardrailIdentifier}'");
        }
    }

    /// <summary>
    /// List Bedrock guardrails, automatically paginating.
    /// </summary>
    public static async Task<ListGuardrailsResult> ListGuardrailsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var guardrails = new List<GuardrailSummary>();
        var request = new ListGuardrailsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListGuardrailsAsync(request);
                guardrails.AddRange(resp.Guardrails);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Bedrock guardrails");
        }

        return new ListGuardrailsResult(Guardrails: guardrails);
    }

    /// <summary>
    /// Update a Bedrock guardrail.
    /// </summary>
    public static async Task UpdateGuardrailAsync(
        UpdateGuardrailRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateGuardrailAsync(request);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock guardrail '{request.GuardrailIdentifier}'");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a Bedrock resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new Amazon.Bedrock.Model.TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags.Select(kv =>
                    new Amazon.Bedrock.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList()
            });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Bedrock resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Bedrock resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new Amazon.Bedrock.Model.UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Bedrock resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Bedrock resource.
    /// </summary>
    public static async Task<ListBedrockTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new Amazon.Bedrock.Model.ListTagsForResourceRequest
                {
                    ResourceARN = resourceArn
                });
            return new ListBedrockTagsResult(Tags: resp.Tags);
        }
        catch (AmazonBedrockException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Bedrock resource '{resourceArn}'");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Bedrock Runtime (Data Plane)
    // ══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Invoke a Bedrock model with a request body.
    /// </summary>
    public static async Task<InvokeModelResult> InvokeModelAsync(
        string modelId,
        string body,
        string? contentType = null,
        string? accept = null,
        RegionEndpoint? region = null)
    {
        var runtimeClient = GetRuntimeClient(region);
        var request = new InvokeModelRequest
        {
            ModelId = modelId,
            Body = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(body))
        };
        if (contentType != null) request.ContentType = contentType;
        if (accept != null) request.Accept = accept;

        try
        {
            var resp = await runtimeClient.InvokeModelAsync(request);
            using var reader = new StreamReader(resp.Body);
            var responseBody = await reader.ReadToEndAsync();
            return new InvokeModelResult(
                Body: responseBody,
                ContentType: resp.ContentType);
        }
        catch (AmazonBedrockRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke Bedrock model '{modelId}'");
        }
    }

    /// <summary>
    /// Invoke a Bedrock model with response streaming.
    /// Returns the response stream for the caller to consume.
    /// </summary>
    public static async Task<InvokeModelWithResponseStreamResponse>
        InvokeModelWithResponseStreamAsync(
            string modelId,
            string body,
            string? contentType = null,
            string? accept = null,
            RegionEndpoint? region = null)
    {
        var runtimeClient = GetRuntimeClient(region);
        var request = new InvokeModelWithResponseStreamRequest
        {
            ModelId = modelId,
            Body = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(body))
        };
        if (contentType != null) request.ContentType = contentType;
        if (accept != null) request.Accept = accept;

        try
        {
            return await runtimeClient.InvokeModelWithResponseStreamAsync(request);
        }
        catch (AmazonBedrockRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke Bedrock model with response stream '{modelId}'");
        }
    }

    /// <summary>
    /// Send a conversational request to a Bedrock model using the Converse API.
    /// </summary>
    public static async Task<ConverseResult> ConverseAsync(
        string modelId,
        List<Message> messages,
        List<SystemContentBlock>? system = null,
        InferenceConfiguration? inferenceConfig = null,
        ToolConfiguration? toolConfig = null,
        RegionEndpoint? region = null)
    {
        var runtimeClient = GetRuntimeClient(region);
        var request = new ConverseRequest
        {
            ModelId = modelId,
            Messages = messages
        };
        if (system != null) request.System = system;
        if (inferenceConfig != null) request.InferenceConfig = inferenceConfig;
        if (toolConfig != null) request.ToolConfig = toolConfig;

        try
        {
            var resp = await runtimeClient.ConverseAsync(request);
            return new ConverseResult(
                Output: resp.Output,
                StopReason: resp.StopReason,
                Usage: resp.Usage);
        }
        catch (AmazonBedrockRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to converse with Bedrock model '{modelId}'");
        }
    }

    /// <summary>
    /// Send a conversational streaming request to a Bedrock model.
    /// Returns the response for the caller to consume the stream.
    /// </summary>
    public static async Task<ConverseStreamResponse> ConverseStreamAsync(
        string modelId,
        List<Message> messages,
        List<SystemContentBlock>? system = null,
        InferenceConfiguration? inferenceConfig = null,
        ToolConfiguration? toolConfig = null,
        RegionEndpoint? region = null)
    {
        var runtimeClient = GetRuntimeClient(region);
        var request = new ConverseStreamRequest
        {
            ModelId = modelId,
            Messages = messages
        };
        if (system != null) request.System = system;
        if (inferenceConfig != null) request.InferenceConfig = inferenceConfig;
        if (toolConfig != null) request.ToolConfig = toolConfig;

        try
        {
            return await runtimeClient.ConverseStreamAsync(request);
        }
        catch (AmazonBedrockRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to converse stream with Bedrock model '{modelId}'");
        }
    }
}
