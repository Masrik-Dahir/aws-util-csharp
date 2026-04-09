using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of a Lambda Invoke call.</summary>
public sealed record InvokeResult(
    int StatusCode,
    object? Payload = null,
    string? FunctionError = null,
    string? LogResult = null)
{
    /// <summary><c>true</c> when the invocation completed without a function error.</summary>
    public bool Succeeded => FunctionError is null;
}

/// <summary>Result of invoke_with_response_stream.</summary>
public sealed record InvokeWithResponseStreamResult(
    int? StatusCode = null,
    string? ExecutedVersion = null,
    Dictionary<string, object?>? EventStream = null,
    string? ResponseStreamContentType = null);

/// <summary>Result of add_layer_version_permission.</summary>
public sealed record AddLayerVersionPermissionResult(
    string? Statement = null,
    string? RevisionId = null);

/// <summary>Result of add_permission.</summary>
public sealed record AddPermissionResult(string? Statement = null);

/// <summary>Result of create_alias / get_alias / update_alias.</summary>
public sealed record AliasResult(
    string? AliasArn = null,
    string? Name = null,
    string? FunctionVersion = null,
    string? Description = null,
    Dictionary<string, object?>? RoutingConfig = null,
    string? RevisionId = null);

/// <summary>Result of create_code_signing_config / get_code_signing_config / update_code_signing_config.</summary>
public sealed record CodeSigningConfigResult(
    Dictionary<string, object?>? CodeSigningConfig = null);

/// <summary>Result of create/delete/get/update event source mapping.</summary>
public sealed record EventSourceMappingResult(
    string? Uuid = null,
    string? StartingPosition = null,
    string? StartingPositionTimestamp = null,
    int? BatchSize = null,
    int? MaximumBatchingWindowInSeconds = null,
    int? ParallelizationFactor = null,
    string? EventSourceArn = null,
    Dictionary<string, object?>? FilterCriteria = null,
    string? FunctionArn = null,
    string? LastModified = null,
    string? LastProcessingResult = null,
    string? State = null,
    string? StateTransitionReason = null,
    Dictionary<string, object?>? DestinationConfig = null,
    List<string>? Topics = null,
    List<string>? Queues = null,
    List<Dictionary<string, object?>>? SourceAccessConfigurations = null,
    Dictionary<string, object?>? SelfManagedEventSource = null,
    int? MaximumRecordAgeInSeconds = null,
    bool? BisectBatchOnFunctionError = null,
    int? MaximumRetryAttempts = null,
    int? TumblingWindowInSeconds = null,
    List<string>? FunctionResponseTypes = null,
    Dictionary<string, object?>? AmazonManagedKafkaEventSourceConfig = null,
    Dictionary<string, object?>? SelfManagedKafkaEventSourceConfig = null,
    Dictionary<string, object?>? ScalingConfig = null,
    Dictionary<string, object?>? DocumentDbEventSourceConfig = null,
    string? KmsKeyArn = null,
    Dictionary<string, object?>? FilterCriteriaError = null,
    string? EventSourceMappingArn = null,
    Dictionary<string, object?>? MetricsConfig = null,
    Dictionary<string, object?>? ProvisionedPollerConfig = null);

/// <summary>Result of create_function / publish_version / update_function_code / update_function_configuration / get_function_configuration.</summary>
public sealed record FunctionConfigurationResult(
    string? FunctionName = null,
    string? FunctionArn = null,
    string? Runtime = null,
    string? Role = null,
    string? Handler = null,
    long? CodeSize = null,
    string? Description = null,
    int? Timeout = null,
    int? MemorySize = null,
    string? LastModified = null,
    string? CodeSha256 = null,
    string? Version = null,
    Dictionary<string, object?>? VpcConfig = null,
    Dictionary<string, object?>? DeadLetterConfig = null,
    Dictionary<string, object?>? Environment = null,
    string? KmsKeyArn = null,
    Dictionary<string, object?>? TracingConfig = null,
    string? MasterArn = null,
    string? RevisionId = null,
    List<Dictionary<string, object?>>? Layers = null,
    string? State = null,
    string? StateReason = null,
    string? StateReasonCode = null,
    string? LastUpdateStatus = null,
    string? LastUpdateStatusReason = null,
    string? LastUpdateStatusReasonCode = null,
    List<Dictionary<string, object?>>? FileSystemConfigs = null,
    string? PackageType = null,
    Dictionary<string, object?>? ImageConfigResponse = null,
    string? SigningProfileVersionArn = null,
    string? SigningJobArn = null,
    List<string>? Architectures = null,
    Dictionary<string, object?>? EphemeralStorage = null,
    Dictionary<string, object?>? SnapStart = null,
    Dictionary<string, object?>? RuntimeVersionConfig = null,
    Dictionary<string, object?>? LoggingConfig = null);

/// <summary>Result of create_function_url_config / get_function_url_config / update_function_url_config.</summary>
public sealed record FunctionUrlConfigResult(
    string? FunctionUrl = null,
    string? FunctionArn = null,
    string? AuthType = null,
    Dictionary<string, object?>? Cors = null,
    string? CreationTime = null,
    string? LastModifiedTime = null,
    string? InvokeMode = null);

/// <summary>Result of get_account_settings.</summary>
public sealed record GetAccountSettingsResult(
    Dictionary<string, object?>? AccountLimit = null,
    Dictionary<string, object?>? AccountUsage = null);

/// <summary>Result of get_function.</summary>
public sealed record GetFunctionResult(
    Dictionary<string, object?>? Configuration = null,
    Dictionary<string, object?>? Code = null,
    Dictionary<string, object?>? Tags = null,
    Dictionary<string, object?>? TagsError = null,
    Dictionary<string, object?>? Concurrency = null);

/// <summary>Result of get/put function_code_signing_config.</summary>
public sealed record FunctionCodeSigningConfigResult(
    string? CodeSigningConfigArn = null,
    string? FunctionName = null);

/// <summary>Result of get_function_concurrency / put_function_concurrency.</summary>
public sealed record FunctionConcurrencyResult(
    int? ReservedConcurrentExecutions = null);

/// <summary>Result of get/put function_event_invoke_config / update.</summary>
public sealed record FunctionEventInvokeConfigResult(
    string? LastModified = null,
    string? FunctionArn = null,
    int? MaximumRetryAttempts = null,
    int? MaximumEventAgeInSeconds = null,
    Dictionary<string, object?>? DestinationConfig = null);

/// <summary>Result of get/put function_recursion_config.</summary>
public sealed record FunctionRecursionConfigResult(
    string? RecursiveLoop = null);

/// <summary>Result of get_layer_version / get_layer_version_by_arn / publish_layer_version.</summary>
public sealed record LayerVersionResult(
    Dictionary<string, object?>? Content = null,
    string? LayerArn = null,
    string? LayerVersionArn = null,
    string? Description = null,
    string? CreatedDate = null,
    long? Version = null,
    List<string>? CompatibleRuntimes = null,
    string? LicenseInfo = null,
    List<string>? CompatibleArchitectures = null);

/// <summary>Result of get_layer_version_policy.</summary>
public sealed record GetLayerVersionPolicyResult(
    string? Policy = null,
    string? RevisionId = null);

/// <summary>Result of get_policy.</summary>
public sealed record GetPolicyResult(
    string? Policy = null,
    string? RevisionId = null);

/// <summary>Result of get/put provisioned_concurrency_config.</summary>
public sealed record ProvisionedConcurrencyConfigResult(
    int? RequestedProvisionedConcurrentExecutions = null,
    int? AvailableProvisionedConcurrentExecutions = null,
    int? AllocatedProvisionedConcurrentExecutions = null,
    string? Status = null,
    string? StatusReason = null,
    string? LastModified = null);

/// <summary>Result of get/put runtime_management_config.</summary>
public sealed record RuntimeManagementConfigResult(
    string? UpdateRuntimeOn = null,
    string? RuntimeVersionArn = null,
    string? FunctionArn = null);

/// <summary>Result of list_aliases.</summary>
public sealed record ListAliasesResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? Aliases = null);

/// <summary>Result of list_code_signing_configs.</summary>
public sealed record ListCodeSigningConfigsResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? CodeSigningConfigs = null);

/// <summary>Result of list_event_source_mappings.</summary>
public sealed record ListEventSourceMappingsResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? EventSourceMappings = null);

/// <summary>Result of list_function_event_invoke_configs.</summary>
public sealed record ListFunctionEventInvokeConfigsResult(
    List<Dictionary<string, object?>>? FunctionEventInvokeConfigs = null,
    string? NextMarker = null);

/// <summary>Result of list_function_url_configs.</summary>
public sealed record ListFunctionUrlConfigsResult(
    List<Dictionary<string, object?>>? FunctionUrlConfigs = null,
    string? NextMarker = null);

/// <summary>Result of list_functions.</summary>
public sealed record ListFunctionsResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? Functions = null);

/// <summary>Result of list_functions_by_code_signing_config.</summary>
public sealed record ListFunctionsByCodeSigningConfigResult(
    string? NextMarker = null,
    List<string>? FunctionArns = null);

/// <summary>Result of list_layer_versions.</summary>
public sealed record ListLayerVersionsResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? LayerVersions = null);

/// <summary>Result of list_layers.</summary>
public sealed record ListLayersResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? Layers = null);

/// <summary>Result of list_provisioned_concurrency_configs.</summary>
public sealed record ListProvisionedConcurrencyConfigsResult(
    List<Dictionary<string, object?>>? ProvisionedConcurrencyConfigs = null,
    string? NextMarker = null);

/// <summary>Result of list_tags.</summary>
public sealed record ListTagsResult(
    Dictionary<string, string>? Tags = null);

/// <summary>Result of list_versions_by_function.</summary>
public sealed record ListVersionsByFunctionResult(
    string? NextMarker = null,
    List<Dictionary<string, object?>>? Versions = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for AWS Lambda.
/// </summary>
public static class LambdaService
{
    private static AmazonLambdaClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLambdaClient>(region);

    // -----------------------------------------------------------------------
    // Invoke
    // -----------------------------------------------------------------------

    /// <summary>
    /// Invoke an AWS Lambda function.
    /// </summary>
    /// <param name="functionName">Function name, ARN, or partial ARN.</param>
    /// <param name="payload">Event payload sent to the function. Objects are JSON-serialized; null sends an empty payload.</param>
    /// <param name="invocationType">"RequestResponse" (sync), "Event" (async), or "DryRun" (validate only).</param>
    /// <param name="logType">"Tail" returns the last 4 KB of execution logs.</param>
    /// <param name="qualifier">Function version or alias to invoke.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<InvokeResult> InvokeAsync(
        string functionName,
        object? payload = null,
        string invocationType = "RequestResponse",
        string logType = "None",
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InvokeRequest
        {
            FunctionName = functionName,
            InvocationType = invocationType,
            LogType = logType
        };

        if (payload is not null)
        {
            var json = payload is string s ? s : JsonSerializer.Serialize(payload);
            request.Payload = json;
        }

        if (qualifier is not null)
            request.Qualifier = qualifier;

        try
        {
            var resp = await client.InvokeAsync(request);

            object? parsedPayload = null;
            if (resp.Payload is { Length: > 0 })
            {
                using var reader = new StreamReader(resp.Payload, Encoding.UTF8);
                var raw = await reader.ReadToEndAsync();
                try
                {
                    parsedPayload = JsonSerializer.Deserialize<object>(raw);
                }
                catch (JsonException)
                {
                    parsedPayload = raw;
                }
            }

            string? logResult = null;
            if (!string.IsNullOrEmpty(resp.LogResult))
                logResult = Encoding.UTF8.GetString(Convert.FromBase64String(resp.LogResult));

            return new InvokeResult(
                StatusCode: resp.StatusCode ?? 0,
                Payload: parsedPayload,
                FunctionError: string.IsNullOrEmpty(resp.FunctionError) ? null : resp.FunctionError,
                LogResult: logResult);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to invoke Lambda '{functionName}'");
        }
    }

    /// <summary>
    /// Fire-and-forget Lambda invocation (Event invocation type).
    /// </summary>
    public static async Task InvokeEventAsync(
        string functionName,
        object? payload = null,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        await InvokeAsync(
            functionName,
            payload: payload,
            invocationType: "Event",
            qualifier: qualifier,
            region: region);
    }

    /// <summary>
    /// Invoke with response stream.
    /// </summary>
    public static async Task<InvokeWithResponseStreamResult> InvokeWithResponseStreamAsync(
        string functionName,
        string? invocationType = null,
        string? logType = null,
        string? clientContext = null,
        string? qualifier = null,
        byte[]? payload = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InvokeWithResponseStreamRequest { FunctionName = functionName };
        if (invocationType is not null) request.InvocationType = invocationType;
        if (logType is not null) request.LogType = logType;
        if (clientContext is not null) request.ClientContext = clientContext;
        if (qualifier is not null) request.Qualifier = qualifier;
        if (payload is not null) request.Payload = new MemoryStream(payload);

        try
        {
            var resp = await client.InvokeWithResponseStreamAsync(request);
            return new InvokeWithResponseStreamResult(
                StatusCode: resp.StatusCode,
                ExecutedVersion: resp.ExecutedVersion,
                ResponseStreamContentType: resp.ResponseStreamContentType);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to invoke with response stream");
        }
    }

    // -----------------------------------------------------------------------
    // Function CRUD
    // -----------------------------------------------------------------------

    /// <summary>Create a Lambda function.</summary>
    public static async Task<FunctionConfigurationResult> CreateFunctionAsync(
        string functionName,
        string role,
        FunctionCode code,
        string? runtime = null,
        string? handler = null,
        string? description = null,
        int? timeout = null,
        int? memorySize = null,
        bool? publish = null,
        VpcConfig? vpcConfig = null,
        string? packageType = null,
        DeadLetterConfig? deadLetterConfig = null,
        Amazon.Lambda.Model.Environment? environment = null,
        string? kmsKeyArn = null,
        TracingConfig? tracingConfig = null,
        Dictionary<string, string>? tags = null,
        List<string>? layers = null,
        List<FileSystemConfig>? fileSystemConfigs = null,
        ImageConfig? imageConfig = null,
        string? codeSigningConfigArn = null,
        List<string>? architectures = null,
        EphemeralStorage? ephemeralStorage = null,
        SnapStart? snapStart = null,
        LoggingConfig? loggingConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFunctionRequest
        {
            FunctionName = functionName,
            Role = role,
            Code = code
        };
        if (runtime is not null) request.Runtime = runtime;
        if (handler is not null) request.Handler = handler;
        if (description is not null) request.Description = description;
        if (timeout.HasValue) request.Timeout = timeout.Value;
        if (memorySize.HasValue) request.MemorySize = memorySize.Value;
        if (publish.HasValue) request.Publish = publish.Value;
        if (vpcConfig is not null) request.VpcConfig = vpcConfig;
        if (packageType is not null) request.PackageType = packageType;
        if (deadLetterConfig is not null) request.DeadLetterConfig = deadLetterConfig;
        if (environment is not null) request.Environment = environment;
        if (kmsKeyArn is not null) request.KMSKeyArn = kmsKeyArn;
        if (tracingConfig is not null) request.TracingConfig = tracingConfig;
        if (tags is not null) request.Tags = tags;
        if (layers is not null) request.Layers = layers;
        if (fileSystemConfigs is not null) request.FileSystemConfigs = fileSystemConfigs;
        if (imageConfig is not null) request.ImageConfig = imageConfig;
        if (codeSigningConfigArn is not null) request.CodeSigningConfigArn = codeSigningConfigArn;
        if (architectures is not null) request.Architectures = architectures;
        if (ephemeralStorage is not null) request.EphemeralStorage = ephemeralStorage;
        if (snapStart is not null) request.SnapStart = snapStart;
        if (loggingConfig is not null) request.LoggingConfig = loggingConfig;

        try
        {
            var resp = await client.CreateFunctionAsync(request);
            return MapFunctionConfiguration(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create function");
        }
    }

    /// <summary>Delete a Lambda function.</summary>
    public static async Task DeleteFunctionAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteFunctionRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            await client.DeleteFunctionAsync(request);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete function");
        }
    }

    /// <summary>Get function details.</summary>
    public static async Task<GetFunctionResult> GetFunctionAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFunctionRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetFunctionAsync(request);
            return new GetFunctionResult(
                Configuration: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.Configuration)),
                Code: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.Code)),
                Tags: resp.Tags?.ToDictionary(kv => kv.Key, kv => (object?)kv.Value),
                Concurrency: resp.Concurrency is not null
                    ? new Dictionary<string, object?>
                    {
                        ["ReservedConcurrentExecutions"] = resp.Concurrency.ReservedConcurrentExecutions
                    }
                    : null);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function");
        }
    }

    /// <summary>Get function configuration.</summary>
    public static async Task<FunctionConfigurationResult> GetFunctionConfigurationAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFunctionConfigurationRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetFunctionConfigurationAsync(request);
            return MapFunctionConfiguration(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function configuration");
        }
    }

    /// <summary>Update function code.</summary>
    public static async Task<FunctionConfigurationResult> UpdateFunctionCodeAsync(
        string functionName,
        MemoryStream? zipFile = null,
        string? s3Bucket = null,
        string? s3Key = null,
        string? s3ObjectVersion = null,
        string? imageUri = null,
        bool? publish = null,
        string? revisionId = null,
        List<string>? architectures = null,
        string? sourceKmsKeyArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFunctionCodeRequest { FunctionName = functionName };
        if (zipFile is not null) request.ZipFile = zipFile;
        if (s3Bucket is not null) request.S3Bucket = s3Bucket;
        if (s3Key is not null) request.S3Key = s3Key;
        if (s3ObjectVersion is not null) request.S3ObjectVersion = s3ObjectVersion;
        if (imageUri is not null) request.ImageUri = imageUri;
        if (publish.HasValue) request.Publish = publish.Value;
        if (revisionId is not null) request.RevisionId = revisionId;
        if (architectures is not null) request.Architectures = architectures;
        if (sourceKmsKeyArn is not null) request.SourceKMSKeyArn = sourceKmsKeyArn;

        try
        {
            var resp = await client.UpdateFunctionCodeAsync(request);
            return MapFunctionConfiguration(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update function code");
        }
    }

    /// <summary>Update function configuration.</summary>
    public static async Task<FunctionConfigurationResult> UpdateFunctionConfigurationAsync(
        string functionName,
        string? role = null,
        string? handler = null,
        string? description = null,
        int? timeout = null,
        int? memorySize = null,
        VpcConfig? vpcConfig = null,
        Amazon.Lambda.Model.Environment? environment = null,
        string? runtime = null,
        DeadLetterConfig? deadLetterConfig = null,
        string? kmsKeyArn = null,
        TracingConfig? tracingConfig = null,
        string? revisionId = null,
        List<string>? layers = null,
        List<FileSystemConfig>? fileSystemConfigs = null,
        ImageConfig? imageConfig = null,
        EphemeralStorage? ephemeralStorage = null,
        SnapStart? snapStart = null,
        LoggingConfig? loggingConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFunctionConfigurationRequest { FunctionName = functionName };
        if (role is not null) request.Role = role;
        if (handler is not null) request.Handler = handler;
        if (description is not null) request.Description = description;
        if (timeout.HasValue) request.Timeout = timeout.Value;
        if (memorySize.HasValue) request.MemorySize = memorySize.Value;
        if (vpcConfig is not null) request.VpcConfig = vpcConfig;
        if (environment is not null) request.Environment = environment;
        if (runtime is not null) request.Runtime = runtime;
        if (deadLetterConfig is not null) request.DeadLetterConfig = deadLetterConfig;
        if (kmsKeyArn is not null) request.KMSKeyArn = kmsKeyArn;
        if (tracingConfig is not null) request.TracingConfig = tracingConfig;
        if (revisionId is not null) request.RevisionId = revisionId;
        if (layers is not null) request.Layers = layers;
        if (fileSystemConfigs is not null) request.FileSystemConfigs = fileSystemConfigs;
        if (imageConfig is not null) request.ImageConfig = imageConfig;
        if (ephemeralStorage is not null) request.EphemeralStorage = ephemeralStorage;
        if (snapStart is not null) request.SnapStart = snapStart;
        if (loggingConfig is not null) request.LoggingConfig = loggingConfig;

        try
        {
            var resp = await client.UpdateFunctionConfigurationAsync(request);
            return MapFunctionConfiguration(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update function configuration");
        }
    }

    /// <summary>List Lambda functions.</summary>
    public static async Task<ListFunctionsResult> ListFunctionsAsync(
        string? masterRegion = null,
        string? functionVersion = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFunctionsRequest();
        if (masterRegion is not null) request.MasterRegion = masterRegion;
        if (functionVersion is not null) request.FunctionVersion = functionVersion;
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListFunctionsAsync(request);
            return new ListFunctionsResult(
                NextMarker: resp.NextMarker,
                Functions: resp.Functions?.Select(f =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(f))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list functions");
        }
    }

    // -----------------------------------------------------------------------
    // Versions
    // -----------------------------------------------------------------------

    /// <summary>Publish a version.</summary>
    public static async Task<FunctionConfigurationResult> PublishVersionAsync(
        string functionName,
        string? codeSha256 = null,
        string? description = null,
        string? revisionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishVersionRequest { FunctionName = functionName };
        if (codeSha256 is not null) request.CodeSha256 = codeSha256;
        if (description is not null) request.Description = description;
        if (revisionId is not null) request.RevisionId = revisionId;

        try
        {
            var resp = await client.PublishVersionAsync(request);
            return MapFunctionConfiguration(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to publish version");
        }
    }

    /// <summary>List versions by function.</summary>
    public static async Task<ListVersionsByFunctionResult> ListVersionsByFunctionAsync(
        string functionName,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVersionsByFunctionRequest { FunctionName = functionName };
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListVersionsByFunctionAsync(request);
            return new ListVersionsByFunctionResult(
                NextMarker: resp.NextMarker,
                Versions: resp.Versions?.Select(v =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(v))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list versions by function");
        }
    }

    // -----------------------------------------------------------------------
    // Aliases
    // -----------------------------------------------------------------------

    /// <summary>Create an alias.</summary>
    public static async Task<AliasResult> CreateAliasAsync(
        string functionName,
        string name,
        string functionVersion,
        string? description = null,
        AliasRoutingConfiguration? routingConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAliasRequest
        {
            FunctionName = functionName,
            Name = name,
            FunctionVersion = functionVersion
        };
        if (description is not null) request.Description = description;
        if (routingConfig is not null) request.RoutingConfig = routingConfig;

        try
        {
            var resp = await client.CreateAliasAsync(request);
            return MapAlias(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create alias");
        }
    }

    /// <summary>Update an alias.</summary>
    public static async Task<AliasResult> UpdateAliasAsync(
        string functionName,
        string name,
        string? functionVersion = null,
        string? description = null,
        AliasRoutingConfiguration? routingConfig = null,
        string? revisionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAliasRequest
        {
            FunctionName = functionName,
            Name = name
        };
        if (functionVersion is not null) request.FunctionVersion = functionVersion;
        if (description is not null) request.Description = description;
        if (routingConfig is not null) request.RoutingConfig = routingConfig;
        if (revisionId is not null) request.RevisionId = revisionId;

        try
        {
            var resp = await client.UpdateAliasAsync(request);
            return MapAlias(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update alias");
        }
    }

    /// <summary>Delete an alias.</summary>
    public static async Task DeleteAliasAsync(
        string functionName,
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAliasAsync(new DeleteAliasRequest
            {
                FunctionName = functionName,
                Name = name
            });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete alias");
        }
    }

    /// <summary>Get an alias.</summary>
    public static async Task<AliasResult> GetAliasAsync(
        string functionName,
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAliasAsync(new GetAliasRequest
            {
                FunctionName = functionName,
                Name = name
            });
            return MapAlias(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get alias");
        }
    }

    /// <summary>List aliases.</summary>
    public static async Task<ListAliasesResult> ListAliasesAsync(
        string functionName,
        string? functionVersion = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAliasesRequest { FunctionName = functionName };
        if (functionVersion is not null) request.FunctionVersion = functionVersion;
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListAliasesAsync(request);
            return new ListAliasesResult(
                NextMarker: resp.NextMarker,
                Aliases: resp.Aliases?.Select(a =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(a))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list aliases");
        }
    }

    // -----------------------------------------------------------------------
    // Permissions / Resource Policy
    // -----------------------------------------------------------------------

    /// <summary>Add a resource-based policy statement.</summary>
    public static async Task<AddPermissionResult> AddPermissionAsync(
        string functionName,
        string statementId,
        string action,
        string principal,
        string? sourceArn = null,
        string? sourceAccount = null,
        string? eventSourceToken = null,
        string? qualifier = null,
        string? revisionId = null,
        string? principalOrgId = null,
        string? functionUrlAuthType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AddPermissionRequest
        {
            FunctionName = functionName,
            StatementId = statementId,
            Action = action,
            Principal = principal
        };
        if (sourceArn is not null) request.SourceArn = sourceArn;
        if (sourceAccount is not null) request.SourceAccount = sourceAccount;
        if (eventSourceToken is not null) request.EventSourceToken = eventSourceToken;
        if (qualifier is not null) request.Qualifier = qualifier;
        if (revisionId is not null) request.RevisionId = revisionId;
        if (principalOrgId is not null) request.PrincipalOrgID = principalOrgId;
        if (functionUrlAuthType is not null) request.FunctionUrlAuthType = functionUrlAuthType;

        try
        {
            var resp = await client.AddPermissionAsync(request);
            return new AddPermissionResult(Statement: resp.Statement);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add permission");
        }
    }

    /// <summary>Remove a resource-based policy statement.</summary>
    public static async Task RemovePermissionAsync(
        string functionName,
        string statementId,
        string? qualifier = null,
        string? revisionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemovePermissionRequest
        {
            FunctionName = functionName,
            StatementId = statementId
        };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (revisionId is not null) request.RevisionId = revisionId;

        try
        {
            await client.RemovePermissionAsync(request);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove permission");
        }
    }

    /// <summary>Get the resource-based policy.</summary>
    public static async Task<GetPolicyResult> GetPolicyAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPolicyRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetPolicyAsync(request);
            return new GetPolicyResult(
                Policy: resp.Policy,
                RevisionId: resp.RevisionId);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get policy");
        }
    }

    // -----------------------------------------------------------------------
    // Event Source Mappings
    // -----------------------------------------------------------------------

    /// <summary>Create an event source mapping.</summary>
    public static async Task<EventSourceMappingResult> CreateEventSourceMappingAsync(
        string functionName,
        string? eventSourceArn = null,
        bool? enabled = null,
        int? batchSize = null,
        FilterCriteria? filterCriteria = null,
        int? maximumBatchingWindowInSeconds = null,
        int? parallelizationFactor = null,
        string? startingPosition = null,
        DateTime? startingPositionTimestamp = null,
        DestinationConfig? destinationConfig = null,
        int? maximumRecordAgeInSeconds = null,
        bool? bisectBatchOnFunctionError = null,
        int? maximumRetryAttempts = null,
        Dictionary<string, string>? tags = null,
        int? tumblingWindowInSeconds = null,
        List<string>? topics = null,
        List<string>? queues = null,
        List<SourceAccessConfiguration>? sourceAccessConfigurations = null,
        SelfManagedEventSource? selfManagedEventSource = null,
        List<string>? functionResponseTypes = null,
        AmazonManagedKafkaEventSourceConfig? amazonManagedKafkaEventSourceConfig = null,
        SelfManagedKafkaEventSourceConfig? selfManagedKafkaEventSourceConfig = null,
        ScalingConfig? scalingConfig = null,
        DocumentDBEventSourceConfig? documentDbEventSourceConfig = null,
        string? kmsKeyArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateEventSourceMappingRequest { FunctionName = functionName };
        if (eventSourceArn is not null) request.EventSourceArn = eventSourceArn;
        if (enabled.HasValue) request.Enabled = enabled.Value;
        if (batchSize.HasValue) request.BatchSize = batchSize.Value;
        if (filterCriteria is not null) request.FilterCriteria = filterCriteria;
        if (maximumBatchingWindowInSeconds.HasValue) request.MaximumBatchingWindowInSeconds = maximumBatchingWindowInSeconds.Value;
        if (parallelizationFactor.HasValue) request.ParallelizationFactor = parallelizationFactor.Value;
        if (startingPosition is not null) request.StartingPosition = startingPosition;
        if (startingPositionTimestamp.HasValue) request.StartingPositionTimestamp = startingPositionTimestamp.Value;
        if (destinationConfig is not null) request.DestinationConfig = destinationConfig;
        if (maximumRecordAgeInSeconds.HasValue) request.MaximumRecordAgeInSeconds = maximumRecordAgeInSeconds.Value;
        if (bisectBatchOnFunctionError.HasValue) request.BisectBatchOnFunctionError = bisectBatchOnFunctionError.Value;
        if (maximumRetryAttempts.HasValue) request.MaximumRetryAttempts = maximumRetryAttempts.Value;
        if (tags is not null) request.Tags = tags;
        if (tumblingWindowInSeconds.HasValue) request.TumblingWindowInSeconds = tumblingWindowInSeconds.Value;
        if (topics is not null) request.Topics = topics;
        if (queues is not null) request.Queues = queues;
        if (sourceAccessConfigurations is not null) request.SourceAccessConfigurations = sourceAccessConfigurations;
        if (selfManagedEventSource is not null) request.SelfManagedEventSource = selfManagedEventSource;
        if (functionResponseTypes is not null) request.FunctionResponseTypes = functionResponseTypes;
        if (amazonManagedKafkaEventSourceConfig is not null) request.AmazonManagedKafkaEventSourceConfig = amazonManagedKafkaEventSourceConfig;
        if (selfManagedKafkaEventSourceConfig is not null) request.SelfManagedKafkaEventSourceConfig = selfManagedKafkaEventSourceConfig;
        if (scalingConfig is not null) request.ScalingConfig = scalingConfig;
        if (documentDbEventSourceConfig is not null) request.DocumentDBEventSourceConfig = documentDbEventSourceConfig;
        if (kmsKeyArn is not null) request.KMSKeyArn = kmsKeyArn;

        try
        {
            var resp = await client.CreateEventSourceMappingAsync(request);
            return MapEventSourceMapping(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create event source mapping");
        }
    }

    /// <summary>Delete an event source mapping.</summary>
    public static async Task<EventSourceMappingResult> DeleteEventSourceMappingAsync(
        string uuid,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteEventSourceMappingAsync(
                new DeleteEventSourceMappingRequest { UUID = uuid });
            return MapEventSourceMapping(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete event source mapping");
        }
    }

    /// <summary>Get an event source mapping.</summary>
    public static async Task<EventSourceMappingResult> GetEventSourceMappingAsync(
        string uuid,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEventSourceMappingAsync(
                new GetEventSourceMappingRequest { UUID = uuid });
            return MapEventSourceMapping(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get event source mapping");
        }
    }

    /// <summary>List event source mappings.</summary>
    public static async Task<ListEventSourceMappingsResult> ListEventSourceMappingsAsync(
        string? eventSourceArn = null,
        string? functionName = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEventSourceMappingsRequest();
        if (eventSourceArn is not null) request.EventSourceArn = eventSourceArn;
        if (functionName is not null) request.FunctionName = functionName;
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListEventSourceMappingsAsync(request);
            return new ListEventSourceMappingsResult(
                NextMarker: resp.NextMarker,
                EventSourceMappings: resp.EventSourceMappings?.Select(e =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(e))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list event source mappings");
        }
    }

    /// <summary>Update an event source mapping.</summary>
    public static async Task<EventSourceMappingResult> UpdateEventSourceMappingAsync(
        string uuid,
        string? functionName = null,
        bool? enabled = null,
        int? batchSize = null,
        FilterCriteria? filterCriteria = null,
        int? maximumBatchingWindowInSeconds = null,
        DestinationConfig? destinationConfig = null,
        int? maximumRecordAgeInSeconds = null,
        bool? bisectBatchOnFunctionError = null,
        int? maximumRetryAttempts = null,
        int? parallelizationFactor = null,
        List<SourceAccessConfiguration>? sourceAccessConfigurations = null,
        int? tumblingWindowInSeconds = null,
        List<string>? functionResponseTypes = null,
        ScalingConfig? scalingConfig = null,
        DocumentDBEventSourceConfig? documentDbEventSourceConfig = null,
        string? kmsKeyArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateEventSourceMappingRequest { UUID = uuid };
        if (functionName is not null) request.FunctionName = functionName;
        if (enabled.HasValue) request.Enabled = enabled.Value;
        if (batchSize.HasValue) request.BatchSize = batchSize.Value;
        if (filterCriteria is not null) request.FilterCriteria = filterCriteria;
        if (maximumBatchingWindowInSeconds.HasValue) request.MaximumBatchingWindowInSeconds = maximumBatchingWindowInSeconds.Value;
        if (destinationConfig is not null) request.DestinationConfig = destinationConfig;
        if (maximumRecordAgeInSeconds.HasValue) request.MaximumRecordAgeInSeconds = maximumRecordAgeInSeconds.Value;
        if (bisectBatchOnFunctionError.HasValue) request.BisectBatchOnFunctionError = bisectBatchOnFunctionError.Value;
        if (maximumRetryAttempts.HasValue) request.MaximumRetryAttempts = maximumRetryAttempts.Value;
        if (parallelizationFactor.HasValue) request.ParallelizationFactor = parallelizationFactor.Value;
        if (sourceAccessConfigurations is not null) request.SourceAccessConfigurations = sourceAccessConfigurations;
        if (tumblingWindowInSeconds.HasValue) request.TumblingWindowInSeconds = tumblingWindowInSeconds.Value;
        if (functionResponseTypes is not null) request.FunctionResponseTypes = functionResponseTypes;
        if (scalingConfig is not null) request.ScalingConfig = scalingConfig;
        if (documentDbEventSourceConfig is not null) request.DocumentDBEventSourceConfig = documentDbEventSourceConfig;
        if (kmsKeyArn is not null) request.KMSKeyArn = kmsKeyArn;

        try
        {
            var resp = await client.UpdateEventSourceMappingAsync(request);
            return MapEventSourceMapping(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update event source mapping");
        }
    }

    // -----------------------------------------------------------------------
    // Function URL Config
    // -----------------------------------------------------------------------

    /// <summary>Create a function URL config.</summary>
    public static async Task<FunctionUrlConfigResult> CreateFunctionUrlConfigAsync(
        string functionName,
        string authType,
        string? qualifier = null,
        Cors? cors = null,
        string? invokeMode = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFunctionUrlConfigRequest
        {
            FunctionName = functionName,
            AuthType = authType
        };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (cors is not null) request.Cors = cors;
        if (invokeMode is not null) request.InvokeMode = invokeMode;

        try
        {
            var resp = await client.CreateFunctionUrlConfigAsync(request);
            return new FunctionUrlConfigResult(
                FunctionUrl: resp.FunctionUrl,
                FunctionArn: resp.FunctionArn,
                AuthType: resp.AuthType?.Value,
                CreationTime: resp.CreationTime,
                InvokeMode: resp.InvokeMode?.Value);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create function url config");
        }
    }

    /// <summary>Get a function URL config.</summary>
    public static async Task<FunctionUrlConfigResult> GetFunctionUrlConfigAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFunctionUrlConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetFunctionUrlConfigAsync(request);
            return new FunctionUrlConfigResult(
                FunctionUrl: resp.FunctionUrl,
                FunctionArn: resp.FunctionArn,
                AuthType: resp.AuthType?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime,
                InvokeMode: resp.InvokeMode?.Value);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function url config");
        }
    }

    /// <summary>Update a function URL config.</summary>
    public static async Task<FunctionUrlConfigResult> UpdateFunctionUrlConfigAsync(
        string functionName,
        string? qualifier = null,
        string? authType = null,
        Cors? cors = null,
        string? invokeMode = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFunctionUrlConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (authType is not null) request.AuthType = authType;
        if (cors is not null) request.Cors = cors;
        if (invokeMode is not null) request.InvokeMode = invokeMode;

        try
        {
            var resp = await client.UpdateFunctionUrlConfigAsync(request);
            return new FunctionUrlConfigResult(
                FunctionUrl: resp.FunctionUrl,
                FunctionArn: resp.FunctionArn,
                AuthType: resp.AuthType?.Value,
                CreationTime: resp.CreationTime,
                LastModifiedTime: resp.LastModifiedTime,
                InvokeMode: resp.InvokeMode?.Value);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update function url config");
        }
    }

    /// <summary>Delete a function URL config.</summary>
    public static async Task DeleteFunctionUrlConfigAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteFunctionUrlConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            await client.DeleteFunctionUrlConfigAsync(request);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete function url config");
        }
    }

    /// <summary>List function URL configs.</summary>
    public static async Task<ListFunctionUrlConfigsResult> ListFunctionUrlConfigsAsync(
        string functionName,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFunctionUrlConfigsRequest { FunctionName = functionName };
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListFunctionUrlConfigsAsync(request);
            return new ListFunctionUrlConfigsResult(
                FunctionUrlConfigs: resp.FunctionUrlConfigs?.Select(c =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(c))).ToList(),
                NextMarker: resp.NextMarker);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list function url configs");
        }
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    /// <summary>Put function concurrency (reserved).</summary>
    public static async Task<FunctionConcurrencyResult> PutFunctionConcurrencyAsync(
        string functionName,
        int reservedConcurrentExecutions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutFunctionConcurrencyAsync(
                new PutFunctionConcurrencyRequest
                {
                    FunctionName = functionName,
                    ReservedConcurrentExecutions = reservedConcurrentExecutions
                });
            return new FunctionConcurrencyResult(
                ReservedConcurrentExecutions: resp.ReservedConcurrentExecutions);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put function concurrency");
        }
    }

    /// <summary>Get function concurrency.</summary>
    public static async Task<FunctionConcurrencyResult> GetFunctionConcurrencyAsync(
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFunctionConcurrencyAsync(
                new GetFunctionConcurrencyRequest { FunctionName = functionName });
            return new FunctionConcurrencyResult(
                ReservedConcurrentExecutions: resp.ReservedConcurrentExecutions);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function concurrency");
        }
    }

    /// <summary>Delete function concurrency.</summary>
    public static async Task DeleteFunctionConcurrencyAsync(
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFunctionConcurrencyAsync(
                new DeleteFunctionConcurrencyRequest { FunctionName = functionName });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete function concurrency");
        }
    }

    // -----------------------------------------------------------------------
    // Tags
    // -----------------------------------------------------------------------

    /// <summary>Tag a Lambda resource.</summary>
    public static async Task TagResourceAsync(
        string resource,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                Resource = resource,
                Tags = tags
            });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resource");
        }
    }

    /// <summary>Untag a Lambda resource.</summary>
    public static async Task UntagResourceAsync(
        string resource,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                Resource = resource,
                TagKeys = tagKeys
            });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag resource");
        }
    }

    /// <summary>List tags for a Lambda resource.</summary>
    public static async Task<ListTagsResult> ListTagsAsync(
        string resource,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsAsync(new ListTagsRequest { Resource = resource });
            return new ListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags");
        }
    }

    // -----------------------------------------------------------------------
    // Layers
    // -----------------------------------------------------------------------

    /// <summary>Publish a layer version.</summary>
    public static async Task<LayerVersionResult> PublishLayerVersionAsync(
        string layerName,
        LayerVersionContentInput content,
        string? description = null,
        List<string>? compatibleRuntimes = null,
        string? licenseInfo = null,
        List<string>? compatibleArchitectures = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishLayerVersionRequest
        {
            LayerName = layerName,
            Content = content
        };
        if (description is not null) request.Description = description;
        if (compatibleRuntimes is not null) request.CompatibleRuntimes = compatibleRuntimes;
        if (licenseInfo is not null) request.LicenseInfo = licenseInfo;
        if (compatibleArchitectures is not null) request.CompatibleArchitectures = compatibleArchitectures;

        try
        {
            var resp = await client.PublishLayerVersionAsync(request);
            return new LayerVersionResult(
                LayerArn: resp.LayerArn,
                LayerVersionArn: resp.LayerVersionArn,
                Description: resp.Description,
                CreatedDate: resp.CreatedDate,
                Version: resp.Version,
                CompatibleRuntimes: resp.CompatibleRuntimes?.ToList(),
                LicenseInfo: resp.LicenseInfo,
                CompatibleArchitectures: resp.CompatibleArchitectures?.ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to publish layer version");
        }
    }

    /// <summary>Get a layer version.</summary>
    public static async Task<LayerVersionResult> GetLayerVersionAsync(
        string layerName,
        long versionNumber,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetLayerVersionAsync(new GetLayerVersionRequest
            {
                LayerName = layerName,
                VersionNumber = versionNumber
            });
            return new LayerVersionResult(
                LayerArn: resp.LayerArn,
                LayerVersionArn: resp.LayerVersionArn,
                Description: resp.Description,
                CreatedDate: resp.CreatedDate,
                Version: resp.Version,
                CompatibleRuntimes: resp.CompatibleRuntimes?.ToList(),
                LicenseInfo: resp.LicenseInfo,
                CompatibleArchitectures: resp.CompatibleArchitectures?.ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get layer version");
        }
    }

    /// <summary>Delete a layer version.</summary>
    public static async Task DeleteLayerVersionAsync(
        string layerName,
        long versionNumber,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteLayerVersionAsync(new DeleteLayerVersionRequest
            {
                LayerName = layerName,
                VersionNumber = versionNumber
            });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete layer version");
        }
    }

    /// <summary>List layer versions.</summary>
    public static async Task<ListLayerVersionsResult> ListLayerVersionsAsync(
        string layerName,
        string? compatibleRuntime = null,
        string? marker = null,
        int? maxItems = null,
        string? compatibleArchitecture = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListLayerVersionsRequest { LayerName = layerName };
        if (compatibleRuntime is not null) request.CompatibleRuntime = compatibleRuntime;
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;
        if (compatibleArchitecture is not null) request.CompatibleArchitecture = compatibleArchitecture;

        try
        {
            var resp = await client.ListLayerVersionsAsync(request);
            return new ListLayerVersionsResult(
                NextMarker: resp.NextMarker,
                LayerVersions: resp.LayerVersions?.Select(lv =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(lv))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list layer versions");
        }
    }

    /// <summary>List layers.</summary>
    public static async Task<ListLayersResult> ListLayersAsync(
        string? compatibleRuntime = null,
        string? marker = null,
        int? maxItems = null,
        string? compatibleArchitecture = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListLayersRequest();
        if (compatibleRuntime is not null) request.CompatibleRuntime = compatibleRuntime;
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;
        if (compatibleArchitecture is not null) request.CompatibleArchitecture = compatibleArchitecture;

        try
        {
            var resp = await client.ListLayersAsync(request);
            return new ListLayersResult(
                NextMarker: resp.NextMarker,
                Layers: resp.Layers?.Select(l =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(l))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list layers");
        }
    }

    // -----------------------------------------------------------------------
    // Layer Version Permissions
    // -----------------------------------------------------------------------

    /// <summary>Add a layer version permission.</summary>
    public static async Task<AddLayerVersionPermissionResult> AddLayerVersionPermissionAsync(
        string layerName,
        long versionNumber,
        string statementId,
        string action,
        string principal,
        string? organizationId = null,
        string? revisionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AddLayerVersionPermissionRequest
        {
            LayerName = layerName,
            VersionNumber = versionNumber,
            StatementId = statementId,
            Action = action,
            Principal = principal
        };
        if (organizationId is not null) request.OrganizationId = organizationId;
        if (revisionId is not null) request.RevisionId = revisionId;

        try
        {
            var resp = await client.AddLayerVersionPermissionAsync(request);
            return new AddLayerVersionPermissionResult(
                Statement: resp.Statement,
                RevisionId: resp.RevisionId);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add layer version permission");
        }
    }

    /// <summary>Remove a layer version permission.</summary>
    public static async Task RemoveLayerVersionPermissionAsync(
        string layerName,
        long versionNumber,
        string statementId,
        string? revisionId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemoveLayerVersionPermissionRequest
        {
            LayerName = layerName,
            VersionNumber = versionNumber,
            StatementId = statementId
        };
        if (revisionId is not null) request.RevisionId = revisionId;

        try
        {
            await client.RemoveLayerVersionPermissionAsync(request);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove layer version permission");
        }
    }

    /// <summary>Get a layer version policy.</summary>
    public static async Task<GetLayerVersionPolicyResult> GetLayerVersionPolicyAsync(
        string layerName,
        long versionNumber,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetLayerVersionPolicyAsync(new GetLayerVersionPolicyRequest
            {
                LayerName = layerName,
                VersionNumber = versionNumber
            });
            return new GetLayerVersionPolicyResult(
                Policy: resp.Policy,
                RevisionId: resp.RevisionId);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get layer version policy");
        }
    }

    // -----------------------------------------------------------------------
    // Code Signing Config
    // -----------------------------------------------------------------------

    /// <summary>Create a code signing config.</summary>
    public static async Task<CodeSigningConfigResult> CreateCodeSigningConfigAsync(
        AllowedPublishers allowedPublishers,
        string? description = null,
        CodeSigningPolicies? codeSigningPolicies = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCodeSigningConfigRequest { AllowedPublishers = allowedPublishers };
        if (description is not null) request.Description = description;
        if (codeSigningPolicies is not null) request.CodeSigningPolicies = codeSigningPolicies;
        if (tags is not null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCodeSigningConfigAsync(request);
            return new CodeSigningConfigResult(
                CodeSigningConfig: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.CodeSigningConfig)));
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create code signing config");
        }
    }

    /// <summary>Get a code signing config.</summary>
    public static async Task<CodeSigningConfigResult> GetCodeSigningConfigAsync(
        string codeSigningConfigArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCodeSigningConfigAsync(
                new GetCodeSigningConfigRequest { CodeSigningConfigArn = codeSigningConfigArn });
            return new CodeSigningConfigResult(
                CodeSigningConfig: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.CodeSigningConfig)));
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get code signing config");
        }
    }

    /// <summary>Update a code signing config.</summary>
    public static async Task<CodeSigningConfigResult> UpdateCodeSigningConfigAsync(
        string codeSigningConfigArn,
        string? description = null,
        AllowedPublishers? allowedPublishers = null,
        CodeSigningPolicies? codeSigningPolicies = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateCodeSigningConfigRequest
        {
            CodeSigningConfigArn = codeSigningConfigArn
        };
        if (description is not null) request.Description = description;
        if (allowedPublishers is not null) request.AllowedPublishers = allowedPublishers;
        if (codeSigningPolicies is not null) request.CodeSigningPolicies = codeSigningPolicies;

        try
        {
            var resp = await client.UpdateCodeSigningConfigAsync(request);
            return new CodeSigningConfigResult(
                CodeSigningConfig: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.CodeSigningConfig)));
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update code signing config");
        }
    }

    /// <summary>Delete a code signing config.</summary>
    public static async Task DeleteCodeSigningConfigAsync(
        string codeSigningConfigArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCodeSigningConfigAsync(
                new DeleteCodeSigningConfigRequest { CodeSigningConfigArn = codeSigningConfigArn });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete code signing config");
        }
    }

    /// <summary>List code signing configs.</summary>
    public static async Task<ListCodeSigningConfigsResult> ListCodeSigningConfigsAsync(
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCodeSigningConfigsRequest();
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListCodeSigningConfigsAsync(request);
            return new ListCodeSigningConfigsResult(
                NextMarker: resp.NextMarker,
                CodeSigningConfigs: resp.CodeSigningConfigs?.Select(c =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(c))).ToList());
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list code signing configs");
        }
    }

    // -----------------------------------------------------------------------
    // Function Code Signing Config
    // -----------------------------------------------------------------------

    /// <summary>Put function code signing config.</summary>
    public static async Task<FunctionCodeSigningConfigResult> PutFunctionCodeSigningConfigAsync(
        string codeSigningConfigArn,
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutFunctionCodeSigningConfigAsync(
                new PutFunctionCodeSigningConfigRequest
                {
                    CodeSigningConfigArn = codeSigningConfigArn,
                    FunctionName = functionName
                });
            return new FunctionCodeSigningConfigResult(
                CodeSigningConfigArn: resp.CodeSigningConfigArn,
                FunctionName: resp.FunctionName);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put function code signing config");
        }
    }

    /// <summary>Get function code signing config.</summary>
    public static async Task<FunctionCodeSigningConfigResult> GetFunctionCodeSigningConfigAsync(
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFunctionCodeSigningConfigAsync(
                new GetFunctionCodeSigningConfigRequest { FunctionName = functionName });
            return new FunctionCodeSigningConfigResult(
                CodeSigningConfigArn: resp.CodeSigningConfigArn,
                FunctionName: resp.FunctionName);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function code signing config");
        }
    }

    /// <summary>Delete function code signing config.</summary>
    public static async Task DeleteFunctionCodeSigningConfigAsync(
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFunctionCodeSigningConfigAsync(
                new DeleteFunctionCodeSigningConfigRequest { FunctionName = functionName });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete function code signing config");
        }
    }

    // -----------------------------------------------------------------------
    // Function Event Invoke Config
    // -----------------------------------------------------------------------

    /// <summary>Put function event invoke config.</summary>
    public static async Task<FunctionEventInvokeConfigResult> PutFunctionEventInvokeConfigAsync(
        string functionName,
        string? qualifier = null,
        int? maximumRetryAttempts = null,
        int? maximumEventAgeInSeconds = null,
        DestinationConfig? destinationConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutFunctionEventInvokeConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (maximumRetryAttempts.HasValue) request.MaximumRetryAttempts = maximumRetryAttempts.Value;
        if (maximumEventAgeInSeconds.HasValue) request.MaximumEventAgeInSeconds = maximumEventAgeInSeconds.Value;
        if (destinationConfig is not null) request.DestinationConfig = destinationConfig;

        try
        {
            var resp = await client.PutFunctionEventInvokeConfigAsync(request);
            return MapFunctionEventInvokeConfig(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put function event invoke config");
        }
    }

    /// <summary>Get function event invoke config.</summary>
    public static async Task<FunctionEventInvokeConfigResult> GetFunctionEventInvokeConfigAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFunctionEventInvokeConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetFunctionEventInvokeConfigAsync(request);
            return MapFunctionEventInvokeConfig(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function event invoke config");
        }
    }

    /// <summary>List function event invoke configs.</summary>
    public static async Task<ListFunctionEventInvokeConfigsResult> ListFunctionEventInvokeConfigsAsync(
        string functionName,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFunctionEventInvokeConfigsRequest { FunctionName = functionName };
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListFunctionEventInvokeConfigsAsync(request);
            return new ListFunctionEventInvokeConfigsResult(
                FunctionEventInvokeConfigs: resp.FunctionEventInvokeConfigs?.Select(c =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(c))).ToList(),
                NextMarker: resp.NextMarker);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list function event invoke configs");
        }
    }

    /// <summary>Update function event invoke config.</summary>
    public static async Task<FunctionEventInvokeConfigResult> UpdateFunctionEventInvokeConfigAsync(
        string functionName,
        string? qualifier = null,
        int? maximumRetryAttempts = null,
        int? maximumEventAgeInSeconds = null,
        DestinationConfig? destinationConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateFunctionEventInvokeConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (maximumRetryAttempts.HasValue) request.MaximumRetryAttempts = maximumRetryAttempts.Value;
        if (maximumEventAgeInSeconds.HasValue) request.MaximumEventAgeInSeconds = maximumEventAgeInSeconds.Value;
        if (destinationConfig is not null) request.DestinationConfig = destinationConfig;

        try
        {
            var resp = await client.UpdateFunctionEventInvokeConfigAsync(request);
            return MapFunctionEventInvokeConfig(resp);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update function event invoke config");
        }
    }

    /// <summary>Delete function event invoke config.</summary>
    public static async Task DeleteFunctionEventInvokeConfigAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteFunctionEventInvokeConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            await client.DeleteFunctionEventInvokeConfigAsync(request);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete function event invoke config");
        }
    }

    // -----------------------------------------------------------------------
    // Account Settings
    // -----------------------------------------------------------------------

    /// <summary>Get Lambda account settings.</summary>
    public static async Task<GetAccountSettingsResult> GetAccountSettingsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccountSettingsAsync(new GetAccountSettingsRequest());
            return new GetAccountSettingsResult(
                AccountLimit: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.AccountLimit)),
                AccountUsage: JsonSerializer.Deserialize<Dictionary<string, object?>>(
                    JsonSerializer.Serialize(resp.AccountUsage)));
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get account settings");
        }
    }

    // -----------------------------------------------------------------------
    // Provisioned Concurrency
    // -----------------------------------------------------------------------

    /// <summary>Get provisioned concurrency config.</summary>
    public static async Task<ProvisionedConcurrencyConfigResult> GetProvisionedConcurrencyConfigAsync(
        string functionName,
        string qualifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetProvisionedConcurrencyConfigAsync(
                new GetProvisionedConcurrencyConfigRequest
                {
                    FunctionName = functionName,
                    Qualifier = qualifier
                });
            return new ProvisionedConcurrencyConfigResult(
                RequestedProvisionedConcurrentExecutions: resp.RequestedProvisionedConcurrentExecutions,
                AvailableProvisionedConcurrentExecutions: resp.AvailableProvisionedConcurrentExecutions,
                AllocatedProvisionedConcurrentExecutions: resp.AllocatedProvisionedConcurrentExecutions,
                Status: resp.Status?.Value,
                StatusReason: resp.StatusReason,
                LastModified: resp.LastModified);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get provisioned concurrency config");
        }
    }

    /// <summary>Put provisioned concurrency config.</summary>
    public static async Task<ProvisionedConcurrencyConfigResult> PutProvisionedConcurrencyConfigAsync(
        string functionName,
        string qualifier,
        int provisionedConcurrentExecutions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutProvisionedConcurrencyConfigAsync(
                new PutProvisionedConcurrencyConfigRequest
                {
                    FunctionName = functionName,
                    Qualifier = qualifier,
                    ProvisionedConcurrentExecutions = provisionedConcurrentExecutions
                });
            return new ProvisionedConcurrencyConfigResult(
                RequestedProvisionedConcurrentExecutions: resp.RequestedProvisionedConcurrentExecutions,
                AvailableProvisionedConcurrentExecutions: resp.AvailableProvisionedConcurrentExecutions,
                AllocatedProvisionedConcurrentExecutions: resp.AllocatedProvisionedConcurrentExecutions,
                Status: resp.Status?.Value,
                StatusReason: resp.StatusReason,
                LastModified: resp.LastModified);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put provisioned concurrency config");
        }
    }

    /// <summary>Delete provisioned concurrency config.</summary>
    public static async Task DeleteProvisionedConcurrencyConfigAsync(
        string functionName,
        string qualifier,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProvisionedConcurrencyConfigAsync(
                new DeleteProvisionedConcurrencyConfigRequest
                {
                    FunctionName = functionName,
                    Qualifier = qualifier
                });
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete provisioned concurrency config");
        }
    }

    /// <summary>List provisioned concurrency configs.</summary>
    public static async Task<ListProvisionedConcurrencyConfigsResult> ListProvisionedConcurrencyConfigsAsync(
        string functionName,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListProvisionedConcurrencyConfigsRequest { FunctionName = functionName };
        if (marker is not null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value;

        try
        {
            var resp = await client.ListProvisionedConcurrencyConfigsAsync(request);
            return new ListProvisionedConcurrencyConfigsResult(
                ProvisionedConcurrencyConfigs: resp.ProvisionedConcurrencyConfigs?.Select(c =>
                    JsonSerializer.Deserialize<Dictionary<string, object?>>(
                        JsonSerializer.Serialize(c))).ToList(),
                NextMarker: resp.NextMarker);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list provisioned concurrency configs");
        }
    }

    // -----------------------------------------------------------------------
    // Runtime Management Config
    // -----------------------------------------------------------------------

    /// <summary>Get runtime management config.</summary>
    public static async Task<RuntimeManagementConfigResult> GetRuntimeManagementConfigAsync(
        string functionName,
        string? qualifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRuntimeManagementConfigRequest { FunctionName = functionName };
        if (qualifier is not null) request.Qualifier = qualifier;

        try
        {
            var resp = await client.GetRuntimeManagementConfigAsync(request);
            return new RuntimeManagementConfigResult(
                UpdateRuntimeOn: resp.UpdateRuntimeOn?.Value,
                RuntimeVersionArn: resp.RuntimeVersionArn,
                FunctionArn: resp.FunctionArn);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get runtime management config");
        }
    }

    /// <summary>Put runtime management config.</summary>
    public static async Task<RuntimeManagementConfigResult> PutRuntimeManagementConfigAsync(
        string functionName,
        string updateRuntimeOn,
        string? qualifier = null,
        string? runtimeVersionArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutRuntimeManagementConfigRequest
        {
            FunctionName = functionName,
            UpdateRuntimeOn = updateRuntimeOn
        };
        if (qualifier is not null) request.Qualifier = qualifier;
        if (runtimeVersionArn is not null) request.RuntimeVersionArn = runtimeVersionArn;

        try
        {
            var resp = await client.PutRuntimeManagementConfigAsync(request);
            return new RuntimeManagementConfigResult(
                UpdateRuntimeOn: resp.UpdateRuntimeOn?.Value,
                FunctionArn: resp.FunctionArn,
                RuntimeVersionArn: resp.RuntimeVersionArn);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put runtime management config");
        }
    }

    // -----------------------------------------------------------------------
    // Function Recursion Config
    // -----------------------------------------------------------------------

    /// <summary>Get function recursion config.</summary>
    public static async Task<FunctionRecursionConfigResult> GetFunctionRecursionConfigAsync(
        string functionName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetFunctionRecursionConfigAsync(
                new GetFunctionRecursionConfigRequest { FunctionName = functionName });
            return new FunctionRecursionConfigResult(
                RecursiveLoop: resp.RecursiveLoop?.Value);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get function recursion config");
        }
    }

    /// <summary>Put function recursion config.</summary>
    public static async Task<FunctionRecursionConfigResult> PutFunctionRecursionConfigAsync(
        string functionName,
        string recursiveLoop,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutFunctionRecursionConfigAsync(
                new PutFunctionRecursionConfigRequest
                {
                    FunctionName = functionName,
                    RecursiveLoop = recursiveLoop
                });
            return new FunctionRecursionConfigResult(
                RecursiveLoop: resp.RecursiveLoop?.Value);
        }
        catch (AmazonLambdaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put function recursion config");
        }
    }

    // -----------------------------------------------------------------------
    // Private mapping helpers
    // -----------------------------------------------------------------------

    private static FunctionConfigurationResult MapFunctionConfiguration(
        CreateFunctionResponse resp) =>
        new(
            FunctionName: resp.FunctionName,
            FunctionArn: resp.FunctionArn,
            Runtime: resp.Runtime?.Value,
            Role: resp.Role,
            Handler: resp.Handler,
            CodeSize: resp.CodeSize,
            Description: resp.Description,
            Timeout: resp.Timeout,
            MemorySize: resp.MemorySize,
            LastModified: resp.LastModified,
            CodeSha256: resp.CodeSha256,
            Version: resp.Version,
            KmsKeyArn: resp.KMSKeyArn,
            MasterArn: resp.MasterArn,
            RevisionId: resp.RevisionId,
            State: resp.State?.Value,
            StateReason: resp.StateReason,
            StateReasonCode: resp.StateReasonCode?.Value,
            LastUpdateStatus: resp.LastUpdateStatus?.Value,
            LastUpdateStatusReason: resp.LastUpdateStatusReason,
            LastUpdateStatusReasonCode: resp.LastUpdateStatusReasonCode?.Value,
            PackageType: resp.PackageType?.Value,
            SigningProfileVersionArn: resp.SigningProfileVersionArn,
            SigningJobArn: resp.SigningJobArn,
            Architectures: resp.Architectures?.ToList());

    private static FunctionConfigurationResult MapFunctionConfiguration(
        GetFunctionConfigurationResponse resp) =>
        new(
            FunctionName: resp.FunctionName,
            FunctionArn: resp.FunctionArn,
            Runtime: resp.Runtime?.Value,
            Role: resp.Role,
            Handler: resp.Handler,
            CodeSize: resp.CodeSize,
            Description: resp.Description,
            Timeout: resp.Timeout,
            MemorySize: resp.MemorySize,
            LastModified: resp.LastModified,
            CodeSha256: resp.CodeSha256,
            Version: resp.Version,
            KmsKeyArn: resp.KMSKeyArn,
            MasterArn: resp.MasterArn,
            RevisionId: resp.RevisionId,
            State: resp.State?.Value,
            StateReason: resp.StateReason,
            StateReasonCode: resp.StateReasonCode?.Value,
            LastUpdateStatus: resp.LastUpdateStatus?.Value,
            LastUpdateStatusReason: resp.LastUpdateStatusReason,
            LastUpdateStatusReasonCode: resp.LastUpdateStatusReasonCode?.Value,
            PackageType: resp.PackageType?.Value,
            SigningProfileVersionArn: resp.SigningProfileVersionArn,
            SigningJobArn: resp.SigningJobArn,
            Architectures: resp.Architectures?.ToList());

    private static FunctionConfigurationResult MapFunctionConfiguration(
        UpdateFunctionCodeResponse resp) =>
        new(
            FunctionName: resp.FunctionName,
            FunctionArn: resp.FunctionArn,
            Runtime: resp.Runtime?.Value,
            Role: resp.Role,
            Handler: resp.Handler,
            CodeSize: resp.CodeSize,
            Description: resp.Description,
            Timeout: resp.Timeout,
            MemorySize: resp.MemorySize,
            LastModified: resp.LastModified,
            CodeSha256: resp.CodeSha256,
            Version: resp.Version,
            KmsKeyArn: resp.KMSKeyArn,
            MasterArn: resp.MasterArn,
            RevisionId: resp.RevisionId,
            State: resp.State?.Value,
            StateReason: resp.StateReason,
            StateReasonCode: resp.StateReasonCode?.Value,
            LastUpdateStatus: resp.LastUpdateStatus?.Value,
            LastUpdateStatusReason: resp.LastUpdateStatusReason,
            LastUpdateStatusReasonCode: resp.LastUpdateStatusReasonCode?.Value,
            PackageType: resp.PackageType?.Value,
            SigningProfileVersionArn: resp.SigningProfileVersionArn,
            SigningJobArn: resp.SigningJobArn,
            Architectures: resp.Architectures?.ToList());

    private static FunctionConfigurationResult MapFunctionConfiguration(
        UpdateFunctionConfigurationResponse resp) =>
        new(
            FunctionName: resp.FunctionName,
            FunctionArn: resp.FunctionArn,
            Runtime: resp.Runtime?.Value,
            Role: resp.Role,
            Handler: resp.Handler,
            CodeSize: resp.CodeSize,
            Description: resp.Description,
            Timeout: resp.Timeout,
            MemorySize: resp.MemorySize,
            LastModified: resp.LastModified,
            CodeSha256: resp.CodeSha256,
            Version: resp.Version,
            KmsKeyArn: resp.KMSKeyArn,
            MasterArn: resp.MasterArn,
            RevisionId: resp.RevisionId,
            State: resp.State?.Value,
            StateReason: resp.StateReason,
            StateReasonCode: resp.StateReasonCode?.Value,
            LastUpdateStatus: resp.LastUpdateStatus?.Value,
            LastUpdateStatusReason: resp.LastUpdateStatusReason,
            LastUpdateStatusReasonCode: resp.LastUpdateStatusReasonCode?.Value,
            PackageType: resp.PackageType?.Value,
            SigningProfileVersionArn: resp.SigningProfileVersionArn,
            SigningJobArn: resp.SigningJobArn,
            Architectures: resp.Architectures?.ToList());

    private static FunctionConfigurationResult MapFunctionConfiguration(
        PublishVersionResponse resp) =>
        new(
            FunctionName: resp.FunctionName,
            FunctionArn: resp.FunctionArn,
            Runtime: resp.Runtime?.Value,
            Role: resp.Role,
            Handler: resp.Handler,
            CodeSize: resp.CodeSize,
            Description: resp.Description,
            Timeout: resp.Timeout,
            MemorySize: resp.MemorySize,
            LastModified: resp.LastModified,
            CodeSha256: resp.CodeSha256,
            Version: resp.Version,
            KmsKeyArn: resp.KMSKeyArn,
            MasterArn: resp.MasterArn,
            RevisionId: resp.RevisionId,
            State: resp.State?.Value,
            StateReason: resp.StateReason,
            StateReasonCode: resp.StateReasonCode?.Value,
            LastUpdateStatus: resp.LastUpdateStatus?.Value,
            LastUpdateStatusReason: resp.LastUpdateStatusReason,
            LastUpdateStatusReasonCode: resp.LastUpdateStatusReasonCode?.Value,
            PackageType: resp.PackageType?.Value,
            SigningProfileVersionArn: resp.SigningProfileVersionArn,
            SigningJobArn: resp.SigningJobArn,
            Architectures: resp.Architectures?.ToList());

    private static AliasResult MapAlias(CreateAliasResponse resp) =>
        new(
            AliasArn: resp.AliasArn,
            Name: resp.Name,
            FunctionVersion: resp.FunctionVersion,
            Description: resp.Description,
            RoutingConfig: resp.RoutingConfig?.AdditionalVersionWeights?.ToDictionary(
                kv => kv.Key, kv => (object?)kv.Value),
            RevisionId: resp.RevisionId);

    private static AliasResult MapAlias(UpdateAliasResponse resp) =>
        new(
            AliasArn: resp.AliasArn,
            Name: resp.Name,
            FunctionVersion: resp.FunctionVersion,
            Description: resp.Description,
            RoutingConfig: resp.RoutingConfig?.AdditionalVersionWeights?.ToDictionary(
                kv => kv.Key, kv => (object?)kv.Value),
            RevisionId: resp.RevisionId);

    private static AliasResult MapAlias(GetAliasResponse resp) =>
        new(
            AliasArn: resp.AliasArn,
            Name: resp.Name,
            FunctionVersion: resp.FunctionVersion,
            Description: resp.Description,
            RoutingConfig: resp.RoutingConfig?.AdditionalVersionWeights?.ToDictionary(
                kv => kv.Key, kv => (object?)kv.Value),
            RevisionId: resp.RevisionId);

    private static EventSourceMappingResult MapEventSourceMapping(
        CreateEventSourceMappingResponse resp) =>
        new(
            Uuid: resp.UUID,
            BatchSize: resp.BatchSize,
            EventSourceArn: resp.EventSourceArn,
            FunctionArn: resp.FunctionArn,
            State: resp.State,
            StateTransitionReason: resp.StateTransitionReason,
            MaximumRecordAgeInSeconds: resp.MaximumRecordAgeInSeconds,
            BisectBatchOnFunctionError: resp.BisectBatchOnFunctionError,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            ParallelizationFactor: resp.ParallelizationFactor,
            MaximumBatchingWindowInSeconds: resp.MaximumBatchingWindowInSeconds,
            TumblingWindowInSeconds: resp.TumblingWindowInSeconds,
            Topics: resp.Topics,
            Queues: resp.Queues,
            EventSourceMappingArn: resp.EventSourceMappingArn);

    private static EventSourceMappingResult MapEventSourceMapping(
        DeleteEventSourceMappingResponse resp) =>
        new(
            Uuid: resp.UUID,
            BatchSize: resp.BatchSize,
            EventSourceArn: resp.EventSourceArn,
            FunctionArn: resp.FunctionArn,
            State: resp.State,
            StateTransitionReason: resp.StateTransitionReason,
            MaximumRecordAgeInSeconds: resp.MaximumRecordAgeInSeconds,
            BisectBatchOnFunctionError: resp.BisectBatchOnFunctionError,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            ParallelizationFactor: resp.ParallelizationFactor,
            MaximumBatchingWindowInSeconds: resp.MaximumBatchingWindowInSeconds,
            TumblingWindowInSeconds: resp.TumblingWindowInSeconds,
            Topics: resp.Topics,
            Queues: resp.Queues,
            EventSourceMappingArn: resp.EventSourceMappingArn);

    private static EventSourceMappingResult MapEventSourceMapping(
        GetEventSourceMappingResponse resp) =>
        new(
            Uuid: resp.UUID,
            BatchSize: resp.BatchSize,
            EventSourceArn: resp.EventSourceArn,
            FunctionArn: resp.FunctionArn,
            State: resp.State,
            StateTransitionReason: resp.StateTransitionReason,
            MaximumRecordAgeInSeconds: resp.MaximumRecordAgeInSeconds,
            BisectBatchOnFunctionError: resp.BisectBatchOnFunctionError,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            ParallelizationFactor: resp.ParallelizationFactor,
            MaximumBatchingWindowInSeconds: resp.MaximumBatchingWindowInSeconds,
            TumblingWindowInSeconds: resp.TumblingWindowInSeconds,
            Topics: resp.Topics,
            Queues: resp.Queues,
            EventSourceMappingArn: resp.EventSourceMappingArn);

    private static EventSourceMappingResult MapEventSourceMapping(
        UpdateEventSourceMappingResponse resp) =>
        new(
            Uuid: resp.UUID,
            BatchSize: resp.BatchSize,
            EventSourceArn: resp.EventSourceArn,
            FunctionArn: resp.FunctionArn,
            State: resp.State,
            StateTransitionReason: resp.StateTransitionReason,
            MaximumRecordAgeInSeconds: resp.MaximumRecordAgeInSeconds,
            BisectBatchOnFunctionError: resp.BisectBatchOnFunctionError,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            ParallelizationFactor: resp.ParallelizationFactor,
            MaximumBatchingWindowInSeconds: resp.MaximumBatchingWindowInSeconds,
            TumblingWindowInSeconds: resp.TumblingWindowInSeconds,
            Topics: resp.Topics,
            Queues: resp.Queues,
            EventSourceMappingArn: resp.EventSourceMappingArn);

    private static FunctionEventInvokeConfigResult MapFunctionEventInvokeConfig(
        PutFunctionEventInvokeConfigResponse resp) =>
        new(
            LastModified: resp.LastModified.ToString(),
            FunctionArn: resp.FunctionArn,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            MaximumEventAgeInSeconds: resp.MaximumEventAgeInSeconds);

    private static FunctionEventInvokeConfigResult MapFunctionEventInvokeConfig(
        GetFunctionEventInvokeConfigResponse resp) =>
        new(
            LastModified: resp.LastModified.ToString(),
            FunctionArn: resp.FunctionArn,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            MaximumEventAgeInSeconds: resp.MaximumEventAgeInSeconds);

    private static FunctionEventInvokeConfigResult MapFunctionEventInvokeConfig(
        UpdateFunctionEventInvokeConfigResponse resp) =>
        new(
            LastModified: resp.LastModified.ToString(),
            FunctionArn: resp.FunctionArn,
            MaximumRetryAttempts: resp.MaximumRetryAttempts,
            MaximumEventAgeInSeconds: resp.MaximumEventAgeInSeconds);
}
