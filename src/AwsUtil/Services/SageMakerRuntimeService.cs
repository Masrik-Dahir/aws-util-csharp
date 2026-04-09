using Amazon;
using Amazon.SageMakerRuntime;
using Amazon.SageMakerRuntime.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record SageMakerInvokeEndpointResult(
    byte[]? Body = null,
    string? ContentType = null,
    string? InvokedProductionVariant = null,
    string? CustomAttributes = null,
    string? NewSessionId = null,
    string? ClosedSessionId = null);

public sealed record SageMakerInvokeEndpointStreamResult(
    string? ContentType = null,
    string? InvokedProductionVariant = null,
    string? CustomAttributes = null);

public sealed record SageMakerInvokeEndpointAsyncResult(
    string? OutputLocation = null,
    string? InferenceId = null,
    string? FailureLocation = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon SageMaker Runtime.
/// </summary>
public static class SageMakerRuntimeService
{
    private static AmazonSageMakerRuntimeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSageMakerRuntimeClient>(region);

    /// <summary>Invoke a SageMaker endpoint synchronously.</summary>
    public static async Task<SageMakerInvokeEndpointResult>
        InvokeEndpointAsync(
            string endpointName,
            MemoryStream body,
            string? contentType = null,
            string? accept = null,
            string? targetModel = null,
            string? targetVariant = null,
            string? targetContainerHostname = null,
            string? inferenceId = null,
            string? inferenceComponentName = null,
            string? customAttributes = null,
            string? sessionId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InvokeEndpointRequest
        {
            EndpointName = endpointName,
            Body = body
        };
        if (contentType != null) request.ContentType = contentType;
        if (accept != null) request.Accept = accept;
        if (targetModel != null) request.TargetModel = targetModel;
        if (targetVariant != null) request.TargetVariant = targetVariant;
        if (targetContainerHostname != null)
            request.TargetContainerHostname = targetContainerHostname;
        if (inferenceId != null) request.InferenceId = inferenceId;
        if (inferenceComponentName != null)
            request.InferenceComponentName = inferenceComponentName;
        if (customAttributes != null)
            request.CustomAttributes = customAttributes;
        if (sessionId != null) request.SessionId = sessionId;

        try
        {
            var resp = await client.InvokeEndpointAsync(request);
            byte[]? bodyBytes = null;
            if (resp.Body != null)
            {
                using var ms = new MemoryStream();
                await resp.Body.CopyToAsync(ms);
                bodyBytes = ms.ToArray();
            }

            return new SageMakerInvokeEndpointResult(
                Body: bodyBytes,
                ContentType: resp.ContentType,
                InvokedProductionVariant: resp.InvokedProductionVariant,
                CustomAttributes: resp.CustomAttributes,
                NewSessionId: resp.NewSessionId,
                ClosedSessionId: resp.ClosedSessionId);
        }
        catch (AmazonSageMakerRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke endpoint '{endpointName}'");
        }
    }

    /// <summary>Invoke a SageMaker endpoint with response streaming.</summary>
    public static async Task<SageMakerInvokeEndpointStreamResult>
        InvokeEndpointWithResponseStreamAsync(
            string endpointName,
            MemoryStream body,
            string? contentType = null,
            string? accept = null,
            string? targetModel = null,
            string? targetVariant = null,
            string? targetContainerHostname = null,
            string? inferenceId = null,
            string? inferenceComponentName = null,
            string? customAttributes = null,
            string? sessionId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InvokeEndpointWithResponseStreamRequest
        {
            EndpointName = endpointName,
            Body = body
        };
        if (contentType != null) request.ContentType = contentType;
        if (accept != null) request.Accept = accept;
        // TargetModel is not available on InvokeEndpointWithResponseStreamRequest in SDK v4
        if (targetVariant != null) request.TargetVariant = targetVariant;
        if (targetContainerHostname != null)
            request.TargetContainerHostname = targetContainerHostname;
        if (inferenceId != null) request.InferenceId = inferenceId;
        if (inferenceComponentName != null)
            request.InferenceComponentName = inferenceComponentName;
        if (customAttributes != null)
            request.CustomAttributes = customAttributes;
        if (sessionId != null) request.SessionId = sessionId;

        try
        {
            var resp =
                await client.InvokeEndpointWithResponseStreamAsync(request);
            return new SageMakerInvokeEndpointStreamResult(
                ContentType: resp.ContentType,
                InvokedProductionVariant: resp.InvokedProductionVariant,
                CustomAttributes: resp.CustomAttributes);
        }
        catch (AmazonSageMakerRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke endpoint with stream '{endpointName}'");
        }
    }

    /// <summary>Invoke a SageMaker endpoint asynchronously.</summary>
    public static async Task<SageMakerInvokeEndpointAsyncResult>
        InvokeEndpointAsyncAsync(
            string endpointName,
            string inputLocation,
            string? contentType = null,
            string? accept = null,
            string? inferenceId = null,
            int? invocationTimeoutSeconds = null,
            int? requestTTLSeconds = null,
            string? customAttributes = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new InvokeEndpointAsyncRequest
        {
            EndpointName = endpointName,
            InputLocation = inputLocation
        };
        if (contentType != null) request.ContentType = contentType;
        if (accept != null) request.Accept = accept;
        if (inferenceId != null) request.InferenceId = inferenceId;
        if (invocationTimeoutSeconds.HasValue)
            request.InvocationTimeoutSeconds =
                invocationTimeoutSeconds.Value;
        if (requestTTLSeconds.HasValue)
            request.RequestTTLSeconds = requestTTLSeconds.Value;
        if (customAttributes != null)
            request.CustomAttributes = customAttributes;

        try
        {
            var resp = await client.InvokeEndpointAsyncAsync(request);
            return new SageMakerInvokeEndpointAsyncResult(
                OutputLocation: resp.OutputLocation,
                InferenceId: resp.InferenceId,
                FailureLocation: resp.FailureLocation);
        }
        catch (AmazonSageMakerRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke endpoint async '{endpointName}'");
        }
    }
}
