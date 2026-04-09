using Amazon;
using Amazon.BedrockAgentRuntime;
using Amazon.BedrockAgentRuntime.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Bedrock Agent Runtime operations.
/// </summary>
public sealed record InvokeAgentResult(
    string? ContentType = null,
    string? SessionId = null);

public sealed record RetrieveResult(
    List<KnowledgeBaseRetrievalResult>? RetrievalResults = null);

public sealed record RetrieveAndGenerateResult(
    string? SessionId = null,
    RetrieveAndGenerateOutput? Output = null);

/// <summary>
/// Utility helpers for Amazon Bedrock Agent Runtime.
/// </summary>
public static class BedrockAgentRuntimeService
{
    private static AmazonBedrockAgentRuntimeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonBedrockAgentRuntimeClient>(region);

    /// <summary>
    /// Invoke a Bedrock agent.
    /// Returns the response for the caller to consume the event stream.
    /// </summary>
    public static async Task<InvokeAgentResponse> InvokeAgentAsync(
        InvokeAgentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            return await client.InvokeAgentAsync(request);
        }
        catch (AmazonBedrockAgentRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to invoke Bedrock agent '{request.AgentId}'");
        }
    }

    /// <summary>
    /// Retrieve information from a Bedrock knowledge base.
    /// </summary>
    public static async Task<RetrieveResult> RetrieveAsync(
        RetrieveRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RetrieveAsync(request);
            return new RetrieveResult(RetrievalResults: resp.RetrievalResults);
        }
        catch (AmazonBedrockAgentRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to retrieve from knowledge base '{request.KnowledgeBaseId}'");
        }
    }

    /// <summary>
    /// Retrieve information from a Bedrock knowledge base and generate a response.
    /// </summary>
    public static async Task<RetrieveAndGenerateResult> RetrieveAndGenerateAsync(
        RetrieveAndGenerateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RetrieveAndGenerateAsync(request);
            return new RetrieveAndGenerateResult(
                SessionId: resp.SessionId,
                Output: resp.Output);
        }
        catch (AmazonBedrockAgentRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to retrieve and generate from Bedrock knowledge base");
        }
    }
}
