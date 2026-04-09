using System.Diagnostics;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Standard Lambda proxy response.</summary>
public sealed record LambdaResponse(
    int StatusCode,
    Dictionary<string, string>? Headers = null,
    string? Body = null,
    bool IsBase64Encoded = false)
{
    /// <summary>Shorthand for a 200 OK response with a JSON body.</summary>
    public static LambdaResponse Ok(object? body = null) =>
        new(200,
            Headers: new Dictionary<string, string> { ["Content-Type"] = "application/json" },
            Body: body != null ? JsonSerializer.Serialize(body) : null);

    /// <summary>Shorthand for an error response.</summary>
    public static LambdaResponse Error(int statusCode, string message) =>
        new(statusCode,
            Headers: new Dictionary<string, string> { ["Content-Type"] = "application/json" },
            Body: JsonSerializer.Serialize(new { error = message }));
}

/// <summary>Result of batch processing SQS records.</summary>
public sealed record BatchProcessorResult(
    int TotalRecords = 0,
    int Succeeded = 0,
    int Failed = 0,
    List<string>? FailedMessageIds = null)
{
    /// <summary>
    /// Build the SQS partial-batch-failure response (batchItemFailures).
    /// </summary>
    public object ToPartialBatchResponse() =>
        new
        {
            batchItemFailures = (FailedMessageIds ?? [])
                .Select(id => new { itemIdentifier = id })
                .ToList()
        };
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Lambda middleware patterns implemented as composable async handlers.
/// Provides idempotency, batch processing, middleware chaining,
/// response building, event parsing, cold start tracking, and timeout guards.
/// </summary>
public static class LambdaMiddlewareService
{
    private static bool _isColdStart = true;
    private static readonly object ColdStartLock = new();

    /// <summary>
    /// Idempotent Lambda handler backed by DynamoDB. Before executing
    /// the handler, checks whether the idempotency key has already been
    /// processed. If so, returns the cached result. Otherwise, executes
    /// the handler, stores the result, and returns it.
    /// </summary>
    /// <param name="tableName">DynamoDB table for idempotency records.</param>
    /// <param name="idempotencyKey">Unique key for this invocation (e.g. event ID).</param>
    /// <param name="handler">The actual handler function to execute.</param>
    /// <param name="ttlSeconds">Time-to-live for the idempotency record in seconds.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<LambdaResponse> IdempotentHandlerAsync(
        string tableName,
        string idempotencyKey,
        Func<Task<LambdaResponse>> handler,
        int ttlSeconds = 3600,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Check for an existing idempotency record
            var existingItem = await DynamoDbService.GetItemAsync(
                tableName,
                new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = idempotencyKey }
                },
                consistentRead: true,
                region: region);

            if (existingItem != null &&
                existingItem.TryGetValue("response", out var responseAttr) &&
                responseAttr.S != null)
            {
                // Return the cached response
                var cached = JsonSerializer.Deserialize<LambdaResponse>(responseAttr.S);
                if (cached != null)
                    return cached;
            }

            // 2. Execute the handler
            var response = await handler();

            // 3. Store the result in DynamoDB with a TTL
            var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + ttlSeconds;
            await DynamoDbService.PutItemAsync(
                tableName,
                new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = idempotencyKey },
                    ["response"] = new() { S = JsonSerializer.Serialize(response) },
                    ["ttl"] = new() { N = ttl.ToString() },
                    ["createdAt"] = new() { S = DateTime.UtcNow.ToString() }
                },
                region: region);

            return response;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Idempotent handler failed for key '{idempotencyKey}'");
        }
    }

    /// <summary>
    /// SQS batch processor with per-item error handling. Processes each
    /// record independently and returns partial-batch-failure results
    /// so that only failed messages are retried.
    /// </summary>
    /// <typeparam name="T">Type to deserialize each SQS record body into.</typeparam>
    /// <param name="records">List of SQS records from the Lambda event.</param>
    /// <param name="processor">Async processor for each deserialized record.</param>
    public static async Task<BatchProcessorResult> BatchProcessorAsync<T>(
        List<SqsBatchRecord> records,
        Func<T, Task> processor)
    {
        var failedIds = new List<string>();
        var succeeded = 0;

        foreach (var record in records)
        {
            try
            {
                var item = JsonSerializer.Deserialize<T>(record.Body);
                if (item != null)
                {
                    await processor(item);
                }
                succeeded++;
            }
            catch (Exception)
            {
                failedIds.Add(record.MessageId);
            }
        }

        return new BatchProcessorResult(
            TotalRecords: records.Count,
            Succeeded: succeeded,
            Failed: failedIds.Count,
            FailedMessageIds: failedIds.Count > 0 ? failedIds : null);
    }

    /// <summary>
    /// Compose multiple middleware functions into a single handler chain.
    /// Each middleware receives the next handler and can run logic before
    /// and/or after calling it.
    /// </summary>
    /// <param name="handler">The innermost handler.</param>
    /// <param name="middlewares">
    /// Middleware functions, applied outermost-first.
    /// Each receives the next handler in the chain and returns a wrapped handler.
    /// </param>
    public static Func<JsonElement, Task<LambdaResponse>> MiddlewareChain(
        Func<JsonElement, Task<LambdaResponse>> handler,
        params Func<Func<JsonElement, Task<LambdaResponse>>,
            Func<JsonElement, Task<LambdaResponse>>>[] middlewares)
    {
        var current = handler;
        // Apply in reverse so the first middleware in the array is the outermost
        for (var i = middlewares.Length - 1; i >= 0; i--)
        {
            current = middlewares[i](current);
        }
        return current;
    }

    /// <summary>
    /// Build a standard Lambda proxy integration response.
    /// </summary>
    /// <param name="statusCode">HTTP status code.</param>
    /// <param name="body">Response body (will be JSON-serialized if not a string).</param>
    /// <param name="headers">Optional response headers.</param>
    /// <param name="isBase64Encoded">Whether the body is Base64-encoded.</param>
    public static Task<LambdaResponse> LambdaResponseAsync(
        int statusCode,
        object? body = null,
        Dictionary<string, string>? headers = null,
        bool isBase64Encoded = false)
    {
        var defaultHeaders = new Dictionary<string, string>
        {
            ["Content-Type"] = "application/json",
            ["Access-Control-Allow-Origin"] = "*"
        };

        if (headers != null)
        {
            foreach (var kv in headers)
                defaultHeaders[kv.Key] = kv.Value;
        }

        string? serializedBody = null;
        if (body is string s)
            serializedBody = s;
        else if (body != null)
            serializedBody = JsonSerializer.Serialize(body);

        return Task.FromResult(new LambdaResponse(
            StatusCode: statusCode,
            Headers: defaultHeaders,
            Body: serializedBody,
            IsBase64Encoded: isBase64Encoded));
    }

    /// <summary>
    /// Parse a raw Lambda event JSON string into a <see cref="JsonElement"/>.
    /// </summary>
    /// <param name="eventJson">Raw JSON string from the Lambda invocation.</param>
    /// <returns>Parsed <see cref="JsonElement"/>.</returns>
    public static JsonElement ParseEvent(string eventJson)
    {
        return JsonSerializer.Deserialize<JsonElement>(eventJson);
    }

    /// <summary>
    /// Track Lambda cold starts. Returns <c>true</c> on the first call
    /// (cold start) and <c>false</c> on all subsequent calls (warm invocations).
    /// Thread-safe.
    /// </summary>
    public static bool ColdStartTracker()
    {
        lock (ColdStartLock)
        {
            if (!_isColdStart) return false;
            _isColdStart = false;
            return true;
        }
    }

    /// <summary>
    /// Execute a handler with a timeout guard. If the handler does not
    /// complete within the specified timeout, returns a 504 Gateway Timeout
    /// response instead of letting the Lambda runtime kill the process.
    /// </summary>
    /// <param name="handler">The handler to execute.</param>
    /// <param name="timeoutMs">Timeout in milliseconds. Should be set slightly
    /// less than the Lambda function's configured timeout.</param>
    public static async Task<LambdaResponse> LambdaTimeoutGuard(
        Func<Task<LambdaResponse>> handler,
        int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        try
        {
            var handlerTask = handler();
            var completedTask = await Task.WhenAny(
                handlerTask,
                Task.Delay(timeoutMs, cts.Token));

            if (completedTask == handlerTask)
            {
                await cts.CancelAsync();
                return await handlerTask;
            }

            return LambdaResponse.Error(504, "Function execution timed out");
        }
        catch (OperationCanceledException)
        {
            return LambdaResponse.Error(504, "Function execution timed out");
        }
        catch (Exception exc)
        {
            return LambdaResponse.Error(500, $"Internal error: {exc.Message}");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="IdempotentHandlerAsync"/>.</summary>
    public static LambdaResponse IdempotentHandler(string tableName, string idempotencyKey, Func<Task<LambdaResponse>> handler, int ttlSeconds = 3600, RegionEndpoint? region = null)
        => IdempotentHandlerAsync(tableName, idempotencyKey, handler, ttlSeconds, region).GetAwaiter().GetResult();

}

/// <summary>
/// Represents a single SQS record from a Lambda event.
/// </summary>
public sealed record SqsBatchRecord(
    string MessageId,
    string ReceiptHandle,
    string Body,
    string? EventSource = null,
    string? EventSourceArn = null,
    string? AwsRegion = null);
